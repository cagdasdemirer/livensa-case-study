import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from google.cloud import storage
from google.cloud import pubsub_v1
import csv
import ijson
import zipfile
from io import BytesIO
import logging
from datetime import datetime

# Define custom PipelineOptions and change Project-ID to your GCP project ID like "Feraset-Project"

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input_topic', type=str, help='Pub/Sub topic to read from')
        parser.add_value_provider_argument('--output_table', type=str, help='BigQuery table to write to')
        parser.add_value_provider_argument('--output_topic', type=str, help='Pub/Sub topic to write to')
        parser.add_value_provider_argument('--error_log', type=str, help='GCS path to log errors')

def filter_files(message):
    attributes = message.attributes
    content_type = attributes.get('body.contentType')
    file_id = attributes.get('body.id')

    if content_type == "text/csv" or content_type == "application/zip":
        return (content_type, file_id)
    return None

def process_csv_file(file_id):
    client = storage.Client()
    bucket_name, blob_name = file_id.split("/", 1)  # Split into bucket and blob
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    content = blob.download_as_text()

    reader = csv.DictReader(content.splitlines())

    for row in reader:
        try:
            yield {
                'user_id': row.get('user_id'),
                'event_name': row.get('Event_Name'),
                'event_datetime': int(datetime.strptime(row.get('event_datetime'),"%B %d, %Y, %I:%M %p").timestamp()),
                'city': None,
                'region': None,
                'country': None,
                'os': None,
                'app_version': None,
                'state': None,
                'event_parameters': None,
                'proceeds_usd': row.get('proceeds_usd'),
                'profile_total_revenue_usd': row.get('profile_total_revenue_usd'),
                'cancellation_reason': row.get('cancellation_reason'),
            }
        except Exception as e:
            logging.error(f"Error processing row: {row}, Error: {e}")
            yield beam.pvalue.TaggedOutput('error', {'row': row, 'error': str(e)})

def process_zip_file(file_id):
    client = storage.Client()
    bucket_name, blob_name = file_id.split("/", 1)  # Split into bucket and blob
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(blob_name)
    zip_content = blob.download_as_bytes()

    with zipfile.ZipFile(BytesIO(zip_content)) as zf:
        for file_info in zf.infolist():
            with zf.open(file_info) as json_file:
                try:
                    # Use ijson to parse the JSON file content in a streaming manner
                    parser = ijson.parse(json_file)

                    # Stream through JSON objects
                    for prefix, event, value in parser:
                        if prefix == 'item':
                            yield {
                                'user_id': value.get('user_id'),
                                'event_name': value.get('event_name'),
                                'event_datetime': int(value.get('event_timestamp')),
                                'city': None,
                                'region': None,
                                'country': None,
                                'os': None,
                                'app_version': None,
                                'state': None,
                                'event_parameters': flatten_json(value),
                                'proceeds_usd': None,
                                'profile_total_revenue_usd': None,
                                'cancellation_reason': None
                            }
                except Exception as e:
                    logging.error(f"Error processing JSON content from {file_info.filename}: Error: {e}")
                    yield beam.pvalue.TaggedOutput('error', {'row': None, 'error': str(e)})


def publish_to_topic_if_trigger_event(row, output_topic):
    if (row['Event_Name'] == "first_event" or "subscription_started"
            or "non_subscription_purchase" or "subscription_expired"
            or "subscription_cancelled" or "subscription_renewed"):
        publisher = pubsub_v1.PublisherClient()
        topic_path = publisher.topic_path('Project-ID', output_topic)  # Replace with your project ID
        publisher.publish(topic_path, ijson.dumps(row).encode('utf-8'))
    return row

def flatten_json(y):
    out = {}
    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], name + a + '_')
        elif isinstance(x, list):
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def format_error_with_timestamp(error_info):
    timestamp = datetime.utcnow().isoformat() + 'Z'
    error_info['timestamp'] = timestamp
    return ijson.dumps(error_info)

def run(argv=None):
    pipeline_options = MyOptions(argv)
    pipeline_options.view_as(StandardOptions).streaming = True  # Enable streaming

    with beam.Pipeline(options=pipeline_options) as p:
        (
            p
            # Read messages from Pub/Sub
            | 'ReadFromPubSub' >> beam.io.ReadFromPubSub(topic=pipeline_options.input_topic)
            # Decode and filter for CSV or ZIP files
            | 'FilterFiles' >> beam.FlatMap(lambda message: filter_files(message))
            # Branch processing for CSV and ZIP files
            | 'ProcessFiles' >> beam.FlatMap(lambda content: process_csv_file(content[1]) if content[0] == 'text/csv' else process_zip_file(content[1]))
            # Publish message if event_name == "first_event"
            | 'CheckEventAndPublish' >> beam.Map(lambda row: publish_to_topic_if_trigger_event(row, pipeline_options.output_topic))
            # Write to BigQuery and handle errors
            | 'WriteToBigQuery' >> beam.ParDo(lambda row: row, beam.pvalue.TaggedOutput('error'))
            | 'FormatErrorsWithTimestamp' >> beam.Map(format_error_with_timestamp).with_outputs('error')
            | 'WriteErrorsToLog' >> beam.io.WriteToText(pipeline_options.error_log, file_name_suffix='.txt', append=True)
        )

if __name__ == '__main__':
    run()
