import base64
import json
from google.cloud import bigquery

# Change the your_project_id.dataset_id.profiles to your BigQuery table
# Add requirements.txt file with google-cloud-bigquery
# Python 3.10 is required to use match-case statement

def handle_first_event(client, data):
    table_id = 'your_project_id.dataset_id.profiles'
    rows_to_insert = [data]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        print(f"Errors occurred while inserting rows: {errors}")


def handle_subscription_started_or_non_subscription_purchase(client, user_id, data):
    query = """
    UPDATE `your_project_id.dataset_id.profiles`
    SET state = @state, profile_total_revenue_usd = @profile_total_revenue_usd
    WHERE user_id = @user_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('user_id', 'STRING', user_id),
            bigquery.ScalarQueryParameter('state', 'STRING', data.get('vendor_product_id')),
            bigquery.ScalarQueryParameter('profile_total_revenue_usd', 'NUMERIC', data.get('total_revenue_usd'))
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    if query_job.errors:
        print(f"Errors occurred while updating rows: {query_job.errors}")

def handle_subscription_expired(client, user_id):
    query = """
    UPDATE `your_project_id.dataset_id.profiles`
    SET state = @state
    WHERE user_id = @user_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('user_id', 'STRING', user_id),
            bigquery.ScalarQueryParameter('state', 'STRING', 'Expired')
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()  # Wait for the query to finish

    if query_job.errors:
        print(f"Errors occurred while updating rows: {query_job.errors}")

def handle_subscription_cancelled(client, user_id):
    query = """
    UPDATE `your_project_id.dataset_id.profiles`
    SET state = @state
    WHERE user_id = @user_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('user_id', 'STRING', user_id),
            bigquery.ScalarQueryParameter('state', 'STRING', 'Cancelled')
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    if query_job.errors:
        print(f"Errors occurred while updating rows: {query_job.errors}")

def handle_subscription_renewed(client, user_id, data):
    query = """
    UPDATE `your_project_id.dataset_id.profiles`
    SET profile_total_revenue_usd = @profile_total_revenue_usd
    WHERE user_id = @user_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter('user_id', 'STRING', user_id),
            bigquery.ScalarQueryParameter('profile_total_revenue_usd', 'NUMERIC', data.get('total_revenue_usd'))
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    if query_job.errors:
        print(f"Errors occurred while updating rows: {query_job.errors}")


def pubsub_to_bigquery(event, context):
    client = bigquery.Client()
    message = base64.b64decode(event['data']).decode('utf-8')
    data = json.loads(message)

    event_name = data.get('event_name')
    user_id = data.get('user_id')

    match event_name:
        case 'first_event':
            handle_first_event(client, data)
        case 'subscription_started' | 'non_subscription_purchase':
            handle_subscription_started_or_non_subscription_purchase(client, user_id, data)
        case 'subscription_expired':
            handle_subscription_expired(client, user_id)
        case 'subscription_cancelled':
            handle_subscription_cancelled(client, user_id)
        case 'subscription_renewed':
            handle_subscription_renewed(client, user_id, data)
        case _:
            print(f"Unhandled event_name: {event_name}")

    print("Processing complete.")
