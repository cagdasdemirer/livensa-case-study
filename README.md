# livensa-case-study

## Architecture
![project architecture](https://i.ibb.co/rwgBYXB/diagram.png)

## Pipeline
1- The topic in Pub/Sub receives Google Cloud Storage notification for Uploaded Objects in bucket\
2- Received message trigger the dataflow pipeline\
3- Extract and transform data depending on whether the object type is CSV or Zip file\
4-concurrent- Publish message to Pub/Sub topic for events that will affect Profiles table\
4-concurrent- Stream events data to Unified_events table\
4-concurrent- Stream profiles data to Profiles table\
5-Save error logs in GCS\

## Discussion Points
--Batch Processing vs Stream Processing\
--RDBMS\
--Serious flaws in table designs and case\
--Shortcomings of BigQuery, When Does it Make Sense to Use Google BigQuery?\
--Cost Effectiveness and limitations\
--Future scope of the project (Cloud Composer, Kafka, Data Fusion Sync with Relational Database)\

## Steps I followed
First, I reviewed the case requirements and took notes of the discussion points that came to my mind. Then I checked the documents for limit and feature changes in the GCP Services that I could potentially use. There were two events that I did not want to miss and wanted to use as triggers: New file uploads and specific event_names that would affect the Profiles table. I created two topics in GCP Pub/Sub without using schema for both events.Bigquerry does not have a native notification system for specific event_names. I thought of creating a sink from Google Audit Logger and feeding it from there, but since Audit Logs are kept as SQL queries, it would take longer. I decided to feed this topic myself. There is no UI setting for the GCS notification - Pub/Sub connection. Therefore, I completed the first sub by running the following command in Google Shell.
```sh
gcloud storage buckets notifications create gs://livensa_case_study11 --topic=gcs-new-file --event-types=OBJECT_FINALIZE
```
Then, I created my Bigquery tables according to the designs that were challenging in the case.In the next step, I created a pipeline that would process and stream data according to the uploaded file type. Creating a Dataflow for a small CSV file was a bit overkill, but because of the two table usage limit, I wanted to manage the entire pipeline in a single runner.Finally, I placed a publish message mechanism for the rows that will feed the Profiles table into the ETL pipeline. In this way, I provided some security for important events and also removed the burden of update operations from the dataflow and loaded Google Functions.
## Thoughts
Trigger mechanisms are working well, and since events were streamed row-based, they were ready for analysis as quickly as possible. However, I think we couldn't use the real power of bigquerry. If there were no table design constraints in the case, I would design big data tables differently. If we ignored cost-effectiveness and focused on maximum efficiency, I would use a relational database and native trigger functions for easier and more maintainable implementation. A query-optimized bigquery table with a completed partition and cluster structure would be much more useful. I intentionally did not add the columns that came from the Profiles table and should be added to each unified_events row, and I don't think there was no need to add them in the first place. However, if it is a "must" to add, the most logical and fastest solution would be to add a column to Profiles that we can track the update time and to create a scheduled query to update the necessary columns in unified_events. In this way, we would have created a system close to CDC.

## License
MIT
