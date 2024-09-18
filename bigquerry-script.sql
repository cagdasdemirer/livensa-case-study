CREATE TABLE feraset.unified_events (
  user_id STRING OPTIONS (description = '"profile_id" for Adapty events, and from "user_id" for Google Analytics events'),
  event_name STRING OPTIONS (description = '“Event_Name” for Adapty events, and from “event_name” for Google Analytics events'),
  event_datetime INT64 OPTIONS (description = 'UNIX Timestamp stıred in minutes for Adapty events, and microseconds for Google Analytics events'),
  city STRING OPTIONS (description = 'retrieved from "profile" table'),
  region STRING OPTIONS (description = 'retrieved from "profile" table'),
  country STRING OPTIONS (description = 'retrieved from "profile" table'),
  os STRING OPTIONS (description = 'retrieved from "profile" table'),
  app_version STRING OPTIONS (description = 'retrieved from "profile" table'),
  state STRING OPTIONS (description = 'retrieved from "profile" table'),
  event_parameters JSON OPTIONS(description = 'flattened JSON for Google Analytics events'),
  proceeds_usd NUMERIC OPTIONS(description = 'retrieved from “proceeds_usd” for Adapty events'),
  profile_total_revenue_usd NUMERIC OPTIONS(description = 'retrieved from “profile_total_revenue_usd” for Adapty events'),
  cancellation_reason STRING OPTIONS(description = 'retrieved from “cancellation_reason” for Adapty events')
)
CLUSTER BY user_id;

CREATE TABLE feraset.profiles (
  user_id STRING PRIMARY KEY OPTIONS (description = 'retrieved from “user_id” from the first_open event'),
  city STRING OPTIONS (description = 'retrieved from “geo.city” from the first_open event'),
  region STRING OPTIONS (description = 'retrieved from “geo.region” from the first_open event'),
  country STRING OPTIONS (description = 'retrieved from “geo.country” from the first_open event'),
  os STRING OPTIONS (description = 'retrieved from “platform” from the first_open event'),
  app_version STRING OPTIONS (description = 'retrieved from “app_info.version” from the first_open event'),
  install_date INT64 OPTIONS (description = 'retrieved from “user_first_touch_timestamp” from first_open event as an UNIX Timestamp'),
  device STRING OPTIONS (description = 'retrieved from “device.mobile_marketing_name” if the OS is Android, from “device.mobile_model_name” if the OS is iOS from the first_open event'),
  os_version STRING OPTIONS (description = 'retrieved from “device.operating_system_version” from the first_open event'),
  profile_total_revenue_usd NUMERIC OPTIONS (description = 'retrieved from “profile_total_revenue_usd” for Adapty events'),
  cancellation_reason STRING OPTIONS (description = 'retrieved from “cancellation_reason” for Adapty events'),
  state STRING OPTIONS (description = 'updated with trigger functions'),
);
