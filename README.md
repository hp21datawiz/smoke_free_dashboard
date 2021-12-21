# smoke_free_dashboard
## gcp function for smoke free dashboard

We use a few different GCP services in order to prepare the data for the dashboard.

## Cloud Functions 

The Python scripts that pull and aggregate data are Cloud Functions. They're serverless so we don't have to pay for storage when the functions aren't in use.

## PubSub
PubSub sends and recieves messages between services. We use it to trigger the cloud functions.

## Cloud Scheduler
Cloud Scheduler is where we schedule the PubSub messages which then trigger the Functions. .

## BigQuery
BigQuery is a managed data warehouse where some of the data we're working with is stored.