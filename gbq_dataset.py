from google_cloud import bigquery

# in google cloud set this configuration
# gcloud config set core/project YOUR_PROJECT_NAME

# dataset = pd.read_csv('quered_car.csv')

PROJECT_NAME = 'Study-GBQ-EAR'
# upload to BigQuery
client = bigquery.Client(project=PROJECT_NAME)


# study-gbq-ear.crawler_ear.consolidate_data
table_ref = client.dataset("files").table("consolidate_data")

job_config = bigquery.LoadJobConfig()
job_config.source_format = bigquery.SourceFormat.CSV
job_config.skip_leading_rows = 1 # ignore the header
job_config.autodetect = True

with open("quered_car.csv", "rb") as source_file:
    job = client.load_table_from_file(
        source_file, table_ref, job_config=job_config
    )

# job is async operation so we have to wait for it to finish
job.result()
