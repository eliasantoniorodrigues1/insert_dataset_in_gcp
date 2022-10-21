from google.cloud import bigquery
import pandas as pd
import sys
import os

from gbq_dataset import PROJECT_NAME

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append('..')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(BASE_DIR, 'credentials/gcp_credentials.json')

def create_gcp_dataset(client):
    dataset_id = f'{client.project}.scraper_query_price'
    dataset = bigquery.Dataset(dataset_id)

    dataset.location = 'US'

    dataset = client.create_dataset(dataset, timeout=30) # make an API request
    return f'Created a dataset {client.project}.{dataset.dataset_id}'




if __name__ == '__main__':
    project_name = 'study-gbq-ear'
    client = bigquery.Client(project=project_name)
    
    # dataset = pd.read_csv('quered_car.csv')
    

