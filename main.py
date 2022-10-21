from google.cloud import bigquery
import pandas as pd
import sys
import os
import re

sys.path.append('..')


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append('..')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.join(
    BASE_DIR, 'credentials/gcp_credentials.json')


def create_gcp_dataset(client):
    dataset_id = f'{client.project}.scraper_query_price'
    dataset = bigquery.Dataset(dataset_id)

    dataset.location = 'US'

    dataset = client.create_dataset(dataset, timeout=30)  # make an API request
    return f'Created a dataset {client.project}.{dataset.dataset_id}'


def create_bigquery_table(df, dataset_tablename, gcp_project_name):
    # df: your pandas dataframe
    # dataset_tablename (str.str): dataset_name.tablename
    # gcp_project_name: GCP Project ID

    df.to_gbq(
        destination_table=dataset_tablename,
        project_id=gcp_project_name,
        if_exists="replace",  # 3 available methods: fail/replace/append
        progressbar=True
    )


def remove_char(text: str):
    try:
        new_text = re.sub(r'[:\s0-9]+', '_', text)
        return new_text
    except:
        return text

if __name__ == '__main__':
    # project definitions
    project_id = 'study-gbq-ear'
    dataset_id = 'scraper_query_price'
    table_id = 'scraped_cars'

    # object creation
    client = bigquery.Client(project=project_id)

    # msg = create_gcp_dataset(client)

    # generate data
    dataset = pd.read_csv('consolidate.csv', encoding='utf-8')
    columns = dataset.columns
    
    new_columns = []
    for column in columns:
        c = remove_char(column)
        new_columns.append(c)    
    
    dataset.columns.values[:] = new_columns

    dataset.to_gbq(
                destination_table=f'{project_id}.{dataset_id}.{table_id}', 
                project_id=project_id, 
                chunksize=None,
                if_exists='append',
                progress_bar=True
    )
    print(f'Dados inseridos com sucesso em {project_id}.{dataset_id}.{table_id}')
