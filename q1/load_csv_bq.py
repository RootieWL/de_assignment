import os
import sys
import pandas as pd
from datetime import datetime
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import BadRequest
import params as params

# Start Time
def get_datetime():
    print('')
    print('----' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '----')
    print('')

def clean_csv(filename, date_time):

    d = {'keyword': 'brand', 'post_date': 'review_date', 
         'post_content': 'review_content', 'like_count': 'review_votes', 
         'comment_count':'review_stars'}
    
    df = pd.read_csv(filename+'.csv',encoding='utf-8')
    df.rename(columns=d, inplace=True)
    
    df['headline'] = df['review_content'].str.split('\n').str[0]
    df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
    df['review_date'] = df['review_date'].dt.date
    df['ingestion_date'] = datetime.now() #create date of ingestion
    
    df_sub = df[['brand','headline','review_date','review_content',
                 'review_votes','review_stars','ingestion_date']]
    df_out = df_sub.drop_duplicates()
    
    return df_out.to_csv('cleaned-'+ filename + '-' + date_time + '.csv',
                         header=True, sep='|', index=False, encoding='utf-8')
    

def load_csv_to_bq(project_name, dataset_name, table_name, uri):
    client = bigquery.Client()
    table = client.get_table(project_name + '.' + dataset_name + '.' + table_name)

    if 'detailed' in table_name:
        job_config = bigquery.LoadJobConfig(
            schema=[bigquery.SchemaField("brand", "STRING"),
                    bigquery.SchemaField("headline", "STRING"),
                    bigquery.SchemaField("review_date", "DATE"),
                    bigquery.SchemaField("review_content", "STRING"),
                    bigquery.SchemaField("review_votes", "INT64"),
                    bigquery.SchemaField("review_stars", "INT64"),
                    bigquery.SchemaField("ingestion_date", "DATETIME")],
            skip_leading_rows = 1,
            field_delimiter = '|',
            time_partitioning = bigquery.TimePartitioning(
                type_ = bigquery.TimePartitioningType.DAY,
                field = 'ingestion_date'))
    else:
        job_config = bigquery.LoadJobConfig(
            schema= table.schema,
            skip_leading_rows = 1,
            field_delimiter = '|')

    load_job = client.load_table_from_uri(uri, table, job_config = job_config)

    try:
        load_job.result()
        print("Loaded {} rows to {}.{}".format(load_job.output_rows, dataset_name, table_name))
    except BadRequest as e:
        for e in load_job.errors:
            print('ERROR: {}'.format(e['message']))


def main():
    get_datetime()
    datetime = params.date
    filename = params.csv_f
    bucket_dir = ' gs://' + params.bucket

    # cleans csv and move to GCS
    clean_csv(filename, datetime)
    os.system('gsutil mv ./cleaned-'+ filename + '-' + date_time + '.csv' + bucket_dir )
    
    csv_uri = bucket_dir + '/' + 'cleaned-'+ filename + '-' + date_time + '.csv'

    # Load csv to table
    if 'detailed' in params.tgt_table:
        partition_table = params.tgt_table + '_' + datetime.now().strftime("%Y_%m_%d")
        load_csv_to_bq(params.project, params.tgt_dataset, partition_table, csv_uri)
    else:
        load_csv_to_bq(params.project, params.tgt_dataset, params.tgt_table, csv_uri)
    
    print('Job Completed')
    get_datetime()

if __name__ == '__main__':
    main()