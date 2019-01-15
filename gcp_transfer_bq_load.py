VS Code studio
Atom

from zipfile import ZipFile
from google.cloud import storage
from google.cloud import bigquery
import time
from time import sleep

from gcp_transfer_bq_load_config import *


def file_local_to_gcs_bucket(bucketname, filename, directory, unzip=True):
    storage_client = storage.Client()
    #Only Root Bucket, Subsequent directories and filename in next line
    bucket = storage_client.get_bucket(bucketname)
    #replace in between quotes with post root path and filename
    blob = bucket.blob(directory+filename)
    blob.download_to_filename(filename)
    print('File Moved to Shell')
    
    t=time.asctime(time.localtime(time.time()))
    time.sleep(40)

    if unzip:
        zip_file = ZipFile(filename, 'r')
        zip_file.extractall('.')
        print('File Unzipped')
        blob_upload = bucket.blob(
            directory+filename.replace('.zip',''))
        blob_upload.upload_from_filename(filename.replace('.zip', ''))
        print('File Move Complete, Starting BQ Load')
    else:
        print('File not unzipped')
        blob_upload = bucket.blob(
            directory+filename)
        blob_upload.upload_from_filename(filename)
        print('File Move Complete, Starting BQ Load')
    pass

################################
##Start Big Query Load Commands
################################
def file_gcs_to_bigquery(dataset_id, uri, filename, table):
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bigquery.SourceFormat.CSV
    load_job = client.load_table_from_uri(
        uri,
        dataset_ref.table(table),  # Name of Final Table
        job_config=job_config)  # API request
    print('Starting job {}'.format(load_job.job_id))
    load_job.result()  # Waits for table load to complete.
    print('Job finished.')
    destination_table = client.get_table(dataset_ref.table(table)) # Name of Final Table
    print('Loaded {} rows.'.format(destination_table.num_rows))
    pass


if __name__ == "__main__":
    file_local_to_gcs_bucket(bucketname=BUCKETNAME,
                             filename=FILENAME, directory=DIRECTORY,
                             unzip=True)
    f = FILENAME.replace('.zip', '')
    file_gcs_to_bigquery(dataset_id=DATASET_ID, uri=URI, filename=f, table=TABLE)