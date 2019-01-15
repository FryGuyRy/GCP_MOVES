BUCKETNAME = '' # Just Root Bucket Name not ensuing path
FILENAME = 'FILENAME.csv.zip' # Original File name, to be moved from Storage to local and back before loading to BQ
DIRECTORY = 'delve strategic/' # Directory location/path after root bucket name
UNZIP = True #'True' to unzip 'False' if not compressed

DATASET_ID = '' # Dataset Id is the BQ "Database/Partition"
URI = 'gs:// BUCKETNAME/DIRECTORY/' # Root bucketname and directory path in GCS
TABLE = 'OUTPUT_TABLENAME' # BQ Final Table Name