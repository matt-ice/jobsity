import boto3
import tempfile


def get_s3_file(filepath):
    # Create a connection to s3, create variable for the file
    s3 = boto3.resource('s3', region_name='x-x-x')
    bucket = s3.Bucket('bucket')
    object = bucket.Object('filepath')

    # Create temp file to store the file and load the file itself into the variable
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'wb') as f:
        object.download_fileobj(f)

    return tmp
