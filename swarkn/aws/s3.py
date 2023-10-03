import boto3
import pyarrow as pa
import pyarrow.dataset as ds

def session_storage_options(session: boto3.Session = None) -> dict:
    """
    convenience function used in DeltaTable
    """
    if not session:
        boto3.setup_default_session()
        session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()

    storage_options = dict(
        AWS_REGION=session.region_name,
        AWS_ACCESS_KEY_ID=creds.access_key,
        AWS_SECRET_ACCESS_KEY=creds.secret_key,
        AWS_S3_ALLOW_UNSAFE_RENAME='true'
    )
    if creds.token:
        storage_options['AWS_SESSION_TOKEN'] = creds.token

    return storage_options

def parquet_dataset(path: str) -> ds.Dataset:
    return ds.dataset(path.replace('s3://', ''),
        format="parquet",
        filesystem=pa.fs.S3FileSystem(),
        partitioning="hive"
    )