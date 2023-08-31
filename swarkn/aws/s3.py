import boto3
import pyarrow as pa
import pyarrow.dataset as ds

def storage_options(session: boto3.Session = None) -> dict:
    """
    convenience function used in DeltaTable
    """
    if not session:
        boto3.setup_default_session()
        session = boto3.DEFAULT_SESSION
    creds = session.get_credentials()

    storage_options = dict(
        AWS_REGION=session.region_name,
        AWS_ACCESS_KEY_ID=creds.access_key,
        AWS_SECRET_ACCESS_KEY=creds.secret_key,
        AWS_S3_ALLOW_UNSAFE_RENAME='true'
    )
    return storage_options

def parquet_dataset(path: str) -> ds.Dataset:
    return ds.dataset(path.replace('s3://', ''),
        format="parquet",
        filesystem=pa.fs.S3FileSystem(),
        partitioning="hive"
    )