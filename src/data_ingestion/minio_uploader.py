from minio import Minio
from minio.error import S3Error
import os

def upload_to_minio(data: str, bucket_name: str, object_name: str):
    client = Minio(
        endpoint="minio:9000",
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )
    
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    
    data_bytes = data.encode('utf-8')
    client.put_object(
        bucket_name,
        object_name,
        data=BytesIO(data_bytes),
        length=len(data_bytes),
        content_type='application/csv'
    )
    print(f"Uploaded {object_name} to {bucket_name}")