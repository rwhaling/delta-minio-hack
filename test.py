import boto3
from botocore.exceptions import ClientError
import fire
import random
import string
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa
from pyarrow import fs
import os
import duckdb

class MinioTest:
    def __init__(self):
        # MinIO connection settings
        self.endpoint_url = 'http://localhost:9000'
        self.access_key = 'minioadmin'
        self.secret_key = 'minioadmin'
        self.bucket_name = 'test-bucket'

        # Create a boto3 client for S3 with MinIO configuration
        self.s3_client = boto3.client('s3',
                                      endpoint_url=self.endpoint_url,
                                      aws_access_key_id=self.access_key,
                                      aws_secret_access_key=self.secret_key,
                                      config=boto3.session.Config(signature_version='s3v4'))

        # Ensure bucket exists
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        try:
            self.s3_client.create_bucket(Bucket=self.bucket_name)
            print(f"Bucket '{self.bucket_name}' created successfully")
        except ClientError as e:
            if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                print(f"Bucket '{self.bucket_name}' already exists")
            else:
                print(f"Error creating bucket: {e}")
                exit(1)

    def create_file(self, file_name='empty_file.txt'):
        """Create an empty file in the MinIO bucket."""
        try:
            self.s3_client.put_object(Bucket=self.bucket_name, Key=file_name, Body='')
            print(f"Empty file '{file_name}' created successfully in bucket '{self.bucket_name}'")
        except ClientError as e:
            print(f"Error creating file: {e}")

    def list_contents(self):
        """List the contents of the MinIO bucket."""
        try:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket_name)
            print(f"\nContents of bucket '{self.bucket_name}':")
            if 'Contents' in response:
                for obj in response['Contents']:
                    print(f"- {obj['Key']}")
            else:
                print("The bucket is empty.")
        except ClientError as e:
            print(f"Error listing bucket contents: {e}")

class DeltaLakeTest:
    def __init__(self):
        self.endpoint_url = 'http://localhost:9000'
        self.access_key = 'minioadmin'
        self.secret_key = 'minioadmin'
        self.bucket_name = 'test-bucket'

        # Create a boto3 session
        self.session = boto3.Session(
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key
        )
        self.credentials = self.session.get_credentials()
        self.current_credentials = self.credentials.get_frozen_credentials()

    def generate_random_string(self, length=5):
        return ''.join(random.choices(string.ascii_lowercase, k=length))

    def write_delta_table(self, num_rows=10, table_name='test_delta_table'):
        """Write a small 2-column Delta table with random strings."""
        # Generate data
        keys = [self.generate_random_string() for _ in range(num_rows)]
        values = [self.generate_random_string() for _ in range(num_rows)]

        # Create PyArrow table
        table = pa.table([keys, values], names=['key', 'value'])

        table_path = f"s3://{self.bucket_name}/{table_name}"
        
        print(f"Writing Delta table to: {table_path}")

        storage_options = {
            "AWS_ACCESS_KEY_ID": self.current_credentials.access_key,
            "AWS_SECRET_ACCESS_KEY": self.current_credentials.secret_key,
            "AWS_ENDPOINT_URL": self.endpoint_url,
            "AWS_REGION": "us-east-1",
            "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
        }
        
        try:
            # Check if MinIO is accessible
            s3 = self.session.client('s3', endpoint_url=self.endpoint_url)
            s3.list_buckets()
            print("Successfully connected to MinIO")

            # Check if the bucket exists
            buckets = s3.list_buckets()['Buckets']
            if not any(bucket['Name'] == self.bucket_name for bucket in buckets):
                print(f"Bucket {self.bucket_name} does not exist. Creating it...")
                s3.create_bucket(Bucket=self.bucket_name)

            # Try writing to S3
            write_deltalake(
                table_path,
                table,
                mode="overwrite",
                storage_options=storage_options
            )
            print(f"Successfully wrote Delta table to {table_path}")

            # Read and print the table metadata
            dt = DeltaTable(table_path, storage_options=storage_options)
            print(f"Table metadata:\n{dt.metadata()}")
            print(f"Table schema:\n{dt.schema().json()}")
            print(f"Table version: {dt.version()}")

        except Exception as e:
            print(f"Error writing Delta table: {e}")
            print("Attempting to write to local filesystem instead...")
            
            # Try writing to local filesystem
            local_path = os.path.join(os.getcwd(), table_name)
            write_deltalake(local_path, table, mode="overwrite")
            print(f"Successfully wrote Delta table to local path: {local_path}")

            # Read and print the local table metadata
            dt = DeltaTable(local_path)
            print(f"Local table metadata:\n{dt.metadata()}")
            print(f"Local table schema:\n{dt.schema().json()}")
            print(f"Local table version: {dt.version()}")

    def query_delta_table(self, table_name='test_delta_table', query=None):
        """Query the local Delta table using DuckDB."""
        local_path = os.path.join(os.getcwd(), table_name)
        
        try:
            # Create a DuckDB connection
            con = duckdb.connect(database=':memory:')
            
            # Install and load the necessary extensions
            con.install_extension("parquet")
            con.load_extension("parquet")
            
            # Read the local Delta table
            con.execute(f"CREATE VIEW delta_view AS SELECT * FROM delta_scan('{local_path}')")
            
            # Execute the query
            if query is None:
                query = "SELECT * FROM delta_view LIMIT 5"
            
            result = con.execute(query).fetchall()
            
            print(f"Query result:")
            for row in result:
                print(row)
            
        except Exception as e:
            print(f"Error querying Delta table: {e}")
            print(f"Make sure the Delta table exists at: {local_path}")

class CLI:
    def __init__(self):
        self.minio = MinioTest()
        self.delta = DeltaLakeTest()

    def query_table(self, table_name='test_delta_table', query=None):
        """CLI command to query the local Delta table."""
        self.delta.query_delta_table(table_name, query)

if __name__ == '__main__':
    fire.Fire(CLI)