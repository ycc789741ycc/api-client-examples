import json
import logging

import boto3
from botocore.exceptions import ClientError, NoCredentialsError, PartialCredentialsError


class S3Client:
    """S3 Client for AWS S3."""

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        region: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        """Initialize the S3 client.

        Args:
            access_key (str): AWS access key
            secret_key (str): AWS secret key
            region (str | None, optional): AWS region. Defaults to None.
            endpoint_url (str | None, optional): Custom endpoint URL. Defaults to None.

        Raises:
            NoCredentialsError: If credentials are missing
            PartialCredentialsError: If credentials are incomplete

        """
        try:
            self.s3 = boto3.client(
                "s3",
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
                endpoint_url=endpoint_url,
            )
            logging.info("S3 Client initialized successfully.")
        except (NoCredentialsError, PartialCredentialsError) as e:
            logging.error(f"Credentials error: {e}")
            raise e

    def upload_file(self, file_path: str, bucket_name: str, s3_key: str) -> None:
        """Upload a file to S3.

        Args:
            file_path (str): Path to the local file
            bucket_name (str): S3 Bucket name
            s3_key (str): Destination path in S3 (object key)

        Raises:
            Exception: If upload fails

        """
        try:
            self.s3.upload_file(file_path, bucket_name, s3_key)
            logging.info(f"File {file_path} uploaded to S3://{bucket_name}/{s3_key}")
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            raise e

    def upload_file_from_bytes(self, file_bytes: bytes, bucket_name: str, s3_key: str) -> None:
        """Upload a file to S3 from bytes.

        Args:
            file_bytes (bytes): Bytes of the file
            bucket_name (str): S3 Bucket name
            s3_key (str): Destination path in S3 (object key)

        Raises:
            Exception: If upload fails

        """
        try:
            self.s3.put_object(Bucket=bucket_name, Key=s3_key, Body=file_bytes)
            logging.info(f"File uploaded to S3://{bucket_name}/{s3_key}")
        except Exception as e:
            logging.error(f"Upload failed: {e}")
            raise e

    def get_file_content(self, bucket_name: str, s3_key: str) -> bytes:
        """Get file content directly from S3 without downloading to local storage.

        Args:
            bucket_name (str): S3 Bucket name
            s3_key (str): Path in S3

        Returns:
            bytes: File content as bytes

        Raises:
            Exception: If file retrieval fails

        """
        try:
            response = self.s3.get_object(Bucket=bucket_name, Key=s3_key)
            content = response["Body"].read()
            logging.info(f"File content retrieved from S3://{bucket_name}/{s3_key}")
            return content
        except Exception as e:
            logging.error(f"Get file content failed: {e}")
            raise e

    def list_buckets(self) -> list[str]:
        """List all buckets.

        Returns:
            list[str]: List of bucket names

        Raises:
            Exception: If listing buckets fails

        """
        buckets = []
        try:
            response = self.s3.list_buckets()
            buckets = [bucket["Name"] for bucket in response["Buckets"]]
            logging.info(f"Buckets: {buckets}")
        except Exception as e:
            logging.error(f"List buckets failed: {e}")
            raise e
        return buckets

    def list_objects(self, bucket_name: str, prefix: str = "") -> list[str]:
        """List objects in an S3 bucket.

        Args:
            bucket_name (str): S3 Bucket name
            prefix (str, optional): Optional prefix filter. Defaults to "".

        Returns:
            list[str]: List of object keys

        Raises:
            Exception: If listing objects fails

        """
        try:
            response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            objects = response.get("Contents", [])
            file_list = [obj["Key"] for obj in objects]
            logging.info(f"Files in S3://{bucket_name}/{prefix}: {file_list}")
            return file_list
        except Exception as e:
            logging.error(f"List objects failed: {e}")
            raise e

    def delete_object(self, bucket_name: str, s3_key: str) -> None:
        """Delete an object from S3.

        Args:
            bucket_name (str): S3 Bucket name
            s3_key (str): Object key in S3

        Raises:
            Exception: If deletion fails

        """
        try:
            self.s3.delete_object(Bucket=bucket_name, Key=s3_key)
            logging.info(f"Deleted S3://{bucket_name}/{s3_key}")
        except Exception as e:
            logging.error(f"Delete failed: {e}")
            raise e

    def create_bucket(self, bucket_name: str) -> None:
        """Create an S3 bucket.

        Args:
            bucket_name (str): Name of the bucket

        Raises:
            ClientError: If bucket creation fails

        """
        try:
            self.s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": self.s3.meta.region_name},
            )
            logging.info(f"Bucket '{bucket_name}' created successfully.")
        except ClientError as e:
            logging.error(f"Bucket creation failed: {e}")
            raise e

    def delete_bucket(self, bucket_name: str) -> None:
        """Delete an S3 bucket.

        Args:
            bucket_name (str): Name of the bucket

        Raises:
            ClientError: If bucket deletion fails

        """
        try:
            self.s3.delete_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' deleted successfully.")
        except ClientError as e:
            logging.error(f"Bucket deletion failed: {e}")
            raise e

    def delete_directory(self, bucket_name: str, directory: str) -> None:
        """Delete all objects under specified directory in S3 bucket.

        Args:
            bucket_name (str): S3 Bucket name
            directory (str): Directory path in S3 (prefix)

        Raises:
            Exception: If directory deletion fails

        """
        try:
            # Ensure directory ends with '/' if not empty
            if directory and not directory.endswith("/"):
                directory += "/"

            # List all objects under the directory
            objects = self.list_objects(bucket_name, prefix=directory)

            # Delete each object
            for obj_key in objects:
                self.delete_object(bucket_name, obj_key)

            logging.info(f"All objects under S3://{bucket_name}/{directory} deleted successfully.")
        except Exception as e:
            logging.error(f"Directory deletion failed: {e}")
            raise e


if __name__ == "__main__":
    import os
    access_key = os.getenv("AWS_ACCESS_KEY")
    secret_key = os.getenv("AWS_SECRET_KEY")
    bucket_name = os.getenv("AWS_BUCKET_NAME")
    s3 = S3Client(access_key=access_key, secret_key=secret_key)

    data = {'name': 'Bob', 'age': 69}
    s3.upload_file_from_bytes(json.dumps(data).encode('utf-8'), bucket_name, "test.json")
    print(s3.get_file_content(bucket_name, "test.json").decode('utf-8'))
