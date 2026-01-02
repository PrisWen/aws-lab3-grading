import json
import boto3
import os
from io import BytesIO
from PIL import Image, ExifTags, TiffImagePlugin
from botocore.exceptions import ClientError
from datetime import datetime
from pathlib import Path


s3 = boto3.client("s3")
OUTPUT_PREFIX = os.environ["OUTPUT_PREFIX"]  # Should be "metadata"


def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            message = json.loads(record["body"])
            bucket = message["bucket"]
            key = message["key"]
            etag = message.get("etag")
            filename = Path(key).name

            metadata_key = f"{OUTPUT_PREFIX}/{filename}.json"

            # -------------------------------
            # Idempotency check
            # -------------------------------
            if metadata_exists(bucket, metadata_key):
                print(f"Metadata already exists, skipping: {metadata_key}")
                continue

            # -------------------------------
            # Download image
            # -------------------------------
            image_obj = s3.get_object(Bucket=bucket, Key=key)
            image_bytes = image_obj["Body"].read()
            file_size = image_obj["ContentLength"]

            image = Image.open(BytesIO(image_bytes))

            # -------------------------------
            # Extract metadata
            # -------------------------------
            metadata = {
                "source_bucket": bucket,
                "source_key": key,
                "etag": etag,
                "format": image.format,
                "width": image.width,
                "height": image.height,
                "file_size_bytes": file_size,
                "exif": extract_exif(image) or {},  # Ensure {} if None
                "processed_at": datetime.utcnow().isoformat()
            }

            # -------------------------------
            # Write metadata JSON to S3
            # -------------------------------
            s3.put_object(
                Bucket=bucket,
                Key=metadata_key,
                Body=json.dumps(metadata, indent=2),
                ContentType="application/json"
            )

            print(f"Metadata written to: {metadata_key}")

        except Exception as e:
            print("Failed processing message")
            print(e)
            raise e  # Let Lambda retry via SQS DLQ or visibility timeout

    return {"statusCode": 200}


def metadata_exists(bucket, key):
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise e


def extract_exif(image):
    """
    Extract EXIF data and recursively convert IFDRational, bytes, and nested structures
    to JSON-serializable types.
    """
    try:
        exif_raw = image._getexif()
        if not exif_raw:
            return None

        def convert_value(val):
            if isinstance(val, TiffImagePlugin.IFDRational):
                return float(val)
            elif isinstance(val, bytes):
                try:
                    return val.decode("utf-8", errors="ignore")
                except Exception:
                    return str(val)
            elif isinstance(val, dict):
                return {k: convert_value(v) for k, v in val.items()}
            elif isinstance(val, (list, tuple)):
                return [convert_value(v) for v in val]
            else:
                return val

        exif_clean = {}
        for tag_id, value in exif_raw.items():
            tag_name = ExifTags.TAGS.get(tag_id, tag_id)
            exif_clean[tag_name] = convert_value(value)

        return exif_clean

    except Exception as e:
        print(f"Error extracting EXIF: {e}")
        return None