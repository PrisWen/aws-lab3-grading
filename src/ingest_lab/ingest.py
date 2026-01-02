import json
import os
import urllib.parse
import boto3
from datetime import datetime

sqs = boto3.client("sqs")

QUEUE_URL = os.environ["QUEUE_URL"]
ALLOWED_EXTENSIONS = (".png", ".jpg", ".jpeg")

def lambda_handler(event, context):
  records = event.get("Records", [])
  
  if not records:
    print("No records found in event")
    return {
        "statusCode": 200, 
        "body": "No records to process"
    }
  
  for record in records:
    key = urllib.parse.unquote_plus(
      record["s3"]["object"]["key"]
    )

    if not key.lower().endswith(ALLOWED_EXTENSIONS):
      print("Skipping NOT valid Image:", key)
      continue

    bucket = record["s3"]["bucket"]["name"]
    etag = record["s3"]["object"].get("eTag")

    message = {
      "bucket": bucket,
      "key": key,
      "etag": etag,
      "timestamp": datetime.utcnow().isoformat()
    }

    sqs.send_message(
      QueueUrl=QUEUE_URL,
      MessageBody=json.dumps(message)
    )

    print("Message sent to SQS:", message)

  return {
    "statusCode": 200,
    "body": "Messages sent successfully"
  }