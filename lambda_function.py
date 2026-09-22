import json
import boto3
import urllib.parse

# Initialize the S3 Client
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # 1. Parse incoming S3 Event payload records safely
        records = event.get('Records', [])
        if not records:
            print("[WARN] No records found in the incoming event payload.")
            return {'statusCode': 400, 'body': 'Empty records.'}
            
        record = records[0]
        bucket_name = record['s3']['bucket']['name']
        file_key = urllib.parse.unquote_plus(record['s3']['object']['key'], encoding='utf-8')
        
        print(f"[INFO] Event triggered. Processing resource: s3://{bucket_name}/{file_key}")
        
        # 2. Extract payload content from landing zone
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        raw_text_data = response['Body'].read().decode('utf-8')
        
        print(f"[SUCCESS] Payload ingested successfully. Data stream preview: {raw_text_data[:100]}")
        
        # TODO: Implement secure API dispatch handover to Databricks/Snowflake
        return {
            'statusCode': 200,
            'body': json.dumps('Data stream processing complete.')
        }
        
    except Exception as e:
        print(f"[ERROR] Failed to orchestrate ingestion stream: {str(e)}")
        raise e
