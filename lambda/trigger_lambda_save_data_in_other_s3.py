import json
import boto3

def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client("s3")

    # Get the details of the uploaded file from the S3 event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Download the content of the uploaded file
    response = s3.get_object(Bucket=bucket, Key=key)
    content = response['Body'].read().decode('utf-8')

    # Modify the content as needed
    modified_content = "Modified: this content is added ------------------------" + content  +"--"*10 # Example modification, you can customize this

    # Get the details of the existing file in another S3 bucket
    existing_bucket = "td-arpit-learning"
    existing_key = "test_s3"
    
    # Download the content of the existing file
    existing_response = s3.get_object(Bucket=existing_bucket, Key=existing_key)
    existing_content = existing_response['Body'].read().decode('utf-8')

    # Combine the content of the existing file with the modified content
    combined_content = existing_content + "\n\n" + modified_content

    # Save the combined content to a new file in the same bucket
    new_key = "test_s3"  # Replace with your desired path and filename
    s3.put_object(Bucket=existing_bucket, Key=new_key, Body=combined_content.encode('utf-8'))

    return {
        'statusCode': 200,
        'body': json.dumps('File modification and saving completed!')
    }
