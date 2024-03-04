
import boto3

def lambda_handler(event, context):
    # Replace 'your_bucket_name' and 'your_file_name.txt' with your actual bucket and file names
    bucket_name = 'lambda-arpit'
    
    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        
        object_key = "new_files/destination_file_name.txt"
        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')

        # Process the content (you can modify this part based on your use case)
        print("File Content:")
        print(content)
        

        # You can return the content or perform other operations as needed
        return {
            'statusCode': 200,
            'body': content
        }
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }
