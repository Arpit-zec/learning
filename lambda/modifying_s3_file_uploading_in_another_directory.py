import boto3

def lambda_handler(event, context):
    # Replace 'your_bucket_name' and 'your_file_name.txt' with your actual bucket and file names
    bucket_name = 'lambda-arpit'
    file_name = 'test_s3'

    destination_bucket_name = 'lambda-arpit'
    destination_folder_name = 'new_files_modifies'
    destination_file_name = 'modified_file.txt'
    
    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        # Read the file from S3
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        content = response['Body'].read().decode('utf-8')

        # Process the content (you can modify this part based on your use case)
        print("File Content:")
        print(content)
        content = content + "THIS IS MODIFIED CONTENT\nWE ARE TRYING TO MAKE SOME CHANGES INTO FILE\nSO NOW WE HAVE SUCCESSFULLY COMPLETED OUR MODIFIED CONTENT TO SAVE INTO NEW FULE IN s3"
        
        destination_object_key = f'{destination_folder_name}/{destination_file_name}'
        
        s3.put_object(Bucket=destination_bucket_name, Key=destination_object_key, Body=content.encode('utf-8'))

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