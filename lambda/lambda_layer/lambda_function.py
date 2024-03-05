import pandas as pd
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)

def lambda_handler(event, context):
    try:
        # Attempt to import Pandas
        # pd_version = pd.__version__
        print("Hello! I am pandas.")
        pd_val = pd.__name__
        pd_val = pd_val+"--"
        pd_val = pd_val*20

        # Log a success message
        print(f'{"#"*10}Pandas {pd_val} imported successfully!{"#"*10}')

        return {
            'statusCode': 200,
            'body': 'Lambda function executed successfully!'
        }

    except Exception as e:
        # Log an error message
        logging.error(f"Error importing Pandas: {str(e)}")

        # Return an error response
        return {
            'statusCode': 500,
            'body': 'Error executing Lambda function.'
        }
