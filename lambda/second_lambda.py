import json

def lambda_handler(event, context):
    
    cre = creator()
    pur = purpose()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!'),
        'creator': json.dumps(cre),
        'purpose': json.dumps(pur)
    }

def creator():
    return "arpit sahu"
    
def purpose():
    print("Hello!!!!")
    return "learning"

