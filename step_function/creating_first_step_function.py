# import json
# import boto3
# import time


# #maxitteratorcount = 0
# def bucketobjcount ():
#     print("i am in bucket")
#     s3_resource = boto3.resource('s3')
#     s3_client = boto3.client('s3')
#     bucket = s3_resource.Bucket('td-arpit-learning-s3')
#     count_obj = 0
#     for obj in bucket.objects.filter(Prefix = 'input/'):
#         count_obj = count_obj + 1
#     # print (count_obj)
#     print("number of object in bucket {}".format(count_obj))
#     return count_obj

# # print("find number of object in bucket folder")
# # maxitercount = bucketobjcount()
# # print("number of object in bucket {}".format(maxitercount))

# def processing(event):
#     s3_resource = boto3.resource('s3')
#     s3_client = boto3.client('s3')
#     bucket = s3_resource.Bucket('td-arpit-learning-s3')
#     # print(f'itteration count is {event['iterationcount']} and max count is {event['maxitercount']}')

#     if event['iterationcount'] <= event['maxitercount']:
#         print("interation not completed and count is {}".format(event['iterationcount']))
#         for obj in bucket.objects.filter(Prefix = 'input/'):
#             split_key = obj.key.split("/",2)
#             file_name = split_key[-1]
#             print(file_name)
#             copy_source={'Bucket':'td-arpit-learning-s3',
#                           'Key':obj.key}
#             s3_client.copy_object(CopySource =copy_source, Key ='output/'+file_name,Bucket = 
#             'td-arpit-learning-s3')
#             print("copied file")
#             s3_client.delete_object (Bucket ='td-arpit-learning-s3',Key = obj.key)

#             print("delete file")
#             event['IsComplete'] = False
#             return event
#     else:
#         print("Iteration completed now.. and count is: ",event['iterationcount'])
#         event['IsComplete'] = True
#         return event
    
# def lambda_handler(event, context):
#     print("==0",event)
#     if 'iterationcount' in event.keys():
#         print("Lamnda_handler,Now...")
#         event['iterationcount'] += 1
#         print("==1",event)
#         return processing(event)
#     else:
#         print("In else lambda_handler now ")
#         event['maxitercount'] = bucketobjcount()
#         event['iterationcount'] = 1
#         print("==2",event)
#         return processing(event)

# event = {'key1': 'value1', 'key2': 'value2', 'key3': 'value3'}
# lambda_handler(event,'')




import boto3


#maxitteratorcount = 0
def bucketobjcount ():
    print("i am in bucket")
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket('td-arpit-learning-s3')
    # count_obj = 0
    # for _ in bucket.objects.filter(Prefix = 'input/'):
    #     count_obj = count_obj + 1
    count_obj = sum(1 for _ in bucket.objects.filter(Prefix='input/'))
    return count_obj

max_iter_count = bucketobjcount()
print("number of object in bucket {}".format(max_iter_count))

def processing(event):
    s3_client = boto3.client('s3')
    bucket_name = 'td-arpit-learning-s3'
    bucket_prefix = 'input/'

    if event['iterationcount'] <= max_iter_count:
        print("interation not completed and count is {}".format(event['iterationcount']))
        # for obj in s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix).get('contents'):
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=bucket_prefix)
        contents = response.get('Contents', [])  # Get the list of objects or an empty list if None
        print("response == ", response)
        print("content == ", contents)
        for obj in contents[1:]:
            print("obj == ", obj)
            key = obj['Key']
            file_name = key.split("/")[-1]
            print(file_name)
            copy_source={'Bucket':bucket_name,
                        'Key':key}
            s3_client.copy_object(CopySource =copy_source, Key ='output/'+file_name,Bucket = bucket_name)
            print("file has copied")
            s3_client.delete_object (Bucket =bucket_name,Key = key)
            print("file has deleted")
            event['IsComplete'] = False
            return event
        event['IsComplete'] = True
        return event
    else:
        print("Iteration completed now.. and count is: ",event['iterationcount'])
        event['IsComplete'] = True
        return event
    
def lambda_handler(event, context):
    if 'iterationcount' in event.keys():
        print("Lamnda_handler,Now...")
        event['iterationcount'] += 1
        print("==1",event)
        return processing(event)
    else:
        print("In else lambda_handler now ")
        event['iterationcount'] = 1
        print("==2",event)
        return processing(event)



# response ==  {'ResponseMetadata': {'RequestId': '9KJE3095NH6TWHWM', 'HostId': '8MnDf4XimdBLT0+il/Vwuq62uyDBkrU/jyXu6ofS4GE9z+glXiivpDxJazrCeAbISL5q0H1AqnM=', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amz-id-2': '8MnDf4XimdBLT0+il/Vwuq62uyDBkrU/jyXu6ofS4GE9z+glXiivpDxJazrCeAbISL5q0H1AqnM=', 'x-amz-request-id': '9KJE3095NH6TWHWM', 'date': 'Fri, 08 Mar 2024 08:06:00 GMT', 'x-amz-bucket-region': 'ap-south-1', 'content-type': 'application/xml', 'transfer-encoding': 'chunked', 'server': 'AmazonS3'}, 'RetryAttempts': 1}, 'IsTruncated': False, 'Contents': [{'Key': 'input/', 'LastModified': datetime.datetime(2024, 3, 8, 7, 45, 46, tzinfo=tzlocal()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD'}, {'Key': 'input/test_1', 'LastModified': datetime.datetime(2024, 3, 8, 7, 46, 6, tzinfo=tzlocal()), 'ETag': '"6626e89af3e50b5e7cdd7dcb0b46d6cd"', 'Size': 104, 'StorageClass': 'STANDARD'}, {'Key': 'input/test_2', 'LastModified': datetime.datetime(2024, 3, 8, 7, 46, 6, tzinfo=tzlocal()), 'ETag': '"545f34979c54678f48ca60e22d0cad9d"', 'Size': 70, 'StorageClass': 'STANDARD'}, {'Key': 'input/test_s3', 'LastModified': datetime.datetime(2024, 3, 8, 7, 46, 7, tzinfo=tzlocal()), 'ETag': '"fcac9050c57a63b18ff8dabf8114fe39"', 'Size': 92, 'StorageClass': 'STANDARD'}], 'Name': 'td-arpit-learning-s3', 'Prefix': 'input/', 'MaxKeys': 1000, 'EncodingType': 'url', 'KeyCount': 4}

# content ==  [{'Key': 'input/', 'LastModified': datetime.datetime(2024, 3, 8, 7, 45, 46, tzinfo=tzlocal()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD'}, {'Key': 'input/test_1', 'LastModified': datetime.datetime(2024, 3, 8, 7, 46, 6, tzinfo=tzlocal()), 'ETag': '"6626e89af3e50b5e7cdd7dcb0b46d6cd"', 'Size': 104, 'StorageClass': 'STANDARD'}, {'Key': 'input/test_2', 'LastModified': datetime.datetime(2024, 3, 8, 7, 46, 6, tzinfo=tzlocal()), 'ETag': '"545f34979c54678f48ca60e22d0cad9d"', 'Size': 70, 'StorageClass': 'STANDARD'}, {'Key': 'input/test_s3', 'LastModified': datetime.datetime(2024, 3, 8, 7, 46, 7, tzinfo=tzlocal()), 'ETag': '"fcac9050c57a63b18ff8dabf8114fe39"', 'Size': 92, 'StorageClass': 'STANDARD'}]
# obj ==  {'Key': 'input/', 'LastModified': datetime.datetime(2024, 3, 8, 7, 45, 46, tzinfo=tzlocal()), 'ETag': '"d41d8cd98f00b204e9800998ecf8427e"', 'Size': 0, 'StorageClass': 'STANDARD'}