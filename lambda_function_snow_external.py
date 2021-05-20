import boto3
import math
import dateutil
import json
import os

# grab environment variables
ENDPOINT_NAME = os.environ['ENDPOINT_NAME']
client = boto3.client(service_name='sagemaker-runtime')

def lambda_handler(event, context):
    try:
        print("Received event: " + json.dumps(event, indent=2))
        # request = json.loads(json.dumps(event))["body"]
        request = json.loads(event["body"])
        print(f"parsed body - {request}")
        #instantiating a list to collect payload body as list
        payload_data = []
        lookup_dict = {"F":0,"I":1,"M":2}
        for i in request['data']:
            i[1] = lookup_dict[i[1]]
            i=i[1:]
            stringifi = ",".join(str(j) for j in i)
            payload_data.append(stringifi)
        print(f"Payload data is sent as {payload_data}")
    
        # XGBoost accepts data in CSV. It does not support JSON.
        # So, we need to submit the request in CSV format
        # Prediction for multiple observations in the same call
        result = client.invoke_endpoint(EndpointName=ENDPOINT_NAME, 
                               Body=('\n'.join(payload_data).encode('utf-8')),
                               ContentType='text/csv')
                               
    
        result = result['Body'].read().decode('utf-8')
        
        print(result)
        result = result.split(',')
        predictions = [float(r) for r in result]
        
        pred_array = [[idx, i] for idx,i in enumerate(predictions)]
        json_to_return = json.dumps({"data":pred_array})
        
        return {
            "statusCode": 200,
            "body": json_to_return
            }

    except Exception as err:
        msg = []
        msg.append('Call Failed {0}'.format(err))
        json_to_return = json.dumps({"data":msg})
        return {
            "statusCode": 400,
            "body":json_to_return
            }
