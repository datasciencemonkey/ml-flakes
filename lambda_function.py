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
        
        request = json.loads(json.dumps(event))
        #instantiating a list to collect payload body as list
        payload_data = []
        #level encode the cat variable
        lookup_dict = {"F":0,"I":1,"M":2}
        
        for i in request["instances"]:
            #stringify the features
            str_features = [str(x) for x in i["features"]]
            #KV lookup for cat var
            str_features[0] = lookup_dict[str_features[0]]
            #append to list
            payload_data.append(",".join(str(y) for y in str_features))
        
        print(f"Payload data is sent as {payload_data}")
    
        # XGBoost accepts data in CSV. It does not support JSON.
        # So, we need to submit the request in CSV format
        # Prediction for multiple observations in the same call
        result = client.invoke_endpoint(EndpointName=ENDPOINT_NAME, 
                               Body=('\n'.join(payload_data).encode('utf-8')),
                               ContentType='text/csv')
                               
    
        result = result['Body'].read().decode('utf-8')
        
        # Apply inverse transformation to get the rental count
        print(result)
        result = result.split(',')
        predictions = [float(r) for r in result]
        
        return {
            'statusCode': 200,
            'isBase64Encoded':False,
            'body': json.dumps(predictions)
        }

    except Exception as err:
        return {
            'statusCode': 400,
            'isBase64Encoded':False,
            'body': 'Call Failed {0}'.format(err)
        }
