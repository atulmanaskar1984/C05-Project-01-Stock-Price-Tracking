import base64
import json
import boto3
import datetime

print('Loading function')


def lambda_handler(event, context):
    sns_client = boto3.client('sns')
    dynamodb_client = boto3.client('dynamodb')

    notification = ''
    # print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        payload_json = json.loads(payload)

        for k, v in payload_json.items():
            if k != "stockid" and k != "52WeeksHigh" and k != "52WeeksLow":
                print(f'For company {payload_json["stockid"]} the price of stock at {k} is {payload_json[k]}')
                if payload_json[k] > int(payload_json["52WeeksHigh"]) * 0.60:
                    notification = f'For company {payload_json["stockid"]} the price of stock at {datetime.datetime.fromtimestamp(int(k) / 1000)} is 60% of 52 week high price {payload_json["52WeeksHigh"]}'
                if payload_json[k] < int(payload_json["52WeeksLow"]) * 0.10:
                    notification = f'For company {payload_json["stockid"]} the price of stock at {datetime.datetime.fromtimestamp(int(k) / 1000)} is 10% of 52 week low price {payload_json["52WeeksLow"]}'

                if notification != '':
                    dynamodb_client.put_item(
                        TableName='StockPricingDB',
                        Item={
                            'stockid': {
                                'S': payload_json["stockid"]
                            },
                            'datetime': {
                                'S': k
                            },
                            'notification': {
                                'S': notification
                            }
                        }
                    )

                    print("Record added to DynamoDB.")

                    response = sns_client.publish(
                        TargetArn="arn:aws:sns:us-east-1:760344735106:StockPricing-SNS",
                        Message=json.dumps({'default': notification}),
                        MessageStructure='json'
                    )
                    print(f'Response {response}')

                    notification = ''

    return 'Successfully processed {} records.'.format(len(event['Records']))


