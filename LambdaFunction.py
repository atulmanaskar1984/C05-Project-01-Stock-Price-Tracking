import base64
import json

print('Loading function')


def lambda_handler(event, context):
    # print("Received event: " + json.dumps(event, indent=2))
    for record in event['Records']:
        # Kinesis data is base64 encoded so decode here
        print("Raw data:" + record['kinesis']['data'])
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        print("Decoded payload: " + payload)
        print(type(payload))

        payload_json = json.loads(payload)

        for k, v in payload_json.items():
            if k != "stockid" and k != "52WeeksHigh" and k != "52WeeksLow":
                print(f'For company {payload_json["stockid"]} the price of stock at {k} is {payload_json[k]}')
                if payload_json[k] > int(payload_json["52WeeksHigh"]) * 0.60:
                    print(
                        f'For company {payload_json["stockid"]} the price of stock at {k} is 80% of 52 week high price {payload_json["52WeeksHigh"]}')
                if payload_json[k] < int(payload_json["52WeeksLow"]) * 0.10:
                    print(
                        f'For company {payload_json["stockid"]} the price of stock at {k} is 20% of 52 week low price {payload_json["52WeeksLow"]}')

    return 'Successfully processed {} records.'.format(len(event['Records']))
