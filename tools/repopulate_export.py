import boto3
import json
from datetime import datetime, timedelta

#d_from = datetime(2023,11,17)
d_from = datetime(2023,11,20)
d_to = datetime(2023,11,27)

client = boto3.client("lambda")

# difference between current and previous date
delta = timedelta(hours=6)

# store the dates between two dates in a list
dates = []

while d_from <= d_to:
    # add current date to list by converting  it to iso format
    dates.append(d_from.isoformat())
    # increment start date by timedelta
    d_from += delta

for d in dates:

    print(f"updating for {d}")

    response = client.invoke(
        FunctionName = "ObsdecoderStack-exportLambda4C713529-2CRW7yYx5xzV",
        Payload = json.dumps(
            {'version': '0', 'id': 'd4891801-c257-0d27-0160-c86469116117', 
             'detail-type': 'Scheduled Event', 'source': 'aws.events', 
             'account': '735286310638', 'time': f"{d}Z", 
             'region': 'eu-central-1', 'resources': ['arn:aws:events:eu-central-1:735286310638:rule/ObsdecoderStack-mylambdarule8DCCEE0D-5kIvnIpBKC6B'], 'detail': {}}
        ).encode("utf-8")
    )

    print(response)