"""ETL for convering raw data to api data"""

import boto3
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
import io
import os


ACCESS_KEY_ID = os.environ["ACCESS_KEY_ID"]
SECRET_ACCESS_KEY = os.environ["SECRET_ACCESS_KEY"]
VERIFY = False

client_s3 = boto3.client(
    "s3",
    endpoint_url="https://" + "minio-simple.lab.groupe-genes.fr",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    verify=VERIFY,
)

print("Starting ETL...")

today = datetime.today()
snapshot_day = datetime(
    today.year, today.month, today.day, tzinfo=timezone.utc
) - timedelta(days=14)


print("INFO: Snapshot date is: ", snapshot_day)

print("Extracting data...")
day_str = snapshot_day.strftime("%Y-%m-%d")
previous_day = snapshot_day - timedelta(days=1)
previous_day_str = previous_day.strftime("%Y-%m-%d")

# Previous balances
data = client_s3.get_object(
    Bucket="projet-datalab-group-jprat",
    Key=f"aavev3-api-datasource/users_balances/users_balances_snapshot_date={previous_day_str}/users_balances.json",
)["Body"].read()
data = json.loads(data)
previous_balances_snapshot = pd.json_normalize(data)

# Active users
data = client_s3.get_object(
    Bucket="projet-datalab-group-jprat",
    Key=f"aavev3-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={day_str}/all_active_users.json",
)["Body"].read()
active_users_list = json.loads(data)

# Active users balances
data = client_s3.get_object(
    Bucket="projet-datalab-group-jprat",
    Key=f"aavev3-raw-datasource/daily-users-balances/users_balances_snapshot_date={day_str}/active_users_balances.json",
)["Body"].read()
data = json.loads(data)
active_users_balances = pd.json_normalize(data)

print("Process data...")
current_balances_snapshot = previous_balances_snapshot[
    ~previous_balances_snapshot.user.isin(active_users_list)
]
current_balances_snapshot = pd.concat(
    (current_balances_snapshot, active_users_balances)
)

print("Generate and save outputs...")
buffer = io.StringIO()
current_balances_snapshot.to_json(buffer, orient="records")
client_s3.put_object(
    Bucket="projet-datalab-group-jprat",
    Key=f"aavev3-api-datasource/users_balances/users_balances_snapshot_date={day_str}/users_balances.json",
    Body=buffer.getvalue(),
)
