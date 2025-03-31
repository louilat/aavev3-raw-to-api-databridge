"""ETL for convering raw data to api data"""

import boto3
from datetime import datetime, timezone, timedelta
import os

from src.reserves_data.reserves_data import get_reserves_data, save_reserves_data
from src.users_snapshot.users_snapshot import (
    get_users_snapshot,
    get_previous_users_snapshot,
    save_users_snapshot,
)

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

reserves_data = get_reserves_data(client_s3=client_s3, current_date=snapshot_day)

previous_users_snapshot = get_previous_users_snapshot(
    client_s3=client_s3, snapshot_date=snapshot_day
)

users_snapshot = get_users_snapshot(
    client_s3=client_s3,
    previous_balances_snapshot=previous_users_snapshot,
    current_date=snapshot_day,
)

print("Process data...")

reserves_data = reserves_data[
    [
        "name",
        "lastUpdateTimestamp",
        "decimals",
        "underlyingAsset",
        "underlyingTokenPriceUSD",
        "liquidityRate",
        "variableBorrowRate",
        "liquidityIndex",
        "variableBorrowIndex",
        "availableLiquidity",
        "totalScaledVariableDebt",
    ]
]

users_snapshot.decimals = users_snapshot.decimals.apply(int)
users_snapshot.snapshot_block = users_snapshot.snapshot_block.apply(int)
users_snapshot.scaledATokenBalance = users_snapshot.scaledATokenBalance.apply(int)
users_snapshot.scaledVariableDebt = users_snapshot.scaledVariableDebt.apply(int)

users_snapshot = users_snapshot[
    [
        "snapshot_block",
        "user_address",
        "name",
        "underlyingAsset",
        "decimals",
        "scaledATokenBalance",
        "scaledVariableDebt",
    ]
]

print("Generate and save outputs...")

save_reserves_data(
    client_s3=client_s3, current_reserves_data=reserves_data, current_date=snapshot_day
)
save_users_snapshot(
    client_s3=client_s3,
    current_users_snapshot=users_snapshot,
    current_date=snapshot_day,
)
print("Done!")
