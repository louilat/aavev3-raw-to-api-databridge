import pandas as pd
from pandas import DataFrame
from datetime import datetime, timedelta
import json


def get_users_snapshot(
    client_s3, previous_balances_snapshot: DataFrame, current_date: datetime
):
    day_str = current_date.strftime("%Y-%m-%d")

    active_users = pd.read_csv(
        client_s3.get_object(
            Bucket="projet-datalab-group-jprat",
            Key=f"aave-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={day_str}/all_active_users.csv",
        )["Body"]
    )
    transfer_users = pd.read_csv(
        client_s3.get_object(
            Bucket="projet-datalab-group-jprat",
            Key=f"aave-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={day_str}/all_atoken_transfer_users.csv",
        )["Body"]
    )
    active_users = pd.concat((active_users, transfer_users)).drop_duplicates()
    active_users_balances = pd.read_csv(
        client_s3.get_object(
            Bucket="projet-datalab-group-jprat",
            Key=f"aave-raw-datasource/daily-users-balances/users_balances_snapshot_date={day_str}/active_users_balances.csv",
        )["Body"]
    )
    transfer_users_balances = pd.read_csv(
        client_s3.get_object(
            Bucket="projet-datalab-group-jprat",
            Key=f"aave-raw-datasource/daily-users-balances/users_balances_snapshot_date={day_str}/atoken_transfer_users_balances.csv",
        )["Body"]
    )
    active_users_balances = pd.concat(
        (active_users_balances, transfer_users_balances)
    ).drop_duplicates(subset=["user_address", "name"])
    balances_snapshot = previous_balances_snapshot[
        ~previous_balances_snapshot.user_address.isin(active_users.active_user_address)
    ]
    balances_snapshot = pd.concat((balances_snapshot, active_users_balances))

    return balances_snapshot


def get_previous_users_snapshot(client_s3, snapshot_date: datetime) -> DataFrame:
    previous_date = snapshot_date - timedelta(days=1)
    day_str = previous_date.strftime("%Y-%m-%d")
    previous_users_snapshot = json.loads(
        client_s3.get_object(
            Bucket="projet-datalab-group-jprat",
            Key=f"aave-api-datasource/daily-users-balances/users_balances_snapshot_date={day_str}/users_balances.json",
        )["Body"]
        .read()
        .decode()
    )
    previous_users_snapshot = pd.json_normalize(previous_users_snapshot)
    return previous_users_snapshot


def save_users_snapshot(
    client_s3, current_users_snapshot: DataFrame, current_date: datetime
) -> None:
    day_str = current_date.strftime("%Y-%m-%d")
    current_users_snapshot_json = json.dumps(
        current_users_snapshot.to_dict(orient="records")
    )
    client_s3.put_object(
        Bucket="projet-datalab-group-jprat",
        Key=f"aave-api-datasource/daily-users-balances/users_balances_snapshot_date={day_str}/users_balances.json",
        Body=current_users_snapshot_json,
    )
