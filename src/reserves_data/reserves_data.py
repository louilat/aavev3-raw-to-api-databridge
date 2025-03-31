from datetime import datetime
import pandas as pd
from pandas import DataFrame
import json


def get_reserves_data(client_s3, current_date: datetime) -> DataFrame:
    day_str = current_date.strftime("%Y-%m-%d")
    reserves_data = pd.read_csv(
        client_s3.get_object(
            Bucket="projet-datalab-group-jprat",
            Key=f"aave-raw-datasource/daily-users-balances/users_balances_snapshot_date={day_str}/reserves_data.csv",
        )["Body"]
    )
    return reserves_data


def save_reserves_data(
    client_s3, current_reserves_data: DataFrame, current_date: datetime
) -> None:
    day_str = current_date.strftime("%Y-%m-%d")
    current_reserves_data_json = json.dumps(
        current_reserves_data.to_dict(orient="records")
    )
    client_s3.put_object(
        Bucket="projet-datalab-group-jprat",
        Key=f"aave-api-datasource/daily-users-balances/users_balances_snapshot_date={day_str}/reserves_data.json",
        Body=current_reserves_data_json,
    )
