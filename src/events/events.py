import json
import pandas as pd
from pandas import DataFrame
from pandas.errors import EmptyDataError
from datetime import datetime


def transfer_events(client_s3, snapshot_day: datetime):
    day_str = snapshot_day.strftime("%Y-%m-%d")
    objects_list = client_s3.list_objects_v2(
        Bucket="projet-datalab-group-jprat",
        Prefix=f"aave-raw-datasource/daily-decoded-events/decoded_events_snapshot_date={day_str}/"
    )["Contents"]
    for obj in objects_list:
        try:
            data = pd.read_csv(
                client_s3.get_object(
                    Bucket="projet-datalab-group-jprat",
                    Key=obj["Key"])["Body"],
            )
        except EmptyDataError:
            data = DataFrame()
            
        for col in data.columns:
            if col in ["amount", "debtToCover", "liquidatedCollateralAmount", "borrowRate", "blockNumber"]:
                data[col] = data[col].apply(int)

        dct = json.dumps(data.to_dict(orient="records"))

        object_name = obj["Key"].split("/")[-1].split(".")[0]
        client_s3.put_object(
            Bucket="projet-datalab-group-jprat",
            Key=f"aave-api-datasource/daily-decoded-events/decoded_events_snapshot_date={day_str}/{object_name}.json",
            Body=dct,
        )