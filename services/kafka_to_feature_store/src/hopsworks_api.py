import hopsworks
import pandas as pd
from src.config import config
from typing import Dict, List

def push_data_to_feature_store(
        feature_group_name: str,
        feature_group_version: int,
        data: Dict,
) -> None:
    """Pushes data to the feature store.

    Args:
        feature_group_name (str): The name of the feature group to write to.
        feature_group_version (int): The version of the feature group to write to.
        data (Dict): The data to write to the feature store.
    """
    print(data)

    # Authenticate with Hopsworks API
    project = hopsworks.login(
        project=config.hopsworks_project_name,
        api_key_value=config.hopsworks_api_key
    )

    # Get the feature store
    feature_store = project.get_feature_store()

    # Get or create the feature group we will be saving data to
    ohlc_feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        description="OHLC data coming from Kraken",
        primary_key=["product_id","timestamp"],
        event_time="timestamp",
        online_enabled=True
    )
    # write data to the feature group
    data = pd.DataFrame([data])
    ohlc_feature_group.insert(data)