import importlib
import os
import tempfile
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from feast import FeatureStore, FeatureView, RepoConfig, driver_test_data
from feast.constants import FULL_REPO_CONFIGS_MODULE_ENV_NAME
from feast.data_source import DataSource
from tests.integration.feature_repos.integration_test_repo_config import (
    IntegrationTestRepoConfig,
)
from tests.integration.feature_repos.universal.data_source_creator import (
    DataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.bigquery import (
    BigQueryDataSourceCreator,
)
from tests.integration.feature_repos.universal.data_sources.redshift import (
    RedshiftDataSourceCreator,
)
from tests.integration.feature_repos.universal.feature_views import (
    conv_rate_plus_100_feature_view,
    create_conv_rate_request_data_source,
    create_customer_daily_profile_feature_view,
    create_driver_age_request_feature_view,
    create_driver_hourly_stats_feature_view,
    create_global_stats_feature_view,
    create_location_stats_feature_view,
    create_order_feature_view,
)

DYNAMO_CONFIG = {"type": "dynamodb", "region": "us-west-2"}
REDIS_CONFIG = {"type": "redis", "connection_string": "localhost:6379,db=0"}

# FULL_REPO_CONFIGS contains the repo configurations (e.g. provider, offline store,
# online store, test data, and more parameters) that most integration tests will test
# against. By default, FULL_REPO_CONFIGS uses the three providers (local, GCP, and AWS)
# with their default offline and online stores; it also tests the providers with the
# Redis online store. It can be overwritten by specifying a Python module through the
# FULL_REPO_CONFIGS_MODULE_ENV_NAME environment variable. In this case, that Python
# module will be imported and FULL_REPO_CONFIGS will be extracted from the file.
DEFAULT_FULL_REPO_CONFIGS: List[IntegrationTestRepoConfig] = [
    # Local configurations
    IntegrationTestRepoConfig(),
    IntegrationTestRepoConfig(online_store=REDIS_CONFIG),
    # GCP configurations
    IntegrationTestRepoConfig(
        provider="gcp",
        offline_store_creator=BigQueryDataSourceCreator,
        online_store="datastore",
    ),
    IntegrationTestRepoConfig(
        provider="gcp",
        offline_store_creator=BigQueryDataSourceCreator,
        online_store=REDIS_CONFIG,
    ),
    # AWS configurations
    IntegrationTestRepoConfig(
        provider="aws",
        offline_store_creator=RedshiftDataSourceCreator,
        online_store=DYNAMO_CONFIG,
    ),
    IntegrationTestRepoConfig(
        provider="aws",
        offline_store_creator=RedshiftDataSourceCreator,
        online_store=REDIS_CONFIG,
    ),
]
full_repo_configs_module = os.environ.get(FULL_REPO_CONFIGS_MODULE_ENV_NAME)
if full_repo_configs_module is not None:
    try:
        module = importlib.import_module(full_repo_configs_module)
        FULL_REPO_CONFIGS = getattr(module, "FULL_REPO_CONFIGS")
    except Exception:
        FULL_REPO_CONFIGS = DEFAULT_FULL_REPO_CONFIGS
else:
    FULL_REPO_CONFIGS = DEFAULT_FULL_REPO_CONFIGS


def construct_universal_entities() -> Dict[str, List[Any]]:
    return {
        "customer": list(range(1001, 1020)),
        "driver": list(range(5001, 5020)),
        "location": list(range(1, 50)),
    }


def construct_universal_datasets(
    entities: Dict[str, List[Any]], start_time: datetime, end_time: datetime
) -> Dict[str, pd.DataFrame]:
    customer_df = driver_test_data.create_customer_daily_profile_df(
        entities["customer"], start_time, end_time
    )
    driver_df = driver_test_data.create_driver_hourly_stats_df(
        entities["driver"], start_time, end_time
    )
    location_df = driver_test_data.create_location_stats_df(
        entities["location"], start_time, end_time
    )
    orders_df = driver_test_data.create_orders_df(
        customers=entities["customer"],
        drivers=entities["driver"],
        locations=entities["location"],
        start_date=start_time,
        end_date=end_time,
        order_count=20,
    )
    global_df = driver_test_data.create_global_daily_stats_df(start_time, end_time)
    entity_df = orders_df[
        [
            "customer_id",
            "driver_id",
            "order_id",
            "origin_id",
            "destination_id",
            "event_timestamp",
        ]
    ]

    return {
        "customer": customer_df,
        "driver": driver_df,
        "location": location_df,
        "orders": orders_df,
        "global": global_df,
        "entity": entity_df,
    }


def construct_universal_data_sources(
    datasets: Dict[str, pd.DataFrame], data_source_creator: DataSourceCreator
) -> Dict[str, DataSource]:
    customer_ds = data_source_creator.create_data_source(
        datasets["customer"],
        destination_name="customer_profile",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    driver_ds = data_source_creator.create_data_source(
        datasets["driver"],
        destination_name="driver_hourly",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    location_ds = data_source_creator.create_data_source(
        datasets["location"],
        destination_name="location_hourly",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    orders_ds = data_source_creator.create_data_source(
        datasets["orders"],
        destination_name="orders",
        event_timestamp_column="event_timestamp",
        created_timestamp_column=None,
    )
    global_ds = data_source_creator.create_data_source(
        datasets["global"],
        destination_name="global",
        event_timestamp_column="event_timestamp",
        created_timestamp_column="created",
    )
    return {
        "customer": customer_ds,
        "driver": driver_ds,
        "location": location_ds,
        "orders": orders_ds,
        "global": global_ds,
    }


def construct_universal_feature_views(
    data_sources: Dict[str, DataSource],
) -> Dict[str, FeatureView]:
    driver_hourly_stats = create_driver_hourly_stats_feature_view(
        data_sources["driver"]
    )
    return {
        "customer": create_customer_daily_profile_feature_view(
            data_sources["customer"]
        ),
        "global": create_global_stats_feature_view(data_sources["global"]),
        "driver": driver_hourly_stats,
        "driver_odfv": conv_rate_plus_100_feature_view(
            {
                "driver": driver_hourly_stats,
                "input_request": create_conv_rate_request_data_source(),
            }
        ),
        "driver_age_request_fv": create_driver_age_request_feature_view(),
        "order": create_order_feature_view(data_sources["orders"]),
        "location": create_location_stats_feature_view(data_sources["location"]),
    }


@dataclass
class Environment:
    name: str
    test_repo_config: IntegrationTestRepoConfig
    feature_store: FeatureStore
    data_source_creator: DataSourceCreator

    end_date: datetime = field(
        default=datetime.now().replace(microsecond=0, second=0, minute=0)
    )

    def __post_init__(self):
        self.start_date: datetime = self.end_date - timedelta(days=3)


def table_name_from_data_source(ds: DataSource) -> Optional[str]:
    if hasattr(ds, "table_ref"):
        return ds.table_ref
    elif hasattr(ds, "table"):
        return ds.table
    return None


@contextmanager
def construct_test_environment(
    test_repo_config: IntegrationTestRepoConfig,
    test_suite_name: str = "integration_test",
) -> Environment:
    project = f"{test_suite_name}_{str(uuid.uuid4()).replace('-', '')[:8]}"

    offline_creator: DataSourceCreator = test_repo_config.offline_store_creator(project)

    offline_store_config = offline_creator.create_offline_store_config()
    online_store = test_repo_config.online_store

    with tempfile.TemporaryDirectory() as repo_dir_name:
        config = RepoConfig(
            registry=str(Path(repo_dir_name) / "registry.db"),
            project=project,
            provider=test_repo_config.provider,
            offline_store=offline_store_config,
            online_store=online_store,
            repo_path=repo_dir_name,
        )
        fs = FeatureStore(config=config)
        # We need to initialize the registry, because if nothing is applied in the test before tearing down
        # the feature store, that will cause the teardown method to blow up.
        fs.registry._initialize_registry()
        environment = Environment(
            name=project,
            test_repo_config=test_repo_config,
            feature_store=fs,
            data_source_creator=offline_creator,
        )

        try:
            yield environment
        finally:
            fs.teardown()
