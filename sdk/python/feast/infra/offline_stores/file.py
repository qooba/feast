from datetime import datetime
from typing import Callable, List, Optional, Tuple, Union

import dask.dataframe as dd
import pandas as pd
import pyarrow
import pytz
from pydantic.typing import Literal

from feast import FileSource, OnDemandFeatureView
from feast.data_source import DataSource
from feast.errors import FeastJoinKeysDuringMaterialization
from feast.feature_view import DUMMY_ENTITY_ID, DUMMY_ENTITY_VAL, FeatureView
from feast.infra.offline_stores.file_source import SavedDatasetFileStorage
from feast.infra.offline_stores.offline_store import (
    OfflineStore,
    RetrievalJob,
    RetrievalMetadata,
)
from feast.infra.offline_stores.offline_utils import (
    DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL,
)
from feast.infra.provider import (
    _get_requested_feature_views_to_features_dict,
    _run_dask_field_mapping,
)
from feast.registry import Registry
from feast.repo_config import FeastConfigBaseModel, RepoConfig
from feast.saved_dataset import SavedDatasetStorage
from feast.usage import log_exceptions_and_usage


class FileOfflineStoreConfig(FeastConfigBaseModel):
    """ Offline store config for local (file-based) store """

    type: Literal["file"] = "file"
    """ Offline store type selector"""


class FileRetrievalJob(RetrievalJob):
    def __init__(
        self,
        evaluation_function: Callable,
        full_feature_names: bool,
        on_demand_feature_views: Optional[List[OnDemandFeatureView]] = None,
        metadata: Optional[RetrievalMetadata] = None,
    ):
        """Initialize a lazy historical retrieval job"""

        # The evaluation function executes a stored procedure to compute a historical retrieval.
        self.evaluation_function = evaluation_function
        self._full_feature_names = full_feature_names
        self._on_demand_feature_views = (
            on_demand_feature_views if on_demand_feature_views else []
        )
        self._metadata = metadata

    @property
    def full_feature_names(self) -> bool:
        return self._full_feature_names

    @property
    def on_demand_feature_views(self) -> Optional[List[OnDemandFeatureView]]:
        return self._on_demand_feature_views

    @log_exceptions_and_usage
    def _to_df_internal(self) -> pd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return df

    @log_exceptions_and_usage
    def _to_dask_df_internal(self) -> dd.DataFrame:
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function().compute()
        return df

    @log_exceptions_and_usage
    def _to_arrow_internal(self):
        # Only execute the evaluation function to build the final historical retrieval dataframe at the last moment.
        df = self.evaluation_function()
        return pyarrow.Table.from_pandas(df)

    def persist(self, storage: SavedDatasetStorage):
        assert isinstance(storage, SavedDatasetFileStorage)

        filesystem, path = FileSource.create_filesystem_and_path(
            storage.file_options.file_url, storage.file_options.s3_endpoint_override,
        )

        if path.endswith(".parquet"):
            pyarrow.parquet.write_table(
                self.to_arrow(), where=path, filesystem=filesystem
            )
        else:
            # otherwise assume destination is directory
            pyarrow.parquet.write_to_dataset(
                self.to_arrow(), root_path=path, filesystem=filesystem
            )

    @property
    def metadata(self) -> Optional[RetrievalMetadata]:
        return self._metadata

class FileOfflineStore(OfflineStore):
    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def get_historical_features(
        config: RepoConfig,
        feature_views: List[FeatureView],
        feature_refs: List[str],
        entity_df: Union[pd.DataFrame, str],
        registry: Registry,
        project: str,
        full_feature_names: bool = False,
    ) -> RetrievalJob:
        if not isinstance(entity_df, pd.DataFrame) and not isinstance(
            entity_df, dd.DataFrame
        ):
            raise ValueError(
                f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
            )
        entity_df_event_timestamp_col = DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL  # local modifiable copy of global variable
        if entity_df_event_timestamp_col not in entity_df.columns:
            datetime_columns = entity_df.select_dtypes(
                include=["datetime", "datetimetz"]
            ).columns
            if len(datetime_columns) == 1:
                print(
                    f"Using {datetime_columns[0]} as the event timestamp. To specify a column explicitly, please name it {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL}."
                )
                entity_df_event_timestamp_col = datetime_columns[0]
            else:
                raise ValueError(
                    f"Please provide an entity_df with a column named {DEFAULT_ENTITY_DF_EVENT_TIMESTAMP_COL} representing the time of events."
                )
        (
            feature_views_to_features,
            on_demand_feature_views_to_features,
        ) = _get_requested_feature_views_to_features_dict(
            feature_refs,
            feature_views,
            registry.list_on_demand_feature_views(config.project),
        )

        entity_df_event_timestamp_range = _get_entity_df_event_timestamp_range(
            entity_df, entity_df_event_timestamp_col
        )

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_historical_retrieval():

            # Create a copy of entity_df to prevent modifying the original
            entity_df_with_features = entity_df.copy()

            entity_df_event_timestamp_col_type = entity_df_with_features.dtypes[
                entity_df_event_timestamp_col
            ]
            if (
                not hasattr(entity_df_event_timestamp_col_type, "tz")
                or entity_df_event_timestamp_col_type.tz != pytz.UTC
            ):
                # Make sure all event timestamp fields are tz-aware. We default tz-naive fields to UTC
                entity_df_with_features[
                    entity_df_event_timestamp_col
                ] = entity_df_with_features[entity_df_event_timestamp_col].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc)
                )

                # Convert event timestamp column to datetime and normalize time zone to UTC
                # This is necessary to avoid issues with pd.merge_asof
                if isinstance(entity_df_with_features, dd.DataFrame):
                    entity_df_with_features[
                        entity_df_event_timestamp_col
                    ] = dd.to_datetime(
                        entity_df_with_features[entity_df_event_timestamp_col], utc=True
                    )
                else:
                    entity_df_with_features[
                        entity_df_event_timestamp_col
                    ] = pd.to_datetime(
                        entity_df_with_features[entity_df_event_timestamp_col], utc=True
                    )

            # Sort event timestamp values
            entity_df_with_features = entity_df_with_features.sort_values(
                entity_df_event_timestamp_col
            )

            # Load feature view data from sources and join them incrementally
            for feature_view, features in feature_views_to_features.items():
                event_timestamp_column = (
                    feature_view.batch_source.event_timestamp_column
                )
                created_timestamp_column = (
                    feature_view.batch_source.created_timestamp_column
                )

                # Build a list of entity columns to join on (from the right table)
                join_keys = []
                for entity_name in feature_view.entities:
                    entity = registry.get_entity(entity_name, project)
                    join_key = feature_view.projection.join_key_map.get(
                        entity.join_key, entity.join_key
                    )
                    join_keys.append(join_key)
                right_entity_columns = join_keys
                right_entity_key_columns = [
                    event_timestamp_column
                ] + right_entity_columns

                entity_name = join_keys[0]
                # entity_df_with_features = entity_df_with_features.set_index(entity_name)

                storage_options = (
                    {
                        "client_kwargs": {
                            "endpoint_url": feature_view.batch_source.file_options.s3_endpoint_override
                        }
                    }
                    if feature_view.batch_source.file_options.s3_endpoint_override
                    else None
                )

                df_to_join = dd.read_parquet(
                    feature_view.batch_source.path,
                    storage_options=storage_options,
                    # index=[entity_name],
                )

                # Get only data with requested entities
                __entity_df_event_timestamp_col = f"__{entity_df_event_timestamp_col}"
                df_to_join = dd.merge(
                    df_to_join,
                    entity_df_with_features[
                        join_keys + [entity_df_event_timestamp_col]
                    ].rename(
                        columns={
                            entity_df_event_timestamp_col: __entity_df_event_timestamp_col
                        }
                    ),
                    left_on=join_keys,
                    right_on=join_keys,
                    suffixes=("", "__"),
                )
                df_to_join = df_to_join.persist()

                # Rename columns by the field mapping dictionary if it exists
                if feature_view.batch_source.field_mapping is not None:
                    _run_dask_field_mapping(
                        df_to_join, feature_view.batch_source.field_mapping
                    )
                # Rename entity columns by the join_key_map dictionary if it exists
                if feature_view.projection.join_key_map:
                    _run_dask_field_mapping(
                        df_to_join, feature_view.projection.join_key_map
                    )

                df_to_join_types = df_to_join.dtypes
                event_timestamp_column_type = df_to_join_types[event_timestamp_column]

                if created_timestamp_column:
                    created_timestamp_column_type = df_to_join_types[
                        created_timestamp_column
                    ]

                if (
                    not hasattr(event_timestamp_column_type, "tz")
                    or event_timestamp_column_type.tz != pytz.UTC
                ):
                    # Make sure all timestamp fields are tz-aware. We default tz-naive fields to UTC
                    df_to_join[event_timestamp_column] = df_to_join[
                        event_timestamp_column
                    ].apply(
                        lambda x: x
                        if x.tzinfo is not None
                        else x.replace(tzinfo=pytz.utc),
                        meta=(event_timestamp_column, "datetime64[ns, UTC]"),
                    )

                if created_timestamp_column and (
                    not hasattr(created_timestamp_column_type, "tz")
                    or created_timestamp_column_type.tz != pytz.UTC
                ):
                    df_to_join[created_timestamp_column] = df_to_join[
                        created_timestamp_column
                    ].apply(
                        lambda x: x
                        if x.tzinfo is not None
                        else x.replace(tzinfo=pytz.utc),
                        meta=(event_timestamp_column, "datetime64[ns, UTC]"),
                    )

                # Filter rows by defined timestamp tolerance
                df_to_join = df_to_join[
                    (
                        df_to_join[event_timestamp_column]
                        >= df_to_join[__entity_df_event_timestamp_col]
                        - feature_view.ttl
                    )
                    & (
                        df_to_join[event_timestamp_column]
                        <= df_to_join[__entity_df_event_timestamp_col]
                    )
                ]
                df_to_join = df_to_join.persist()
                df_to_join = df_to_join.drop([__entity_df_event_timestamp_col], axis=1)
                df_to_join = df_to_join.persist()

                # Build a list of all the features we should select from this source
                feature_names = []
                columns_map = {}
                for feature in features:
                    # Modify the separator for feature refs in column names to double underscore. We are using
                    # double underscore as separator for consistency with other databases like BigQuery,
                    # where there are very few characters available for use as separators
                    if full_feature_names:
                        formatted_feature_name = (
                            f"{feature_view.projection.name_to_use()}__{feature}"
                        )
                    else:
                        formatted_feature_name = feature
                    # Add the feature name to the list of columns
                    feature_names.append(formatted_feature_name)

                    # Ensure that the source dataframe feature column includes the feature view name as a prefix
                df_to_join = df_to_join.rename(columns=columns_map)
                df_to_join = df_to_join.persist()

                # Remove all duplicate entity keys (using created timestamp)
                right_entity_key_sort_columns = right_entity_key_columns
                if created_timestamp_column:
                    # If created_timestamp is available, use it to dedupe deterministically
                    right_entity_key_sort_columns = right_entity_key_sort_columns + [
                        created_timestamp_column
                    ]

                df_to_join = df_to_join.sort_values(by=event_timestamp_column)
                df_to_join = df_to_join.persist()

                df_to_join = df_to_join.reset_index().drop_duplicates(
                    join_keys, keep="last", ignore_index=True,
                )
                df_to_join = df_to_join.persist()

                # Select only the columns we need to join from the feature dataframe
                df_to_join = df_to_join[right_entity_key_columns + feature_names]
                df_to_join = df_to_join.persist()

                # Ensure that we delete dataframes to free up memory
                del entity_df_with_features

            df_to_join = df_to_join.rename(
                columns={event_timestamp_column: entity_df_event_timestamp_col}
            )

            return df_to_join.persist()

        job = FileRetrievalJob(
            evaluation_function=evaluate_historical_retrieval,
            full_feature_names=full_feature_names,
            on_demand_feature_views=OnDemandFeatureView.get_requested_odfvs(
                feature_refs, project, registry
            ),
            metadata=RetrievalMetadata(
                features=feature_refs,
                keys=list(set(entity_df.columns) - {entity_df_event_timestamp_col}),
                min_event_timestamp=entity_df_event_timestamp_range[0],
                max_event_timestamp=entity_df_event_timestamp_range[1],
            ),
        )
        return job

    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def pull_latest_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        created_timestamp_column: Optional[str],
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        assert isinstance(data_source, FileSource)

        # Create lazy function that is only called from the RetrievalJob object
        def evaluate_offline_job():

            storage_options = (
                {
                    "client_kwargs": {
                        "endpoint_url": data_source.file_options.s3_endpoint_override
                    }
                }
                if data_source.file_options.s3_endpoint_override
                else None
            )

            source_df = dd.read_parquet(
                data_source.path, storage_options=storage_options
            )

            source_df_types = source_df.dtypes
            event_timestamp_column_type = source_df_types[event_timestamp_column]

            if created_timestamp_column:
                created_timestamp_column_type = source_df_types[
                    created_timestamp_column
                ]

            if (
                not hasattr(event_timestamp_column_type, "tz")
                or event_timestamp_column_type.tz != pytz.UTC
            ):
                source_df[event_timestamp_column] = source_df[
                    event_timestamp_column
                ].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
                    meta=(event_timestamp_column, "datetime64[ns, UTC]"),
                )

            if created_timestamp_column and (
                not hasattr(created_timestamp_column_type, "tz")
                or created_timestamp_column_type.tz != pytz.UTC
            ):
                source_df[created_timestamp_column] = source_df[
                    created_timestamp_column
                ].apply(
                    lambda x: x if x.tzinfo is not None else x.replace(tzinfo=pytz.utc),
                    meta=(event_timestamp_column, "datetime64[ns, UTC]"),
                )

            source_df = source_df.persist()

            source_columns = set(source_df.columns)
            if not set(join_key_columns).issubset(source_columns):
                raise FeastJoinKeysDuringMaterialization(
                    data_source.path, set(join_key_columns), source_columns
                )

            ts_columns = (
                [event_timestamp_column, created_timestamp_column]
                if created_timestamp_column
                else [event_timestamp_column]
            )

            source_df = source_df.sort_values(by=event_timestamp_column)

            source_df = source_df[
                (source_df[event_timestamp_column] >= start_date)
                & (source_df[event_timestamp_column] < end_date)
            ]

            source_df = source_df.persist()

            columns_to_extract = set(
                join_key_columns + feature_name_columns + ts_columns
            )
            if join_key_columns:
                source_df = source_df.drop_duplicates(
                    join_key_columns, keep="last", ignore_index=True
                )
            else:
                source_df[DUMMY_ENTITY_ID] = DUMMY_ENTITY_VAL
                columns_to_extract.add(DUMMY_ENTITY_ID)

            source_df = source_df.persist()

            return source_df[list(columns_to_extract)].persist()

        # When materializing a single feature view, we don't need full feature names. On demand transforms aren't materialized
        return FileRetrievalJob(
            evaluation_function=evaluate_offline_job, full_feature_names=False,
        )

    @staticmethod
    @log_exceptions_and_usage(offline_store="file")
    def pull_all_from_table_or_query(
        config: RepoConfig,
        data_source: DataSource,
        join_key_columns: List[str],
        feature_name_columns: List[str],
        event_timestamp_column: str,
        start_date: datetime,
        end_date: datetime,
    ) -> RetrievalJob:
        return FileOfflineStore.pull_latest_from_table_or_query(
            config=config,
            data_source=data_source,
            join_key_columns=join_key_columns
            + [event_timestamp_column],  # avoid deduplication
            feature_name_columns=feature_name_columns,
            event_timestamp_column=event_timestamp_column,
            created_timestamp_column=None,
            start_date=start_date,
            end_date=end_date,
        )


def _get_entity_df_event_timestamp_range(
    entity_df: Union[pd.DataFrame, str], entity_df_event_timestamp_col: str,
) -> Tuple[datetime, datetime]:
    if not isinstance(entity_df, pd.DataFrame):
        raise ValueError(
            f"Please provide an entity_df of type {type(pd.DataFrame)} instead of type {type(entity_df)}"
        )

    entity_df_event_timestamp = entity_df.loc[
        :, entity_df_event_timestamp_col
    ].infer_objects()
    if pd.api.types.is_string_dtype(entity_df_event_timestamp):
        entity_df_event_timestamp = pd.to_datetime(entity_df_event_timestamp, utc=True)

    return (
        entity_df_event_timestamp.min().to_pydatetime(),
        entity_df_event_timestamp.max().to_pydatetime(),
    )
