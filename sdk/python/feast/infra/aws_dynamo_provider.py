import boto3
import itertools
from datetime import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import mmh3
import pandas
import pyarrow

from feast import FeatureTable, utils
from feast.feature_view import FeatureView
from feast.infra.key_encoding_utils import serialize_entity_key
from feast.infra.offline_stores.helpers import get_offline_store_from_sources
from feast.infra.provider import (
    Provider,
    RetrievalJob,
    _convert_arrow_to_proto,
    _get_column_names,
    _run_field_mapping,
)
from feast.protos.feast.types.EntityKey_pb2 import EntityKey as EntityKeyProto
from feast.protos.feast.types.Value_pb2 import Value as ValueProto
from feast.registry import Registry
from feast.repo_config import DatastoreOnlineStoreConfig, RepoConfig


class AwsDynamoProvider(Provider):
    _aws_project_id: Optional[str]

    def __init__(self, config: Optional[RepoConfig]):
        if config and config.online_store and config.online_store.project_id:
            self._aws_project_id = config.online_store.project_id
        else:
            self._aws_project_id = None

    def _initialize_client(self):
        client = boto3.client('dynamodb')
        return client


    def update_infra(
            self,
            project: str,
            tables_to_delete: Sequence[Union[FeatureTable, FeatureView]],
            tables_to_keep: Sequence[Union[FeatureTable, FeatureView]],
            partial: bool,
    ):
        client = self._initialize_client()

        for table_name in tables_to_keep:
            # TODO: add table creation to dynamo.
            table = client.Table(table_name.name)
            table.update_item(
                Key={
                    "Project": project
                },
                UpdateExpression='SET created_ts = :val1',
                ExpressionAttributeValues={
                    ':val1': datetime.utcnow()
                }
            )

        for table_name in tables_to_delete:
            table = client.Table(table_name.name)
            table.delete()

    def teardown_infra(
            self, project: str, tables: Sequence[Union[FeatureTable, FeatureView]]
    ) -> None:
        client = self._initialize_client()

        for table_name in tables:
            table = client.Table(table_name)
            table.delete()

    def online_write_batch(
            self,
            project: str,
            table: Union[FeatureTable, FeatureView],
            data: List[
                Tuple[EntityKeyProto, Dict[str, ValueProto], datetime, Optional[datetime]]
            ],
            progress: Optional[Callable[[int], Any]],
    ) -> None:
        client = self._initialize_client()

        table_instance = client.Table(table.name)
        with table_instance.batch_writer() as batch:
            for entity_key, features, timestamp, created_ts in data:
                document_id = compute_datastore_entity_id(entity_key) #TODO check id
                #TODO compression encoding
                batch.put_item(
                    Item={
                        "Row": document_id, #PartitionKey
                        "Project": project, #SortKey
                        "event_ts": utils.make_tzaware(timestamp),
                        "created_ts": utils.make_tzaware(created_ts),
                        "values": {
                                   k: v.SerializeToString() for k, v in features.items() #Serialized Features
                               },
                    }
                )

    def online_read(
            self,
            project: str,
            table: Union[FeatureTable, FeatureView],
            entity_keys: List[EntityKeyProto],
    ) -> List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]]:
        client = self._initialize_client()

        result: List[Tuple[Optional[datetime], Optional[Dict[str, ValueProto]]]] = []
        for entity_key in entity_keys:
            table_instace = client.Table(table.name)
            document_id = compute_datastore_entity_id(entity_key) #TODO check id
            response = table_instace.get_item(
                Key={
                    "Row": document_id,
                    "Project": project
                }
            )
            value = response['Item']

            if value is not None:
                res = {}
                for feature_name, value_bin in value["values"].items():
                    val = ValueProto()
                    val.ParseFromString(value_bin)
                    res[feature_name] = val
                result.append((value["event_ts"], res))
            else:
                result.append((None, None))
        return result


    def materialize_single_feature_view(
            self,
            feature_view: FeatureView,
            start_date: datetime,
            end_date: datetime,
            registry: Registry,
            project: str,
    ) -> None:
        #TODO implement me
        pass


    @staticmethod
    def get_historical_features(
            config: RepoConfig,
            feature_views: List[FeatureView],
            feature_refs: List[str],
            entity_df: Union[pandas.DataFrame, str],
            registry: Registry,
            project: str,
    ) -> RetrievalJob:
        #TODO implement me
        pass

def compute_datastore_entity_id(entity_key: EntityKeyProto) -> str:
    """
    Compute Datastore Entity id given Feast Entity Key.

    Remember that Datastore Entity is a concept from the Datastore data model, that has nothing to
    do with the Entity concept we have in Feast.
    """
    return mmh3.hash_bytes(serialize_entity_key(entity_key)).hex()
