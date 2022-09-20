from typing import List
from dbt.adapters.base import BaseAdapter as adapter_cls

from dbt.adapters.datafusion import DataFusionConnectionManager
from dbt.adapters.datafusion.relation import DataFusionRelation

from dbt.adapters.base import available

import dbt.exceptions


class DataFusionAdapter(adapter_cls):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    Relation = DataFusionRelation
    ConnectionManager = DataFusionConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

    @classmethod
    def is_cancelable(cls) -> bool:
        return False

    @available
    def empty(self) -> str:
        return ""

    @available
    def list_schemas(self, database: str) -> List[str]:
        return []

    def convert_boolean_type(self):
        return "boolean"

    def convert_date_type(self):
        return "date"

    def convert_datetime_type(self):
        raise dbt.exceptions.NotImplementedException(
            "`datetime` is not implemented for this adapter!"
        )

    def convert_number_type(self):
        "double"

    def convert_text_type(self):
        return "string"

    def convert_time_type(self):
        return "time"

    def create_schema(self, relation):
        pass
        # raise dbt.exceptions.NotImplementedException(
        #     f"`create_schema({relation})` is not implemented for this adapter!"
        # )

    def drop_relation(self, relation):
        # raise dbt.exceptions.NotImplementedException(
        #     f"`drop_relation({relation})` is not implemented for this adapter!"
        # )
        return

    def drop_schema(self):
        raise dbt.exceptions.NotImplementedException(
            "`drop_schema` is not implemented for this adapter!"
        )

    def expand_column_types(self):
        raise dbt.exceptions.NotImplementedException(
            "`expand_column_types` is not implemented for this adapter!"
        )

    def get_columns_in_relation(self):
        raise dbt.exceptions.NotImplementedException(
            "`get_columns_in_relation` is not implemented for this adapter!"
        )

    def list_relations_without_caching(self, schema_relation):
        return []

    def quote(self):
        return '"'

    def rename_relation(self, from_relation, to_relation):
        # raise dbt.exceptions.NotImplementedException(
        #     "`rename_relation` is not implemented for this adapter!"
        # )
        return

    def truncate_relation(self):
        raise dbt.exceptions.NotImplementedException(
            "`truncate` is not implemented for this adapter!"
        )

    @available
    def create_table_as(self, name, sql):
        conn = self.connections.get_thread_connection()
        conn.handle.create_table_as(name, sql)
        return
