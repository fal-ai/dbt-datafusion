import datafusion

from deltalake import DeltaTable
from deltalake.writer import write_deltalake
import pyarrow as pa

import os


class DB:
    def __init__(self, root_path):
        self.root_path = root_path
        self.ctx = datafusion.SessionContext()
        for name in next(os.walk(root_path))[1]:
            dt = DeltaTable(root_path + name)
            self.ctx.register_dataset(name, dt.to_pyarrow_dataset())

    def create_table_as(self, name, query):

        print("QUERY:", query)
        # 1. Execute Query
        df = self.ctx.sql(query)

        # 2. Save as parquet
        t = DataFusionDeltaTable(self.root_path, name, df)

        # 3. Register the new parquet file
        self.ctx.deregister_table(name)
        self.ctx.register_dataset(name, t.dt.to_pyarrow_dataset())
        return

    def insert_into(self, name, query):
        df = self.ctx.sql(query)
        ft = DataFusionDeltaTable(name)
        ft.append(df)
        self.ctx.deregister_table(name)
        ft = DataFusionDeltaTable(name)
        self.ctx.register_dataset(name, ft.dt.to_pyarrow_dataset())

    def query(self, query):
        df = self.ctx.sql(query)
        arrow_table = pa.Table.from_batches(df.collect())
        return arrow_table.to_pandas()

    def drop(self, name):
        self.ctx.deregister_table(name)
        return

    def get_delta(self, name):
        dt = DeltaTable(self.root_path + name)
        return dt

    def tables(self):
        return self.ctx.tables()


class DataFusionDeltaTable:
    def __init__(self, root_path, name, df=None):
        if df:
            write_deltalake(
                root_path + name, df.collect(), schema=df.schema(), mode="overwrite"
            )
        self.dt = DeltaTable(root_path + name)

    def append(self, df):
        write_deltalake(self.dt, df.collect(), schema=df.schema(), mode="append")
