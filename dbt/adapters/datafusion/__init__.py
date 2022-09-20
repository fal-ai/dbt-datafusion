from dbt.adapters.datafusion.connections import DataFusionConnectionManager # noqa
from dbt.adapters.datafusion.connections import DataFusionCredentials
from dbt.adapters.datafusion.impl import DataFusionAdapter

from dbt.adapters.base import AdapterPlugin
from dbt.include import datafusion


Plugin = AdapterPlugin(
    adapter=DataFusionAdapter,
    credentials=DataFusionCredentials,
    include_path=datafusion.PACKAGE_PATH
    )
