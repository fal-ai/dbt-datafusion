
from dbt.adapters.base import BaseAdapter as adapter_cls

from dbt.adapters.datafusion import DataFusionConnectionManager



class DataFusionAdapter(adapter_cls):
    """
    Controls actual implmentation of adapter, and ability to override certain methods.
    """

    ConnectionManager = DataFusionConnectionManager

    @classmethod
    def date_function(cls):
        """
        Returns canonical date func
        """
        return "datenow()"

 # may require more build out to make more user friendly to confer with team and community.
