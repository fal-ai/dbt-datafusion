from contextlib import contextmanager
from dataclasses import dataclass
import dbt.exceptions  # noqa
from dbt.adapters.base import Credentials

from dbt.adapters.base import BaseConnectionManager as connection_cls

from dbt.logger import GLOBAL_LOGGER as logger

from dbt.adapters.datafusion import db

from typing import Tuple

from dbt.contracts.connection import AdapterResponse

import dbt.clients.agate_helper


@dataclass
class DataFusionCredentials(Credentials):
    """
    Defines database specific credentials that get added to
    profiles.yml to connect to new adapter
    """

    # Add credentials members here, like:
    # host: str
    # port: int
    # username: str
    # password: str
    path: str

    _ALIASES = {"dbname": "database", "pass": "password", "user": "username"}

    @property
    def type(self):
        """Return name of adapter."""
        return "datafusion"

    @property
    def unique_field(self):
        """
        Hashed and included in anonymous telemetry to track adapter adoption.
        Pick a field that can uniquely identify one team/organization building with this adapter
        """
        return self.host

    def _connection_keys(self):
        """
        List of keys to display in the `dbt debug` output.
        """
        return ("host", "port", "username", "user", "path")


class DataFusionConnectionManager(connection_cls):
    TYPE = "datafusion"

    @contextmanager
    def exception_handler(self, sql: str):
        """
        Returns a context manager, that will handle exceptions raised
        from queries, catch, log, and raise dbt exceptions it knows how to handle.
        """
        # ## Example ##
        # try:
        #     yield
        # except myadapter_library.DatabaseError as exc:
        #     self.release(connection_name)

        #     logger.debug("myadapter error: {}".format(str(e)))
        #     raise dbt.exceptions.DatabaseException(str(exc))
        # except Exception as exc:
        #     logger.debug("Error running SQL: {}".format(sql))
        #     logger.debug("Rolling back transaction.")
        #     self.release(connection_name)
        #     raise dbt.exceptions.RuntimeException(str(exc))
        try:
            yield
        except Exception as e:
            raise

    @classmethod
    def open(cls, connection):
        """
        Receives a connection object and a Credentials object
        and moves it to the "open" state.
        """
        ## Example ##
        if connection.state == "open":
            logger.debug("Connection is already open, skipping open.")
            return connection

        credentials = connection.credentials

        try:
            # handle = myadapter_library.connect(
            #     host=credentials.host,
            #     port=credentials.port,
            #     username=credentials.username,
            #     password=credentials.password,
            #     catalog=credentials.database
            # )

            handle = db.DB(credentials.path)

            connection.state = "open"
            connection.handle = handle

        except Exception as e:
            logger.debug(
                "Got an error when attempting to create a datafusion client: '{}'".format(
                    e
                )
            )
            connection.handle = None
            connection.state = "fail"
            raise Exception(str(e))

        return connection

    @classmethod
    def get_response(cls, cursor):
        """
        Gets a cursor object and returns adapter-specific information
        about the last executed command generally a AdapterResponse ojbect
        that has items such as code, rows_affected,etc. can also just be a string ex. "OK"
        if your cursor does not offer rich metadata.
        """
        # ## Example ##
        # return cursor.status_message
        message = "OK"
        return AdapterResponse(_message=message)

    def cancel(self, connection):
        """
        Gets a connection object and attempts to cancel any ongoing queries.
        """
        # ## Example ##
        # tid = connection.handle.transaction_id()
        # sql = "select cancel_transaction({})".format(tid)
        # logger.debug("Cancelling query "{}" ({})".format(connection_name, pid))
        # _, cursor = self.add_query(sql, "master")
        # res = cursor.fetchone()
        # logger.debug("Canceled query "{}": {}".format(connection_name, res))
        pass

    def begin(self):
        pass

    def cancel_open(self):
        pass

    def commit(self):
        pass

    def execute(self, sql, auto_begin=False, fetch=None) -> Tuple[str, str]:
        conn = self.get_thread_connection()
        client = conn.handle.ctx

        table = dbt.clients.agate_helper.empty_table()

        if fetch:
            table = client.sql(sql)

        response = AdapterResponse(  # type: ignore[call-arg]
            _message="OK", rows_affected=0, code="datafusion"
        )

        return response, table
