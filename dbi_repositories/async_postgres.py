import psycopg.sql

from dbi_repositories.postgres import ConnectionFactory
from psycopg import AsyncConnection
from typing import Optional, List, Any, Generator
from contextlib import asynccontextmanager


class AsyncConnectionFactory(ConnectionFactory):
    # Subclassing to reuse the __init__ definition
    # This class is only for compatibility to `postgres.py`

    def __call__(
            self,
            db_name: Optional[str] = None
    ) -> dict:
        if not db_name:
            db_name = self.db_name

        return dict(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=db_name,
            sslmode='require' if self.ssl else 'allow'
        )



class AsyncPostgresRepository:

    def __init__(
        self,
        connection_factory: AsyncConnectionFactory,
        table_name: str,
        primary_keys: List[str]
    ):
        self.connection_factory = connection_factory
        self.table_name = table_name
        self.primary_keys = primary_keys

    async def _execute_iterable_return(
        self,
        query: str | psycopg.sql.Composed,
        params: Optional[List[Any]] = None
    ) -> List:
        connection = await AsyncConnection.connect(**self.connection_factory())
        async with connection:
            async with connection.cursor() as cursor:
                if isinstance(query, psycopg.sql.Composed):
                    query = query.as_string(cursor)
                return await cursor.execute(query=query, params=params)

    async def _execute_no_return(
        self,
        query: str | psycopg.sql.Composed,
        params: Optional[List[Any]] = None
    ) -> None:
        connection = await AsyncConnection.connect(**self.connection_factory())
        async with connection:
            async with connection.cursor() as cursor:
                if isinstance(query, psycopg.sql.Composed):
                    query = query.as_string(cursor)
                await cursor.execute(query=query, params=params)

    async def _execute_single_return(
        self,
        query: str | psycopg.sql.Composed,
        params: Optional[List[Any]] = None
    ) -> Any:

        connection = await AsyncConnection.connect(**self.connection_factory())
        async with connection:
            async with connection.cursor() as cursor:
                if isinstance(query, psycopg.sql.Composed):
                    query = query.as_string(cursor)
                await cursor.execute(query=query, params=params)
                item = await cursor.fetchone()
                if item:
                    return dict(item)
                else:
                    return None
