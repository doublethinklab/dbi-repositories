import psycopg.sql

from dbi_repositories.postgres import ConnectionFactory
from psycopg import AsyncConnection
from typing import Optional, List, Any, Generator
from contextlib import asynccontextmanager


class AsyncConnectionFactory(ConnectionFactory):
    # Subclassing to reuse the __init__ definition

    async def __call__(
            self,
            db_name: Optional[str] = None
    ) -> AsyncConnection:
        if not db_name:
            db_name = self.db_name

        async with await AsyncConnection.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=db_name,
            sslmode='require' if self.ssl else 'allow'
        ) as conn:
            yield conn



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
        sql: str | psycopg.sql.Composed,
        values: Optional[List[Any]] = None
    ) -> List:
        connection = self.connection_factory()
        async with connection:
            async with connection.cursor() as cursor:
                return await cursor.execute(sql, values)

    async def _execute_no_return(
        self,
        sql: str | psycopg.sql.Composed,
        values: Optional[List[Any]] = None
    ) -> None:
        connection = self.connection_factory()
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(sql, values)

    async def _execute_single_return(
        self,
        sql: str | psycopg.sql.Composed,
        values: Optional[List[Any]] = None
    ) -> Any:
        connection = self.connection_factory()
        async with connection:
            async with connection.cursor() as cursor:
                await cursor.execute(sql, values)
                item = await cursor.fetchone()
                if item:
                    return dict(item)
                else:
                    return None
