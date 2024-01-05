import psycopg.sql

from postgres import ConnectionFactory
from psycopg import AsyncConnection
from typing import Optional, List, Any, Generator


class AsyncConnectionFactory(ConnectionFactory):
    # Subclassing to reuse the __init__ definition

    async def __call__(
            self,
            db_name: Optional[str] = None
    ) -> AsyncConnection:
        if not db_name:
            db_name = self.db_name

        return AsyncConnection.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=db_name,
            sslmode='require' if self.ssl else 'allow')


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
        async with await self.connection_factory() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, values)
                return [dict(item) async for item in cursor]

    async def _execute_no_return(
        self,
        sql: str | psycopg.sql.Composed,
        values: Optional[List[Any]] = None
    ) -> None:
        async with await self.connection_factory() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, values)

    async def _execute_single_return(
        self,
        sql: str | psycopg.sql.Composed,
        values: Optional[List[Any]] = None
    ) -> Any:
        async with await self.connection_factory() as conn:
            async with conn.cursor() as cursor:
                await cursor.execute(sql, values)
                item = await cursor.fetchone()
                if item:
                    return dict(item)
                else:
                    return None
