import asyncio
import logging

import psycopg

logger = logging.getLogger(__name__)


class AsyncPostgresConnection:
    """Single database connection for infrequent events."""

    def __init__(self, dbname: str, user: str, password: str, host: str, port: int) -> None:
        self.dsn = f"dbname={dbname} user={user} password={password} host={host} port={port}"

    async def __aenter__(self) -> psycopg.AsyncConnection:
        self._conn = await psycopg.AsyncConnection.connect(self.dsn)
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        assert isinstance(self._conn, psycopg.AsyncConnection)

        if not exc_type:
            await self._conn.commit()
        else:
            await self._conn.rollback()

        await self._conn.close()


class SyncPostgresConnection:
    """Single database connection for infrequent events."""

    def __init__(self, dbname: str, user: str, password: str, host: str, port: int) -> None:
        self.dsn = f"dbname={dbname} user={user} password={password} host={host} port={port}"

    def __enter__(self) -> psycopg.Connection:
        self._conn = psycopg.connect(self.dsn)
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if not exc_type:
            self._conn.commit()
        else:
            self._conn.rollback()

        self._conn.close()


class AsyncPostgresPool:
    """Pool of database connections for frequent events."""

    def __init__(self, dbname: str, user: str, password: str, host: str, port: int, max_size: int = 25, health_check_interval: int = 300, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self.dsn = f"dbname={dbname} user={user} password={password} host={host} port={port}"
        self.pool: asyncio.Queue[psycopg.AsyncConnection] = asyncio.Queue(max_size)
        self.lock = asyncio.Lock()
        self.health_check_interval = health_check_interval

        if loop:
            self.monitoring_task = loop.create_task(self._monitor_connections())
        else:
            self.monitoring_task = asyncio.create_task(self._monitor_connections())

    async def __aenter__(self) -> psycopg.AsyncConnection:
        self._conn = await self._get_connection()
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not exc_type:
            await self._conn.commit()
        else:
            await self._conn.rollback()

        await self._release_connection(self._conn)

    async def _get_connection(self) -> psycopg.AsyncConnection:
        async with self.lock:
            if self.pool.empty() and self.pool.qsize() < self.pool.maxsize:
                conn = await asyncio.wait_for(psycopg.AsyncConnection.connect(self.dsn), timeout=5)
                return conn
            else:
                return await self.pool.get()

    async def _release_connection(self, conn: psycopg.AsyncConnection) -> None:
        await self.pool.put(conn)

    async def _monitor_connections(self) -> None:
        while True:
            await asyncio.sleep(self.health_check_interval)
            new_pool = asyncio.Queue(self.pool.maxsize)

            while not self.pool.empty():
                conn = await self.pool.get()

                try:
                    async with conn.cursor() as cur:
                        await cur.execute("SELECT 1")
                except (psycopg.OperationalError, psycopg.DatabaseError):
                    await conn.close()

                    try:
                        conn = await psycopg.AsyncConnection.connect(self.dsn)
                    except Exception as e:
                        logger.error(f"Failed to reconnect to database: {e}")
                        continue

                await new_pool.put(conn)

            self.pool = new_pool

    async def close_pool(self) -> None:
        self.monitoring_task.cancel()

        while not self.pool.empty():
            conn = await self.pool.get()
            await conn.close()
