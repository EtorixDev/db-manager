import asyncio
import logging
import sqlite3

import aiosqlite

logger = logging.getLogger(__name__)


class SyncCursorHandler:
    """Custom sqlite3 cursor for using a context manager with DatabaseConnection."""

    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self.cursor = cursor

    def __enter__(self) -> sqlite3.Cursor:
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.cursor.close()


class SyncConnectionHandler:
    """Custom sqlite3 handler for using a synchronous context manager with DatabaseConnection."""

    def __init__(self, conn: sqlite3.Connection) -> None:
        self._conn = conn

    def cursor(self) -> SyncCursorHandler:
        """Create a SyncCursorHandler wrapper for an sqlite3 cursor object."""
        return SyncCursorHandler(self._conn.cursor())

    def __getattr__(self, name):
        """Delegate every other method back to the default sqlite3 cursor."""
        return getattr(self._conn, name)


class DatabaseConnection:
    """Single database connection for infrequent events. Asynchronous and Synchronous."""

    def __init__(self, database_url, sync=True) -> None:
        self.database_url = database_url
        self.sync = sync

    async def __aenter__(self) -> aiosqlite.Connection:
        if self.sync:
            raise ValueError("Cannot use asynchronous context manager with synchronous connection.")

        self._conn = await aiosqlite.connect(self.database_url)
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not exc_type:
            await self._conn.commit()
        else:
            await self._conn.rollback()

        await self._conn.close()

    def __enter__(self) -> SyncConnectionHandler:
        if not self.sync:
            raise ValueError("Cannot use synchronous context manager with asynchronous connection.")

        self._conn = SyncConnectionHandler(sqlite3.connect(self.database_url))
        return self._conn

    def __exit__(self, exc_type, exc, tb) -> None:
        if not exc_type:
            self._conn.commit()
        else:
            self._conn.rollback()

        self._conn.close()


class DatabasePool:
    """Pool of database connections for frequent events. Asynchronous only."""

    def __init__(self, database_url, max_size=25, health_check_interval=300, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self.database_url = database_url
        self.pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue(max_size)
        self.lock = asyncio.Lock()
        self.health_check_interval = health_check_interval

        if loop:
            self.monitoring_task = loop.create_task(self._monitor_connections())
        else:
            self.monitoring_task = asyncio.create_task(self._monitor_connections())

    async def __aenter__(self) -> aiosqlite.Connection:
        self._conn = await self._get_connection()
        return self._conn

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if not exc_type:
            await self._conn.commit()
        else:
            await self._conn.rollback()

        await self._release_connection(self._conn)

    async def _get_connection(self) -> aiosqlite.Connection:
        async with self.lock:
            if self.pool.empty() and self.pool.qsize() < self.pool.maxsize:
                conn = await asyncio.wait_for(aiosqlite.connect(self.database_url), timeout=5)
                return conn
            else:
                return await self.pool.get()

    async def _release_connection(self, conn) -> None:
        await self.pool.put(conn)

    async def _monitor_connections(self) -> None:
        while True:
            await asyncio.sleep(self.health_check_interval)
            new_pool = asyncio.Queue(self.pool.maxsize)

            while not self.pool.empty():
                conn = await self.pool.get()

                try:
                    await conn.execute("SELECT 1")
                except (aiosqlite.OperationalError, aiosqlite.DatabaseError):
                    await conn.close()

                    try:
                        conn = await aiosqlite.connect(self.database_url)
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
