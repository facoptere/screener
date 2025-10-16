from time import time_ns, sleep
from typing import Any, Optional
from sqlitedict import SqliteDict
import logging

logger = logging.getLogger()


class CachedApi:
    def __init__(self, fullpath: str) -> None:
        self.__fullpath = fullpath
        self.__db: Optional[SqliteDict] = None
        logger.debug("Initializing cache at %s", self.__fullpath)

    def open_db(self) -> None:
        """Open the database and perform cleanup of expired entries."""
        logger.debug("Opening DB %s", self.__fullpath)
        self.__db = SqliteDict(self.__fullpath)
        self._cleanup_expired_entries()

    def _cleanup_expired_entries(self) -> None:
        """Remove expired cache entries."""
        if not self.__db:
            return
        removed = 0
        kept = 0
        for key in list(self.__db.keys()):
            try:
                counter, period, _ = key.split("\t", 2)
                current = int(time_ns() / 1_000_000_000 / int(period))
                if current - int(counter) > 1:
                    del self.__db[key]
                    removed += 1
                else:
                    kept += 1
            except ValueError:
                # Invalid key format, skip
                pass
        logger.debug("DB cleanup: keeping %d items, removed %d items", kept, removed)


    def __del__(self) -> None:
        logger.debug(f"Instance {self} destroyed.")

    def cache_get(self, key: str, period: int) -> Any:
        """
        Retrieve a value from cache.

        Args:
            key: Cache key.
            period: Cache period in seconds.

        Returns:
            Cached value or None if not found.
        """
        result = None
        idx = self._get_idx(key, period)
        try:
            result = self.__db[idx]  # type: ignore
        except KeyError:
            sleep(0.1)  # Brief pause on miss
        return result

    def cache_set(self, key: str, period: int, value: Any) -> None:
        """
        Store a value in cache.

        Args:
            key: Cache key.
            period: Cache period in seconds.
            value: Value to cache.
        """
        idx = self._get_idx(key, period)
        self.__db[idx] = value  # type: ignore
        self.__db.commit()  # type: ignore

    def _get_idx(self, key: str, period: int) -> str:
        """Generate cache index."""
        counter = int(time_ns() / 1_000_000_000 / period)
        return f"{counter}\t{period}\t{key}"
