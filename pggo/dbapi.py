# Interface mínima "DB-API-like" (não 100% PEP 249 ainda)
from ._binding import (
    connect_json, 
    close_json, 
    query_params_json,
    exec_params_json,
)

class Error(Exception): ...
class ProgrammingError(Error): ...
class DatabaseError(Error): ...

class Connection:
    def __init__(self, handle: int):
        self._h = handle
        self.closed = False

    # Context manager -> with pggo.connect(...) as conn
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.close()
        finally:
            return False


    def cursor(self):
        return Cursor(self)

    def close(self):
        if self.closed: 
            return
        r = close_json(self._h)
        if r.get("error"):
            raise DatabaseError(r["error"])
        self.closed = True

    # no-ops para compat simples
    def commit(self): pass
    def rollback(self): pass

class Cursor:
    def __init__(self, conn: Connection):
        self._conn = conn
        self._last = None
        self.rowcount = -1
        self.closed = False

    # Context manager -> with conn.cursor() as cur
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False
    
    def close(self):
        self._last = None
        self.closed = True

    def query(self, sql: str, params=""):

        if self.closed:
            raise DatabaseError("cursor already closed")
        
        r = query_params_json(self._conn._h, sql, params)

        if isinstance(r, dict) and r.get("error"):
            raise DatabaseError(r["error"])
        
        self._last = r  # lista de dicts
        self.rowcount = len(self._last)

        return self

    def execute(self, sql: str, params=""):

        r = exec_params_json(self._conn._h, sql, params)

        if r.get("error"):
            raise DatabaseError(r["error"])
        
        self._last = None
        self.rowcount = r.get("rows_affected", -1)
        
        return self

    def fetchall(self):
        if self._last is None:
            return []
        return self._last

    def fetchone(self):
        if not self._last:
            return None
        return self._last.pop(0)

def connect(conninfo: str) -> Connection:
    r = connect_json(conninfo)
    if r.get("error"):
        raise DatabaseError(r["error"])
    return Connection(r["handle"])
