import ctypes, json, sys
from pathlib import Path

if sys.platform.startswith("win"):
    LIB_BASENAME = "pggo.dll"
elif sys.platform == "darwin":
    LIB_BASENAME = "libpggo.dylib"
else:
    LIB_BASENAME = "libpggo.so"

LIB_PATH = Path(__file__).with_name(LIB_BASENAME)
_lib = ctypes.CDLL(str(LIB_PATH))

# assinaturas
_lib.ConnectJSON.argtypes = [ctypes.c_char_p]
_lib.ConnectJSON.restype  = ctypes.c_void_p
_lib.CloseJSON.argtypes   = [ctypes.c_ulonglong]
_lib.CloseJSON.restype    = ctypes.c_void_p
_lib.QueryJSON.argtypes   = [ctypes.c_ulonglong, ctypes.c_char_p]
_lib.QueryJSON.restype    = ctypes.c_void_p
_lib.ExecJSON.argtypes    = [ctypes.c_ulonglong, ctypes.c_char_p]
_lib.ExecJSON.restype     = ctypes.c_void_p
_lib.FreeCString.argtypes = [ctypes.c_void_p]
_lib.FreeCString.restype  = None

def _from_c(ptr):
    try:
        s = ctypes.cast(ptr, ctypes.c_char_p).value.decode()
        return json.loads(s)
    finally:
        _lib.FreeCString(ptr)

def connect_json(conninfo: str):
    return _from_c(_lib.ConnectJSON(conninfo.encode()))

def close_json(handle: int):
    return _from_c(_lib.CloseJSON(handle))

def query_json(handle: int, sql: str):
    return _from_c(_lib.QueryJSON(handle, sql.encode()))

def exec_json(handle: int, sql: str):
    return _from_c(_lib.ExecJSON(handle, sql.encode()))
