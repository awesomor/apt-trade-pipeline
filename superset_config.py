import sys, os, oracledb

_original_connect = oracledb.connect

def _patched_connect(*args, **kwargs):
    kwargs.setdefault('config_dir', os.getenv("WALLET_LOCATION"))
    kwargs.setdefault('wallet_location',os.getenv("WALLET_LOCATION"))
    kwargs.setdefault('wallet_password', os.getenv("WALLET_PASSWORD"))
    return _original_connect(*args, **kwargs)

oracledb.connect = _patched_connect
oracledb.version = "8.3.0"
sys.modules["cx_Oracle"] = oracledb
