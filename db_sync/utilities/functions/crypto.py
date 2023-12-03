import hashlib


def get_hash(data):
    return hashlib.sha3_256(str(data).encode()).hexdigest()


def db_hash(mapper, connection, table, cols: list = None, *args, **kwargs):
    if cols is not None:
        table.sha = get_hash([getattr(table, col) for col in cols])
