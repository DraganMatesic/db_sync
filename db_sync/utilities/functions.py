import hashlib


def get_hash(data):
    return hashlib.sha3_256(str(data).encode()).hexdigest()


def db_hash(orm_objs: list, hash_columns: list, hash_col='sha'):
    """
    Function that serves for calculating hash value of orm objects

    :param list orm_objs: list of sqlalchemy orm objects
    :param list hash_columns: list of column names that will be used for hash calc
    :param str hash_col: name of orm column where hash value will be saved, default column name is sha
    :return:
    """
    [setattr(orm_obj, hash_col, get_hash([getattr(orm_obj, col) for col in hash_columns])) for orm_obj in orm_objs]


def set_compare(objs1, objs2, primary_key, full=True):
    filtered = list()
    records = set(objs1).difference(objs2)
    for obj1 in records:
        if full is False:
            filtered.append(getattr(obj1, primary_key))
        else:
            filtered.append(obj1)
    return filtered
