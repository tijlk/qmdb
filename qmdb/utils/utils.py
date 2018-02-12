from qmdb.database.database import MySQLDatabase

try:
    import httplib
except ModuleNotFoundError:
    import http.client as httplib


def no_internet():
    conn = httplib.HTTPConnection("www.google.com", timeout=5)
    try:
        conn.request("HEAD", "/")
        conn.close()
        return False
    except:
        conn.close()
        return True


def create_copy_of_table(src, tgt, schema='qmdb_test'):
    db = MySQLDatabase(schema=schema)
    db.remove_table(tgt)
    db.connect()
    db.c.execute("create table {} as select * from {}".format(tgt, src))
    db.close()
