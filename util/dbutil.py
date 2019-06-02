#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""
__version__ = "0.2.0"
__author__ = "m.yama"

from util.core import CHUNKSIZE, getencoding, Path
from util.dfutil import df_cast

import re
import os
import sys
from urllib.parse import quote_plus

# 3rd party modules
import pandas as pd

from sqlalchemy import create_engine

try:
    import pyodbc
except:
    if os.name == 'posix':
        sys.stderr.write("Please install pyodbc: \nubuntu => apt-get install unixodbc-dev;pip3 install pyodbc\nredhat => yum install unixODBC unixODBC-devel;pip3 install pyodbc\n")
    else:
        sys.stderr.write("Please install pyodbc:\npip3 install pyodbc\n")



__mdberrmsg = """
Not Found ODBC Driver Microsoft Access Driver...
 Please download -> https://www.microsoft.com/ja-jp/download/details.aspx?id=13255

 How to windows 64bit OS but 32bit Office..

    ########################################
    #  How to Install both 32bit and 64bit MS Access ODBC Driver on 64bit Windows
    #     ref. https://knowledge.autodesk.com/ja/support/autocad/learn-explore/caas/sfdcarticles/sfdcarticles/JPN/How-to-install-64-bit-Microsoft-Database-Drivers-alongside-32-bit-Microsoft-Office.html
    ########################################
    # STEP1 if exists then delete registry
    #           "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Office\14.0\Common\FilesPaths\mso.dll"
    # STEP2 64bit ODBCdriver install:
    #            64 bit Access driver 2010download
    #              ref. http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=13255
    # STEP3 run command
    #            AccessDatabaseEngine_x64.exe /passive
    # STEP4 delete registry
    #            "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Office\14.0\Common\FilesPaths\mso.dll"
    ########################################
"""
def con_mdb(f, uid="", passwd="", *args, **kw):
    target = "Microsoft Access Driver "
    driver = [x for x in pyodbc.drivers() if x.startswith(target)]
    if not driver:
        raise RuntimeError(__mdberrmsg)
    dsnstr = r'DRIVER={{{}}};DBQ={};UID="{}";PWD="{}";'
#    uri = 'access+pyodbc:///?odbc_connect=' + quote_plus(dsnstr.format(driver[0], f, uid, passwd))
#    return create_engine(uri)
    con = pyodbc.connect(dsnstr.format(driver[0], f, uid, passwd))
    with con.cursor() as cur:
        return con, [t.table_name for t in cur.tables() if not re.match("(MSys|~TMPCLP)", t.table_name)]

def con_sqlite(f, uid="", passwd="", *args, **kw):
    con = create_engine(r"sqlite:///" + f)
    return con, con.table_names()

def con_oracle(server, dbname, uid, passwd, port="1521", *args, **kw):
    con = create_engine("oracle+cx_oracle://{}:{}@{}/{}?port={}".format(uid, passwd,server,dbname,port))
    return con, con.table_names()

def con_sqlserver(server, dbname, uid, passwd, port=None, *args, **kw):
    target = "SQL Server Native Client"
    driver = sorted(x for x in pyodbc.drivers() if x.startswith(target))
    if not driver:
        raise RuntimeError("Not Found ODBC Driver " + target)
    dsnstr = r'DRIVER={{{}};SERVER={};DATABASE={};UID={};PWD={}'
    uri = 'mssql+pyodbc:///?odbc_connect=' + quote_plus(dsnstr.format(driver[-1],server,dbname,uid,passwd))
    con = create_engine(uri)
    return con, con.table_names()

def con_mysql(server, dbname, uid, passwd, port="3309", *args, **kw):
    con = create_engine(r'mysql://{}:{}@{}:{}/{}'.format(uid,passwd,server,port,dbname))
    return con, con.table_names()

def con_vertica(server, dbname, uid, passwd, port="5433", *args, **kw):
    con = create_engine(r'vertica+vertica_python://{}:{}@{}:{}/{}'.format(uid, passwd, server, port, dbname))
    return con, con.table_names()

def con_postgres(server, dbname, uid, passwd, port="5432", *args, **kw):
    con = create_engine(r'postgresql://{}:{}@{}:{}/{}'.format(uid,passwd,server,port,dbname))
    return con, con.table_names()

def con_db2(server, dbname, uid, passwd, port="50000", *args, **kw):
    con = create_engine(r'db2+ibm_db://{}:{}@{}:{}/{}'.format(uid,passwd,server,port,dbname))
    return con, con.table_names()

def parsesql(sql_or_table):
    if re.search("(CREATE|ALTER|DROP|SELECT|INSERT|UPDATE|DELETE|GRANT|REVOKE|COMMIT)\s", sql_or_table, re.I):
        return sql_or_table
    p = Path(sql_or_table)
    if p.is_file():
        b = p.read_byte()
        return b.decode(getencoding(b))
    else:
        return "SELECT * FROM {}".format(sql_or_table)

def read_sql(sql_or_table, con, *args, **kw):
    if isinstance(sql_or_table, str):
        ss = [sql_or_table]
    else:
        ss = sql_or_table
    for s in ss:
         for df in pd.read_sql(parsesql(s), con, chunksize=CHUNKSIZE, *args, **kw):
             yield s, df_cast(df)

def read_db(f, sql_or_table=None, uid="", passwd="", *args, **kw):
    sw = {".mdb": con_mdb, ".accdb": con_mdb,
          ".db": con_sqlite, ".sqlite": con_sqlite, ".sqlite3": con_sqlite}
    con, tables = sw[Path(f).ext](f, uid=uid, passwd=passwd)
    return read_sql(sql_or_table or tables, con, *args, **kw)

def read_dbsrv(server, dbname, sql_or_table=None, uid="", passwd="", vendor="mysql", port=None, *args, **kw):
    con_kw = dict(server=server, dbname=dbname, uid=uid, passwd=passwd)
    if port:
        con_kw.update(dict(port=port))
    con, tables = eval("con_" + vendor)(**con_kw)
    return read_sql(sql_or_table or tables, con, *args, **kw)


"""
   TestCase below
"""

def test():
    from util.core import tdir
    from datetime import datetime as dt

    def test_read():
        f = tdir + "sample.accdb"
        print(read_db(f))
        f = tdir + "sample.sqlite3"
        print(list(read_db(f)))


    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = dt.now()
            func()
            t2 = dt.now()
            print("{} : time {}".format(x, t2-t1))


if __name__ == "__main__":
    test()
