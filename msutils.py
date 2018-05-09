# -*- coding: utf-8 -*-
import os, sys
import pyodbc
import pandas as pd
from datetime import datetime
import xlrd
import win32com.client
from tempfile import gettempdir
import codecs
from io import StringIO

BUFSIZE = 1024 * 10 * 10 # 10M Lines

########################################
#  How to Install both 32bit and 64bit MS Access ODBC Driver on 64bit Windows
#     ref. https://knowledge.autodesk.com/ja/support/autocad/learn-explore/caas/sfdcarticles/sfdcarticles/JPN/How-to-install-64-bit-Microsoft-Database-Drivers-alongside-32-bit-Microsoft-Office.html
########################################
#STEP1 if exists then delete registry
#           "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Office\14.0\Common\FilesPaths\mso.dll"
#STEP2 64bit ODBCdriver install:
#            64 bit Access driver 2010download
#              ref. http://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=13255 
#STEP3 run command
#            AccessDatabaseEngine_x64.exe /passive
#STEP4 delete registry
#            "HKEY_LOCAL_MACHINE\SOFTWARE\Microsoft\Office\14.0\Common\FilesPaths\mso.dll"
########################################


ODBCDSN = dict( 
        mdb   = r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={datasource};UID="{uid}";PWD="{passwd}";',
        accdb = r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={datasource};UID="{uid}";PWD="{passwd}";',
        sqlsrv= r'DRIVER={{ODBC Driver 13 for SQL Server}};SERVER={datasource};DATABASE={dbname};UID={uid};PWD={passwd}',
        )
kind = {",": ".csv", "\t": ".tsv"}

ftype = {1 : "Boolean", 2 : "Byte", 3 : "Integer", 4 : "Long", 5 : "Currency", 6 : "Single", 7 : "Double", 8 : "Date", 9 : "Binary", 10 : "Text", 11 : "LongBinary", 12 : "Memo", 15 : "GUID", 16 : "BigInt", 17 : "VarBinary", 18 : "Char", 19 : "Numeric", 20 : "Decimal", 21 : "Float", 22 : "Time", 23 : "TimeStamp", 101 : "Attachment", 102 : "ComplexByte", 103 : "ComplexInteger", 104 : "ComplexLong", 105 : "ComplexSingle", 106 : "ComplexDouble", 107 : "ComplexGUID", 108 : "ComplexDecimal", 109 : "ComplexText"}
ntype = {"table": 0, "query": 1, "forms": 2, "reports": 3, "scripts": 4, "modules": 5, "serverview": 7, "diagram": 8, "storedprocedure": 9, "function": 10, "databaseproperties": 11, "tabledatamacro": 12}

values_at = lambda d, k: dict((x, d[x]) for x in set(d) & set(k))
values_not = lambda d, k: dict((x, d[x]) for x in set(d) - set(k))

def getconnection(datasource, uid="", passwd="", dbname=""):
    ext = os.path.splitext(datasource)[-1][1:]
    if ext in ["mdb", "accdb"]:
        dsn = ODBCDSN[ext].format(datasource=datasource, uid=uid, passwd=passwd)
    elif uid and dbname:
        dsn = ODBCDSN["sqlsrv"].format(datasource=datasource, uid=uid, passwd=passwd, dbname=dbname)
    else:
        raise AttributeError("Invalid arguments")
    return pyodbc.connect(dsn)


class MSDB(object):
    def __init__(self, filename_or_server, uid="", passwd="", dbname=""):
        self.filename = None
        self.server = None
        self.ext = os.path.splitext(filename_or_server)[-1][1:]
        self._engine = None
        self._mdb = None
        if self.ext in ["mdb", "accdb"]:
            self.dbtype =  "access"
            self.filename = filename_or_server
        else:
            self.dbtype = "sqlserver"
            self.server = filename_or_server
        self.uid = uid
        self.passwd = passwd
        self.dbname = dbname
        self.con = getconnection(datasource = filename_or_server, uid=uid, passwd=passwd, dbname=dbname)

    @property
    def engine(self):
        """ very slower office application functions
        """
        if self._engine is None:
            self._engine = win32com.client.Dispatch("Access.Application")
            self._engine.OpenCurrentDatabase(self.filename)
        return self._engine

    @property
    def mdb(self):
        """ very slower office application functions
        """
        if self._mdb is None:
            self._mdb = self.engine.CurrentDb()
        return self._mdb

    def tables(self):
        with self.con.cursor() as cur:
            if self.dbtype == "access":
                return [t.table_name for t in cur.tables() if t.table_type == "TABLE" and not t.table_name.startswith("~TMPCLP")]
            else:
                return [t.table_name for t in cur.tables()]
     
    def columns(self, tablename):
        return self.desc(tablename).name.tolist()

    def desc(self, tablename):
        """ faster pyodbc functions
        """
        with self.con.cursor() as cur:
            cur.execute("SELECT TOP 1 * FROM " + tablename)
            return pd.DataFrame(data=list(cur.description), columns=["name","type","dispsize","size","bytes","precision","nullable"])

    def ncol(self, tablename):
        return len(self.columns(tablename))
    
    def nrow(self, tablename):
        with self.con.cursor() as cur:
            ret = cur.execute("SELECT count(*) FROM " + tablename)
            return ret.fetchone()[0]

    def _mdb_summary(self):
        sio = StringIO("## File summary ##\n")
        st = os.stat(self.filename)
        inf = pd.DataFrame(data=[[
                self.filename or self.server,
                st.st_size,
                datetime.fromtimestamp(st.st_mtime).strftime("%Y/%m/%d %H:%M:%S")
                ]], columns=["datasource", "size", "mtime"])
        sio.write(inf.to_csv(index=False))

        tbl = self.tabledefs()
        if tbl.size > 0:
            sio.write("\n## Table summary ##\n")
            sio.write(tbl.to_csv())

        fld = pd.concat(self.fielddefs(t) for t in self.tables())            
        if fld.size > 0:
            sio.write("\n## Field summary ##\n")
            sio.write(fld.to_csv())

        qry = self.querydefs()
        if qry.size > 0:
            sio.write("\n## Query summary ##\n")
            sio.write(qry.to_csv())

        frm = self.formdefs()
        if frm.size > 0:
            sio.write("\n## Form summary ##\n")
            sio.write(frm.to_csv())

        rep = self.reportdefs()
        if rep.size > 0:
            sio.write("\n## Report summary ##\n")
            sio.write(rep.to_csv())

        mac = self.macrodefs()
        if mac.size > 0:
            sio.write("\n## Macro summary ##\n")
            sio.write(mac.to_csv())

        mod = self.moduledefs()
        if mod.size > 0:
            sio.write("\n## Module summary ##\n")
            sio.write(mod.to_csv())
        
        return sio.getvalue()
    
    def _sqlsrv_summary(self):
        sio = StringIO("## Server summary ##\n")
        tbl = self.tables()
        if tbl.size > 0:
            sio.write("## Table summary ##")
            sio.writelines(tbl)
        fld = self.readsql("""SELECT     t.object_id, t.name [table name], c.column_id, c.name [column name]
                              FROM       sys.objects t
                              INNER JOIN sys.columns c
                                    ON   t.object_id = c.object_id
                              WHERE      t.type = 'U'
                              ORDER BY   t.name, c.column_id;
                           """, chunksize=None)
        if fld.size > 0:
            sio.write("## Field summary ##")
            sio.write(fld.to_csv())
        idx = self.readsql("""SELECT i.name AS index_name, o.name AS table_name, col.name AS column_name
                              FROM   sysindexkeys ik, sysobjects o, syscolumns col, sysindexes i
                              WHERE  ik.id = o.id
                                 AND ik.id = col.id
                                 AND ik.colid = col.colid
                                 AND ik.id = i.id
                                 AND ik.indid = i.indid
                                 AND o.xtype = 'U'
                              ORDER BY i.name, ik.id, ik.indid, ik.keyno;
                           """, chunksize=None)
        if idx.size > 0:
            sio.write("## Index summary ##")
            sio.write(idx.to_csv())

    def summary(self):
        if self.dbtype == "access":
            return self._mdb_summary()
        elif self.dbtype == "sqlserver":
            return self._sqlsrv_summary()
    
    def readsql(self, sql, chunksize=BUFSIZE, *args, **kw):
        """generator table
             return: pandas series of row
        """
        if hasattr(sql, "read"):
            sql = sql.read()
        elif os.path.exists(sql):
            sql = open(sql).read()
        elif isinstance(sql, str):
            sql = sql.strip()
        else:
            raise AttributeError("Unknown is `{}`".format(sql))

        chunk = pd.read_sql(sql, self.con, chunksize=chunksize, *args, **values_not(kw, ["header"]))
        for idx, df in enumerate(chunk):
            if idx == 0 and kw.get("header"):
                yield df.columns
            for i, s in df.iterrows():
                yield s
     
    def readrow(self, tablename, *args, **kw):
        sql = "SELECT * FROM " + tablename
        return self.readsql(sql, *args, **kw)
         
    def readrowall(self, *args, **kw):
        """each All tables Rows Pandas Series
            return: dict -> tablename, generator(pandas series of row)
        """
        return dict([table,self.readrow(table, *args, **kw)] for table in self.tables())

    def readlines(self, tablename, *args, **kw):
        """generator table
            return: list
        """
        return (s.tolist() for s in self.readrow(tablename, *args, **kw)) 

    def readlinesall(self, *args, **kw):
        """each All tables Rows
            return: dict-> tablename, generator(row)
        """
        return dict([t, self.readlines(t, *args, **kw)] for t in self.tables())
        
    def dump(self, tablename, outputfilename, sep=",", encoding="cp932", *args, **kw):
        """
        return filepath
        """
        sql = "SELECT * FROM " + tablename
        return self.sqldump(sql, outputfilename, sep=sep, encoding=encoding, chunksize=BUFSIZE, *args, **kw)

    def sqldump(self, sql, outputfilename, sep=",", encoding="cp932", chunksize=BUFSIZE, *args, **kw):
        """
        return filepath
        """
        chunk = pd.read_sql(sql, self.con, chunksize=chunksize, *args, **kw)
        mode = "w"
        for df in chunk:
            df.to_csv(outputfilename, mode=mode, index=False, header= mode=="w", sep=sep, encoding=encoding)
            mode = "a"
        return outputfilename
    
    def dumpall(self, outputdir, sep=",", *args, **kw):
        """
        return filepath list
        """
        ret = []
        for t in self.tables():
            fn = os.path.join(outputdir, t + kind.get(sep, ".txt"))
            self.dump(t, fn, sep=sep, *args, **kw)
            ret.append(fn)
        return ret
       
    def tabledefs(self):
        """ very slower office application functions
        """
        ret = []
        for td in list(self.mdb.TableDefs):
            name = td.Name
            if name.startswith("~TMPCLP") or name.startswith("MSys"):
                continue
        
            link = td.Connect
            src  = td.SourceTableName # Excel -> sheetname, Access -> Tablename
            
            try:
                ncol = len(td.Fields)
            except:
                ncol = ""
            
            r = [name, ncol]
            if link:
                prefix = link.split(";DATABASE=")[-1]
                if link[:4] in ["Text", "HTML", "dBAS", "Para"]:
                    r.extend(["", os.path.join(prefix, src), ""])
                else:
                    r.extend(["", prefix, src.replace("$","")])
            else:
                r.extend([td.RecordCount, "", ""])
            ret.append(r)
        return pd.DataFrame(data = ret, columns="tablename,ncol,nrow,linkpath,srctablename".split(","))

    def querydefs(self):
        """ very slower office application functions
        """
        return pd.DataFrame(data=([qd.Name, qd.Sql] for qd in list(self.mdb.QueryDefs)), columns=["queryname","querystring"])
    
    def fielddefs(self, tablename):
        """ very slower office application functions
        """
        ret = []
        td = self.mdb.TableDefs(tablename)
        if not td.Connect:
            ret.append([tablename, "", "", "", "", "", ""])
        for fld in list(td.Fields):
            ret.append([tablename, fld.Name, ftype[fld.Type], fld.Size, fld.Required, fld.AllowZeroLength, fld.DefaultValue])
        return pd.DataFrame(data=ret, columns="tablename, fieldname,fieldtype,fieldsize,isrequire,allowzero,defaultvalue".split(","))
    
    def _defs(self, name):
        """ very slower office application functions
            <http://d.hatena.ne.jp/osaca_z4/20100201/1265035288>
        """
        ret = []
        tmp = os.path.join(gettempdir(), name + ".tmp")
        for d in list(self.mdb.Containers(name).Documents):
            self.engine.SaveAsText(ntype[name], d.Name, tmp)
            try:
                ret.append([d.Name, codecs.open(tmp,encoding="utf-16").read()])
            except UnicodeError:
                ret.append([d.Name, codecs.open(tmp,encoding="cp932").read()])
            finally:
                os.remove(tmp)
        return pd.DataFrame(data=ret, columns="{0}name,{0}string".format(name).split(","))
    
    #very slower office application functions
    def formdefs(self):   return self._defs("forms")
    def reportdefs(self): return self._defs("reports")    
    def macrodefs(self):  return self._defs("scripts")    
    def moduledefs(self): return self._defs("modules")

    def close(self):
        self.con.close()
        if self._mdb:
            self._mdb.Close()
            self._mdb = None
        if self._engine:
            self._engine.DoCmd.CloseDatabase
            self._engine.Quit()
            self._engine = None
        
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            sys.stderr.write("{}\n{}\n{}".format(exc_type, exc_value, traceback))
        self.close()

class Excel(object):
    def __init__(self, filename, uid="", passwd=""):
        self.filename = filename
        self.ext = os.path.splitext(filename)[-1][1:]
        self.uid = uid
        self.passwd = passwd
        self.wb = xlrd.open_workbook(filename, on_demand = True)
        self.ws = self.wb.sheets()
        self.ret = {}

    def sheets(self):
        return [s.name for s in self.ws]
     
    def columns(self, sheet):
        return self.readexcel(sheet).columns.tolist()

    def readexcel(self, sheet, keep_default_na=False, *args, **kw):
        if hasattr(sheet, "name"):
            sheet = sheet.name
        header = kw.get("header", None)
        if header is False or header is None:
            kw["header"] = None
        elif header is True:
            kw["header"] = 0
        else:
            kw["header"] = header
        
        return pd.read_excel(self.wb, sheet, engine="xlrd", keep_default_na=keep_default_na, dtype="object", *args, **kw)

    def nrow(self, sheet):
        return self.readexcel(sheet).shape[0]

    def ncol(self, sheet):
        return self.readexcel(sheet).shape[1]

    def summary(self): #TODO
        ret = dict(sheets=[])
        if self.filename:
            st = os.stat(self.filename)
            ret.update(
                        dict(
                            datasource = self.filename,
                            size       = st.st_size,
                            mtime      = datetime.fromtimestamp(st.st_mtime).strftime("%Y/%m/%d %H:%M:%S"),
                            )
                    )
        for s in self.sheets():
            ret["sheets"].append(
                dict(name  = s,
                     nrows = self.nrow(s),
                     columns = self.columns(s))
                )
        return ret
     
    def readrow(self, sheet, *args, **kw):
        if sheet is None:
            raise ValueError("SheetName is require. `None` is Non-Valid")
        df = self.readexcel(sheet, *args, **kw)
        if kw.get("header") or kw.get("header",-1) >= 0:
            return [df.columns] + [r for i, r in df.iterrows()]
        else:
            return [r for i, r in df.iterrows()]
         
    def readrowall(self, *args, **kw):
        """each All tables Rows Pandas Series
            return: dict -> tablename, generator(pandas series of row)
        """
        return dict([sheet,self.readrow(sheet, *args, **kw)] for sheet in self.sheets())

    def readlines(self, sheet, *args, **kw):
        """generator table
            return: list
        """
        return [s.tolist() for s in self.readrow(sheet, *args, **kw)]

    def readlinesall(self, *args, **kw):
        """each All tables Rows
            return: dict-> tablename, generator(row)
        """
        return dict([s, self.readlines(s, *args, **kw)] for s in self.sheets())
        
    def dump(self, sheet, outputfilename, sep=",", encoding="cp932", *args, **kw):
        """
        return filepath
        """
        mode = "w"
        self.readexcel(sheet, *args, **kw).to_csv(outputfilename, mode=mode, index=False, header= mode=="w", sep=sep, encoding=encoding)
        return outputfilename
    
    def dumpall(self, outputdir, sep=",", *args, **kw):
        """
        return filepath list
        """
        ret = []
        for s in self.sheets():
            fn = os.path.join(outputdir, s + kind.get(sep, ".txt"))
            self.dump(s, fn, sep=sep, *args, **kw)
            ret.append(fn)
        return ret
       
    def close(self):
        del self.wb
        
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            sys.stderr.write("{}\n{}\n{}".format(exc_type, exc_value, traceback))
        self.close()

def flatten(L):
    ret = []
    fr = list(L)

    while len(fr) > 0:
        n = fr.pop(0)
        if isinstance(n, list):
            fr = n + fr
        else:
            ret.append(n)
    return ret


def mkreport(filename):
    ext = os.path.splitext(filename)[-1].lower()
    if ext in [".accdb", ".mdb"]:
        with MSDB(filename) as db:
            return db.summary()
    if ext.startswith(".xls"):
        with Excel(filename) as xl:
            return xl.summary()

def test_a():
#    global filename, db, t, c, d, n, r1, r2, dp, ra1, ra2, dpa, sm, de,dm,dm1,dm2,dm3,dm4,dm5,dm6,dm7
    filename = r'C:\temp\sample.accdb'
    db = MSDB(filename)
    t = db.tables()
    c = db.columns(t[0])
    d = db.desc(t[0])
    n = db.nrow(t[0])
    r1 = db.readrow(t[0])
    r2 = db.readlines(t[0])
    dp = db.dump(t[0], os.path.join(os.path.dirname(filename),t[0] + ".csv"))
    ra1 = db.readrowall()
    ra2 = db.readlinesall()
    dpa = db.dumpall(os.path.dirname(filename))
    sm = db.summary()
    de = db.engine
    dm = db.mdb
    #dsq = db.sqldump(sql, outputfilename, sep=",", encoding="cp932", chunksize=BUFSIZE)
    dm1 = db.tabledefs()
    dm2 = db.querydefs()
    dm3 = db.fielddefs(t[0])
    dm4 = db.formdefs()
    dm5 = db.reportdefs()
    dm6 = db.macrodefs()
    dm7 = db.moduledefs()
    db.close()

def test_e():
    filename = r"C:\temp\meta.xlsx"
#    global xl,xs,xc,nr,nc,rr,rl,sm
    xl = Excel(filename)
    xs = xl.sheets()
    xc = xl.columns(xs[0])
    nr = xl.nrow(xs[0])
    nc = xl.ncol(xs[0])
    rr = list(xl.readrow(xs[0]))
    rl = xl.readlines(xs[0])
    sm = xl.summary()
    xl.close()

def test():
    test_a()
    test_e()
        
def main():
    import argparse
    import configparser
    from glob import glob
    
    usage="""Microsoft AccessDB or sqlserver dumper
    MS Access Table Data All Dump
     Usage1: python {0} -o "C:\\hoge" test.mdb
     Usage2: python {0} *.mdb

    MS SQLServer Table Data Dump
     Usage3: python {0} -o "C:\\hoge" test.sql
     Usage4: python {0} *.sql

    Connection Setting Example: env.ini
        [DEFAULT]
        server=192.168.1.1
        uid=admin
        passwd=admin
        dbname=HOGE

    """.format(os.path.basename(sys.argv[0]))
    
    parser = argparse.ArgumentParser(usage)
    conf = configparser.ConfigParser()
    
    parser.add_argument("-o", "--outdir",
                         help="output directory path of dump textdata",
                         default=None)
    parser.add_argument("-c", "--config",
                         help="ini file: SQL Server DB User, Password, DBname",
                         default=os.path.join(os.path.dirname(sys.argv[0]),"env.ini"))
    parser.add_argument("-s", "--sep",
                         help="output file delimitter default `,`",
                         default=",")
    parser.add_argument("-e", "--encoding",
                         help="output file encoding default `cp932`",
                         default="cp932")
    parser.add_argument("-u", "--uid",
                         help="DB Login Username",
                         default=None)
    parser.add_argument("-p", "--passwd",
                         help="DB Login password",
                         default=None)
    parser.add_argument("-d", "--dbname",
                         help="Target DB Name :options SQLSever only",
                         default=None)
    parser.add_argument("-S", "--server",
                         help="Target ServerName :options SQLSever only",
                         default=None)
    parser.add_argument("filename",
                         metavar="<filename>",
                         nargs="+",
                         default=[],
                         help="target Access DB files or sql files for SQL server")
    args = parser.parse_args()
    kw = dict(filename_or_server=None, uid="", passwd="", dbname="")
    #load config
    if os.path.exists(args.config):
        _k = conf.read(args.config)
        if _k["DEFAULT"]["server"]:
            kw["filename_or_server"] = _k["DEFAULT"]["server"]
        for k in set(kw.keys()) & set(_k):
            kw[k] = _k["DEFAULT"][k]

    # options override settings
    if args.uid:
        kw["uid"] = args.uid
    if args.passwd:
        kw["passwd"] = args.passwd
    if args.dbname:
        kw["dbname"] = args.dbname
    if args.server:
        kw["filename_or_server"] = args.server


    files = flatten(glob(x) for x in args.filename) #[glob(x) for x in args.filename]
    
    if len(files) == 0:
        sys.stderr.write("Non Arguments files", newline="\n")
        sys.exit(1)
        
    for f in files:
        outdir = args.outdir or os.path.dirname(f)
        bf, ext = os.path.splitext(os.path.basename(f))
        f = os.path.normpath(f)
        if ext.lower() in [".mdb",".accdb"]:
            with MSDB(f, kw["uid"], kw["passwd"]) as db:
                db.dumpall(outdir, sep=args.sep, encoding=args.encoding)
        elif ext.lower().startswith(".xls"):
            with Excel(f, kw["uid"], kw["passwd"]) as xl:
                xl.dumpall(outdir, sep=args.sep, encoding=args.encoding)
        elif ext.lower() == ".sql":
            with MSDB(**kw) as db:
                db.sqldump(f, os.path.join(outdir, bf + kind.get(args.sep, ".txt"), sep=args.sep, encoding=args.encoding))
        else:
            sys.stderr.write("[WARN]skip: {} \n`{}` is Unknown file type.".format(f, ext[1:]), newline="\n")


if __name__ == "__main__":
#    test()
#    sys.argv.extend([filename])
    main()
    
#    with MSDB("C:/temp/sample.accdb") as db:
#        print(db.summary())
#        print(list(db.tabledefs()))
    
