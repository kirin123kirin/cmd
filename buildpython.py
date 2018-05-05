# -*- coding: utf-8 -*-
from os.path import dirname, basename, join as pathjoin, abspath, sep, splitext, split as pathsplit, exists as pathexists
import shutil
import os
import re
import pathlib
from zipfile import ZipFile, ZIP_DEFLATED
from glob import iglob
import pandas as pd
import numpy as np

inpdir = os.getenv("PYTHONPATH")
inproot,pythondirs = os.path.splitdrive(inpdir)

outroot = r"C:\temp\zipdata"
outdir = outroot + pythondirs

libdir = pathjoin(outdir, "Lib")

archivedir=r"\\FREENAS\data1\Downloads"

def lsdir(path):
    extexclude = ['.c', '.cpp', '.cs', '.csc', '.csh', '.h', '.java', '.lock', '.py', '.sample', '.pyx', 'Thumb.db', '.chm', "desktop.ini", ".shp"]
    direxclude = [".git", ".svn", "\\demos\\","\\demo\\", "\\Demos\\", ".dist-info", ".egg-info", "\\tests\\", "\\example"]
    for pth in iglob(path):
        for root, dirs, files in os.walk(pth):
            if re.search("(PyInstaller|altgraph)-[0-9.]+.dist-info", root):
                pass
            elif any(x in root for x in direxclude):
                continue
            for file in files:
                if file in extexclude or splitext(file)[-1] in extexclude:
                    continue
                if file.endswith(".pyc") and ".opt-" in file:
                    continue
                yield pathjoin(root, file)

def all_exept_python(path):
    for pth in iglob(path):
        for root, dirs, files in os.walk(pth):
            if pythondirs in root:
                continue
            for file in files:
                yield os.path.join(root, file)

def getdst(src):
    dst = src.replace(inpdir, outdir)
    if dst.lower().endswith(".pyc"):
        dst = dst.replace(".cpython-36.pyc","") + ".pyc"
        dst = dst.replace(".pyc.pyc", ".pyc")
    dst = dst.replace("\__pycache__","")
    dst = dst.replace(r"\site-packages","")
    return dst.replace(r"\Lib\pywin32_system32","")

def maketable(inpdir=inpdir, outdir=outdir):
    src = pd.Series(lsdir(inpdir))
    excepts = [e for e in [pathjoin(inpdir, "Lib\\site-packages\\bokeh\\core\\__init__.py")] if pathexists(e)]
    if excepts:
        src = src.append(pd.Series(data=excepts, name="src"),ignore_index=True)
    df = pd.DataFrame(data=src.apply(pathsplit).tolist(), columns=["srcdir","srcfile"])
    df.insert(0, "src", src)
    df["ext"] = src.apply(lambda x: splitext(x)[-1])
    del src
    df["dst"] = df.src.apply(getdst)
    df.drop_duplicates("dst", keep="last",inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    n_out = len(outdir.split(sep))
    df["zipidx"] = df.dst.apply(lambda x: splitext(sep.join(x.split(sep)[:n_out+2]))[0])

    notzip_e = [".pyd",".dll",".pth",".pem",".svg",".json"]
    notzip_d = ["dlls", "lib2to3", "pyinstaller", "ipython", "ipy", "jedi", "parso", "spyder", "jinja2", "astroid", "notebook", "jupyter", "chartpy"]

    df["zipok"] = df.srcdir.str.startswith(pathjoin(inpdir, "Lib")) & ~(df.srcdir.str.lower().str.contains("|".join(notzip_d)) | df.ext.str.lower().isin(notzip_e))

    ch_zip = df.groupby(by="zipidx").zipok.all()
    zipok = ch_zip[ch_zip == True]
    df["zipok"] = df.zipidx.isin(zipok.index)
    df.loc[df.zipok == False, "zipidx"] = np.nan
    df.loc[df.zipok, "zipdir"] = df.dst.apply(lambda x: sep.join(dirname(x).split(sep)[:n_out+2]) if dirname(x) != libdir else np.nan)
    df["python36zip"] = df.zipok
    
    nanoexclude = ["\\jupyter", "\\pyqt", "\\spyder", "\\notebook\\", "\\qtawesome\\", "\\qtconsole\\", "\\qtpy\\", "\\nuitka"]
    df["nano"] = ~df.dst.apply(lambda x: any(ne in x.lower() for ne in nanoexclude))
    return df

def mkdirs(path):
    return pathlib.Path(path).mkdir(parents=True, exist_ok=True)

def buildpython(df, outzipname):
    print("Build `{}`".format(basename(outzipname)))
    for i, row in df[["src", "dst"]][~df.zipok].iterrows():
        mkdirs(dirname(row.dst))
        shutil.copy(row.src, row.dst)
    
    os.chdir(libdir)
    pypth = pathjoin(outdir, "python36._pth")
    mode = pathexists(pypth) and "a" or "w"

    with open(pypth, mode) as pth:
        if mode == "w":
            pth.write(".\npython36.zip\nLib\nDLLs\nimport site\nsite.main()\n\n")
        for zi in df.dst[df.ext == ".pth"]:
            with open(zi) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("import ") or line.startswith("#"):
                        continue
                    abp = abspath(line)
                    if line and libdir in abp:
                        pth.write("\n" + abp.replace(outdir + sep,""))
    
    with ZipFile(pathjoin(outdir, "python36.zip"), mode, ZIP_DEFLATED) as zf:
        for i, row in df.loc[df.python36zip, ["src", "dst"]].iterrows():
            zf.write(row.src, row.dst.replace(libdir + sep, ""))
        
        hasinit = df.groupby("zipdir").apply(lambda x: x.dst.isin(x.zipdir+"\\__init__.pyc").any())
        for x in hasinit[~hasinit].index:
            zf.writestr(pathjoin(x , "__init__.py").replace(libdir + sep, ""),"")
    
    dn = dirname(outzipname)
    fn,ext = splitext(basename(outzipname))
    shutil.make_archive(pathjoin(dn,fn), ext[1:], dirname(outdir), "python")

def buildother(inproot=inproot, outroot=outroot):
    print("Build `{}`".format(inproot))
    root = re.sub(":$", r":\\", inproot)
    for src in all_exept_python(root):
        dst = src.replace(inproot, outroot)
        mkdirs(dirname(dst))
        shutil.copy(src, dst)


def main(archivedir=archivedir):
    shutil.rmtree(outroot, True)    
    mkdirs(outroot)
    
    buildother(inproot=inproot, outroot=outroot)
    
    df = maketable()
    
    buildpython(df[df.nano == True], pathjoin(archivedir, "python_nano64.zip"))
    shutil.make_archive(pathjoin(archivedir,"PortableApp15_nano"), "zip", outroot)
#    
#    buildpython(df[df.nano == False], pathjoin(archivedir, "python_min64.zip"))
#    shutil.make_archive(pathjoin(archivedir,"PortableApp15_min"), "zip", outroot)

main()



