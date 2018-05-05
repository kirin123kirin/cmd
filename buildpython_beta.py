# -*- coding: utf-8 -*-
from os.path import dirname, basename, join as pathjoin, abspath, sep, splitext
import shutil
import os
import re
import pathlib
from zipfile import ZipFile, ZIP_DEFLATED
from glob import iglob
import pandas as pd
import numpy as np

cd = os.getcwd()
inpdir = os.getenv("PYTHONPATH")
outdir = r"C:\temp\build\python"

libdir = pathjoin(outdir, "Lib")

def lsdir(wildcardpath, recursive=False, target="file", dotfileskip=True):
    """ File System Input File Path Exists Files Listing Function
    """
    for pth in iglob(wildcardpath):
        if target in (None, "d", "dir", "dirs"):
            yield pth
        for root, dirs, files in os.walk(pth):
            if target in ("d", "dir", "dirs"):
                lp = dirs
            elif target in ("f", "file", "files"):
                lp = files
            else:
                lp = dirs + files
            
            if dotfileskip is True:
                lp = (x for x in lp if not x.startswith("."))
                
            for file in lp:
                yield os.path.join(root, file)

def ifilter(path):
    extexclude = ['.c', '.cpp', '.cs', '.csc', '.csh', '.h', '.java', '.lock', '.py', '.sample', '.pyx', 'Thumb.db', '.chm', "desktop.ini"]
    direxclude = [".git", ".svn", "\\demos\\","\\demo\\", "\\Demos\\", ".dist-info", ".egg-info", "\\tests\\", "\\example"]
    for pth in lsdir(path ,recursive=True, dotfileskip=False):
        if os.path.isdir(pth) or abspath(path) == abspath(pth):
            continue
        if any(pathjoin(outdir, x) in pth for x in ["\\Tools\\", "\\Scripts\\"]) or re.search("(PyInstaller|altgraph)-[0-9.]+.dist-info", pth):
            yield pth
        if "\\bokeh\\core\\" in pth and pth.endswith("__init__.py"):
            yield pth

        if pth.endswith(".pyc") and ".opt-" in pth:
            continue
        if any(x.lower() in pth for x in direxclude):
            continue
        if any(pth.lower().endswith(x) for x in extexclude):
            continue        
        yield pth

def srcdst(inpdir=inpdir, outdir=outdir):
    for src in ifilter(inpdir):
        dst = src.replace(inpdir, outdir)
        if dst.lower().endswith(".pyc"):
            dst = dst.replace(".cpython-36.pyc","") + ".pyc"
            dst = dst.replace(".pyc.pyc", ".pyc")
        dst = dst.replace("\__pycache__","")
        dst = dst.replace(r"\site-packages","")
        dst = dst.replace(r"\Lib\pypiwin32_system32","")
        yield src, dst

def zipdir(path):
    olen = len(outdir.split(sep))
    return pathjoin(outdir, *path.split(sep)[olen:olen+2])

def is_zipng(x):
    if "\\Lib\\" not in x:
        return True
    if any(pathjoin(libdir, y).lower() in x.lower() for y in ["DLLs", "lib2to3", "pyinstaller", "IPython", "ipy", "jedi", "parso", "spyder", "jinja2", "astroid", "notebook", "jupyter", "chartpy"]):
        return True
    if any(x.lower().endswith(y) for y in [".pyd",".dll",".pth",".pem",".svg",".json"]):
        return True
    return False

def maketable(inpdir=inpdir, outdir=outdir):
    df = pd.DataFrame(data=srcdst(inpdir,outdir), columns=["src","dst"])
    df["zipng"] = df.dst.apply(is_zipng)
    df["zipdir"] = df.dst.apply(zipdir)
    
    for i, gdf in df.groupby(by="zipdir"):
        if gdf.zipng.any():
            df.loc[df.zipdir == i, "zipng"] = True
            df.loc[df.zipdir == i, "zipdir"] = np.nan
            pre, ext = splitext(i)
            if ext == "":
                df.loc[df.dst == pre + ".pyc", "zipng"] = True
                df.loc[df.dst == pre + ".pyc", "zipdir"] = np.nan
    
    df["python36zip"] = df.zipng == False
    
    is_nano = lambda f: all(name not in f.lower() for name in ["\\jupyter", "\\pyqt", "\\spyder", "\\notebook\\", "\\qtawesome\\", "\\qtconsole\\", "\\qtpy\\", "\\nuitka"])
    df["nano"] = df.dst.apply(is_nano)
    ret = df.groupby("dst", as_index=False).last()
    del df
    ret.drop_duplicates(inplace=True)
    return ret


mkdirs = lambda x: pathlib.Path(x).mkdir(parents=True, exist_ok=True)


def buildpython(df, outzipname):
    shutil.rmtree(outdir, True)
    
    mkdirs(outdir)
    
    def check_initfile(path):
        if not path or path is np.nan:
            return True
        if splitext(path)[-1]:
            return True
        if not (df.zipdir == path).any():
            return True
        return df.dst.isin([pathjoin(path,"__init__.pyc")]).any()

    for i, row in df.loc[df.zipng, ["src", "dst"]].iterrows():
        mkdirs(dirname(row.dst))
        shutil.copy(row.src, row.dst)
    
    with open(pathjoin(outdir, "python36._pth"), "w") as pth:
        pth.write(".\npython36.zip\nLib\nDLLs\nimport site\nsite.main()\n\n")
        os.chdir(libdir)
        for zi in df.dst[df.dst.apply(lambda x: x.endswith(".pth"))]:
            with open(zi) as f:
                pl = [line.strip() for line in f.readlines() if line and 
                        not line.startswith("import ") and not line.startswith("#")]
                for p in pl:
                    abp = abspath(p)
                    if p and libdir in abp:
                        pth.write("\n" + abp.replace(outdir+os.path.sep,""))

    
    with ZipFile(pathjoin(outdir, "python36.zip"), 'w', ZIP_DEFLATED) as zf:
        for i, row in df.loc[df.python36zip, ["src", "dst"]].iterrows():
            zf.write(row.src, row.dst.replace(libdir + sep, ""))
        for x in df.zipdir.dropna().unique():
            if check_initfile(x) == False:
                zf.writestr(pathjoin(x , "__init__.py").replace(libdir + sep, ""),"")
    
    dn = dirname(outzipname)
    fn,ext = splitext(basename(outzipname))
    shutil.make_archive(pathjoin(dn,fn), ext[1:], os.path.dirname(outdir), "python")
    
df = maketable()
buildpython(df, r"\\FREENAS\data1\Downloads\\python_min64.zip")
buildpython(df[df.nano == True], r"\\FREENAS\data1\Downloads\\python_nano64.zip")
