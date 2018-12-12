#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from os.path import dirname, basename, join as pathjoin, abspath, sep, splitext, split as pathsplit, exists as pathexists
import shutil
import os, sys
import re
import pathlib
from glob import iglob
import pandas as pd
import numpy as np
from subprocess import check_call
import platform
from itertools import chain

inpdir = os.getenv("PYTHONPATH")
inproot,pythondirs = os.path.splitdrive(inpdir)

outroot = r"C:\temp\zipdata"
outdir = outroot + pythondirs

libdir = pathjoin(outdir, "Lib")

archivedir=r"\\FREENAS\data1\Downloads"

pyver = "{}{}".format(*sys.version_info[:2])

def jppatch():
    #zipfile.py : cp437 -> cp932
    zsrc = pathjoin(inpdir, "Lib", "zipfile.py")
    with open(zsrc, "rb") as r:
        txt = r.read()
    if b"cp437" in txt:
        shutil.copy(zsrc, zsrc+".org")
        with open(zsrc, "wb") as w:
            w.write(txt.replace(b"cp437", b"cp932"))
jppatch()

from zipfile import ZipFile, ZIP_DEFLATED

def lsdir(path):
    dirinclude = ["\\Tools\\", "\\Scripts\\", "\\distutils\\", "\\setuptools\\"]
    extexclude = ['.c', '.cpp', '.cs', '.csc', '.csh', '.h', '.java', '.lock', '.py', '.sample', '.pyx', 'Thumb.db', '.chm', "desktop.ini", ".shp"]
    direxclude = [".git", ".svn", "\\demos\\","\\demo\\", "\\Demos\\", ".dist-info", ".egg-info", "\\tests\\", "\\example"]
    for pth in iglob(path):
        for root, dirs, files in os.walk(pth):
            if re.search("(PyInstaller|altgraph)-[0-9.]+.dist-info", root):
                pass
            elif any(x in root for x in direxclude):
                continue
            for file in files:
                if any(x in root for x in dirinclude):
                    yield pathjoin(root, file)
                elif file in extexclude or splitext(file)[-1] in extexclude:
                    continue
                elif file.endswith(".pyc") and ".opt-" in file:
                    continue
                else:
                    yield pathjoin(root, file)

def all_exept_python(path):
    for pth in iglob(path):
        for root, dirs, files in os.walk(pth):
            if pythondirs in root or ".git" in root or ".svn" in root:
                continue
            for file in files:
                yield os.path.join(root, file)

ENV = os.environ['PATH'].split(os.pathsep)
EXEC = [".exe", ".bat", ".cmd", ".wsh", ".vbs"]
def which(executable):
    for path in ENV:
        path = path.strip('"')

        fpath = os.path.join(path, executable)

        if os.path.isfile(fpath) and os.access(fpath, os.X_OK):
            return fpath

    if os.name == 'nt':
        if splitext(executable)[-1]:
            return None
        else:
            for ext in EXEC:
                ret = which(executable + ext)
                if ret:
                    return ret

    return None


MINIFY = which("minify.exe") + " -a {} -o {}"
def minify(inputpath, outputpath=None):
    if outputpath is None:
        prf, suf = splitext(inputpath)
        outputpath = "{}.min{}".format(prf, suf)
    try:
        return check_call(MINIFY.format(inputpath, outputpath), shell=True)
    except:
        return shutil.copy(inputpath, outputpath)

UPX = which("upx.exe") + " --best "
notupx = re.compile(r"(constants.+\.pyd|_cytest.+\.pyd|indexing.+\.pyd|\\utils.+\.pyd|\\vcamp.+dll|\\wininst.+.exe|\\libgcc.+\.dll|\\qwindows\.dll|\\platforms\\.*\.dll|PyQt5\\.*\.dll|tk.*.dll|\\PyInstaller\\|\\pythonwin\\|\\win32\\|\\pywin32_system32\\|\\gui(?:-[36][24])?\.exe)")
def upx(inputpath, outputpath=None):
#    ext = os.path.splitext(inputpath)[-1].lower()
#    if ext in [".exe", ".dll", ".pyd"] and not notupx.search(inputpath):
#        cmd = UPX + inputpath
#        if outputpath is not None:
#            cmd += " -o " + outputpath
#        try:
#            return check_call(cmd, shell=True)
#        except:
#            pass
    if outputpath is not None:
        return shutil.copy(inputpath, outputpath)

def getdst(src):
    dst = src.replace(inpdir, outdir)
    if dst.lower().endswith(".pyc"):
        dst = dst.replace(".cpython-{}.pyc".format(pyver),"") + ".pyc"
        dst = dst.replace(".pyc.pyc", ".pyc")
    dst = dst.replace("\__pycache__","")
#    dst = dst.replace(r"\site-packages","")
    dst = dst.replace(".min.",".")
    return dst.replace(r"\Lib\pywin32_system32","")

def maketable(inpdir=inpdir, outdir=outdir):
    src = pd.Series(lsdir(inpdir))
    src = src[~src.isin(src[src.apply(lambda x: ".min." in x)].str.replace(".min.","."))]
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
    df["python_zip"] = df.zipok

#    nanoexclude = ["\\jupyter", "\\pyqt", "\\spyder", "\\notebook\\", "\\qtawesome\\", "\\qtconsole\\", "\\qtpy\\", "\\nuitka"]
    nanoexclude = ["\\pyqt", "\\spyder", "\\qtawesome\\", "\\nuitka"]
    df["nano"] = ~df.dst.apply(lambda x: any(ne in x.lower() for ne in nanoexclude))
    return df

def mkdirs(path):
    return pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def buildpython(df, outzipname):
    print("Build `{}`".format(basename(outzipname)))
    for i, row in df[["src", "dst","ext"]][~df.zipok].iterrows():
        mkdirs(dirname(row.dst))
        if row.ext in [".css", ".htm", ".html", ".js", ".json", ".map"] and ".min." not in row.src:
            minify(row.src, row.dst)
        elif row.ext in [".exe", ".dll", ".pyd"]:
            upx(row.src, row.dst)
        else:
            shutil.copy(row.src, row.dst)

    os.chdir(libdir)
    pypth = pathjoin(outdir, "python{}._pth".format(pyver))
    mode = pathexists(pypth) and "a" or "w"

    with open(pypth, mode) as pth:
        if mode == "w":
            pth.write(".\npython{0}.zip\npython{0}.zip\\site-packages\nLib\nLib\\site-packages\nDLLs\nimport site\nsite.main()\n\n".format(pyver))
        for zi in df.dst[df.ext == ".pth"]:
            with open(zi) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("import ") or line.startswith("#"):
                        continue
                    abp = abspath(line)
                    if line and libdir in abp:
                        pth.write("\n" + abp.replace(outdir + sep,""))

    with ZipFile(pathjoin(outdir, "python{}.zip".format(pyver)), mode, ZIP_DEFLATED) as zf:
        for i, row in df.loc[df.python_zip, ["src", "dst"]].iterrows():
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

def dotnetdllcopy(outdir=outdir):
    if platform.architecture()[0] == "64bit":
        r = iglob(r"C:\Windows\SysWOW64\vc*140.dll")
    else:
        r = iglob(r"C:\Windows\System32\vc*140.dll")
    api = r"C:\Program Files\dotnet\shared\Microsoft.NETCore.App"
    for x in chain(iglob(pathjoin(api,os.listdir(api)[-1],"api-ms-win*")),[pathjoin(api,os.listdir(api)[-1],"ucrtbase.dll")], r):
        shutil.copy(x, pathjoin(outdir,"DLLs"))


def main(archivedir=archivedir):
    shutil.rmtree(outroot, True)
    mkdirs(outroot)

    buildother(inproot=inproot, outroot=outroot)

    df = maketable()

    buildpython(df[df.nano == True], pathjoin(archivedir, "python{}_nano64.zip".format(pyver)))
#    dotnetdllcopy(outdir)
    shutil.make_archive(pathjoin(archivedir,"PortableApp18_nano"), "zip", outroot)

    buildpython(df[df.nano == False], pathjoin(archivedir, "python{}_min64.zip".format(pyver)))
    shutil.make_archive(pathjoin(archivedir,"PortableApp18_min"), "zip", outroot)

if __name__ == "__main__":
    main()
