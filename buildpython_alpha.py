from os.path import dirname, join as pathjoin, abspath
import shutil
import os
from lsdir import lsdir
import pathlib
from zipfile import ZipFile, ZIP_DEFLATED
from glob import glob

cd = os.getcwd()
inpdir = os.getenv("PYTHONPATH")
outdir = r"C:\temp\build\python"

libdir = pathjoin(outdir, "Lib")

files = set()
nonzip = set([pathjoin(outdir, "DLLs")] + [pathjoin(outdir, "Lib" , x) for x in ["lib2to3", "IPython", "ipy", "jedi", "parso", "spyder", "jinja2", "astroid", "notebook", "jupyter"]])

shutil.rmtree(outdir, True)
mkdirs = lambda x: pathlib.Path(x).mkdir(parents=True, exist_ok=True)

mkdirs(outdir)

def rmrf(f):
    if os.path.isdir(f):
        shutil.rmtree(f, True)
    elif os.path.isfile(f):
        os.remove(f)

def rm_match(name, target="d"):
    for f in lsdir(outdir, recursive=True, target=target):
        if name in f:
            rmrf(f)

with open(pathjoin(dirname(outdir), "build.log"),"w") as log, open(pathjoin(outdir, "python36._pth"), "w") as pth:
    
    pth.write(".\npython36.zip\nLib\nDLLs\nimport site\nsite.main()\n\n")
    
    extexclude = ['.c', '.cpp', '.cs', '.csc', '.csh', '.h', '.java', '.lock', '.py', '.sample', '.pyx', 'Thumb.db', '.chm', "desktop.ini"]
    direxclude = [".git", ".svn", "\\Scripts\\", "\\Tools\\", "\\demos\\","\\demo\\", "\\Demos\\", ".dist-info", ".egg-info", "\\tests\\", "\\example"]
    
    # building file filtering
    for src in lsdir(inpdir, target="f", recursive=True):
        if any(x in src for x in direxclude) or any(src.lower().endswith(x) for x in extexclude):
            print("Skip : " + src, file=log, flush=True)
            continue

        elif src.endswith(".pyc") and "opt-" not in src:
            dst = src.replace(".cpython-36.pyc","") + ".pyc"
            dst = src.replace(".cpython-36.pyc","") + ".pyc"
            dst = dst.replace("\__pycache__","").replace(inpdir, outdir)
            dst = dst.replace(".pyc.pyc", ".pyc")
        else:
            dst = src.replace(inpdir, outdir)

        dst = dst.replace(r"\site-packages","")
        dst = dst.replace(r"\Lib\pypiwin32_system32","")


        for od in [libdir]: #zip Targets
            if od in dst:
                files.add(dst)
                if any(dst.lower().endswith(x) for x in [".pyd",".dll",".pth",".pem","svg"]):
                    z = pathjoin(od, dst.split(od+os.path.sep)[-1].split(os.path.sep)[0])
                    nonzip.add(z)
        
        print("Mkdir : " + dirname(dst), file=log, flush=True)
        mkdirs(dirname(dst))
        print("Copy : " + src + "->" + dst, file=log, flush=True)
        shutil.copy(src, dst)
    
    # making python.exe
    cp = lambda x: shutil.copy(inpdir + x, outdir + x)
    cp(r"\python.exe")
    cp(r"\pythonw.exe")
    
    # copy api-ms-win*
    api = r"C:\Program Files\dotnet\shared\Microsoft.NETCore.App"
    for x in glob(pathjoin(api,os.listdir(api)[-1],"api-ms-win*")):
        shutil.copy(x, pathjoin(outdir,"DLLs"))
    
    # copy Tools, Scripts
    shutil.copytree(inpdir + r"\Tools", outdir + r"\Tools")
    shutil.copytree(inpdir + r"\Scripts", outdir + r"\Scripts")
    
    rm_match("__pycache__")
    
    # import path is exclude
    os.chdir(libdir)
    for zi in files:
        if zi.endswith(".pth"):
            with open(zi) as f:
                pl = [line.strip() for line in f.readlines() if line and 
                        not line.startswith("import ") and not line.startswith("#")]
                for p in pl:
                    abp = abspath(p)
                    if p and libdir in abp:
                        nonzip.add(abp.replace(libdir+os.path.sep,"").split(os.path.sep)[0])
                        pth.write("\n" + abp.replace(outdir+os.path.sep,""))
    
    # exclude from zipfile
    zipinclude = set()
    for zi in files:
        if any(zi.startswith(nz) for nz in nonzip) or "\\__pycache__\\" in zi:
            continue
        zipinclude.add(zi)
    
    
    # zipping python36.zip
    rmdir = set()
    with ZipFile(pathjoin(outdir, "python36.zip"), 'w', ZIP_DEFLATED) as zf:
        pref = libdir
        for f in zipinclude:
            archivepath = f.replace(pref + os.path.sep, "")
            zf.write(f, archivepath)
            print("Zipped : " + dirname(dst), file=log, flush=True)
            rmdir.add(pathjoin(pref, archivepath.split(os.path.sep)[0]))
    
    # remove recursive of ziped files
    for path in rmdir:
        if os.path.isdir(path) and not os.path.islink(path):
            shutil.rmtree(path)
        elif os.path.exists(path):
            os.remove(path)
        print("Removed : " + dirname(dst), file=log, flush=True)

os.chdir(cd)
shutil.make_archive(pathjoin(r"\\FREENAS\data1\Downloads","python_min64"),"zip", os.path.dirname(outdir), "python")

with ZipFile(pathjoin(r"\\FREENAS\data1\Downloads","python_nano64.zip"), 'w', ZIP_DEFLATED) as zf:
    for f in lsdir(pathjoin(outdir), recursive=True):
        if any(name in f.lower() for name in ["jupyter", "pyqt", "spyder", "notebook", "qt", "nuitka"]):
            continue
        zf.write(f, f.replace(outdir+os.path.sep,""))