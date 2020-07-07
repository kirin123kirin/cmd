import os, sys
from glob import glob
from setuptools import setup, Extension
from Cython.Build import cythonize
from Cython.Distutils import build_ext
from shutil import which

readme="""
"""

pkg = "util"

def read_requirements(path):
    ret = dict(install_requires=[], dependency_links=[])

    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                line = line.strip()
                if "://" in line:
                    ret["dependency_links"].append(line)
                else:
                    ret["install_requires"].append(line)

    return ret

def cancompile():
    if os.name == "posix":
        if which("gcc"):
            return True
        else:
            return False

    elif which("cl.exe") or list(glob("C:/Program Files*/Microsoft Visual Studio/**/VC/Tools/MSVC/**/bin/**/cl.exe", recursive=True)):
        return True
    return False


filepath = os.path.join(os.path.dirname(sys.argv[0]), "requirements.txt")

setup(
    name=pkg,
    version="0.1.6",

    **read_requirements(filepath),

    description='useful high level interface',
    long_description=readme,

    author="m.yama",

    license="MIT License",

    zip_safe=False,

    python_requires='>=3.6.3',
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],

    package_dir={pkg: ''},
    packages=[pkg],

    package_data={ pkg : list(glob("libs/*.xz" if cancompile() else "libs/*")) },

    ext_modules=cythonize([Extension(pkg + ".libs.similar", sources=["libs/similar.pyx"])]) if cancompile() else [],
    cmdclass={'build_ext': build_ext},

    entry_points={
        "console_scripts":[
            "differ = util.differ:main",
            "dumper = util.io:main_row",
            "getinfo = util.io:main_info",
            "getsize = util.io:main_size",
            "lsdirf = util.lsdir:main", #TODO lsdir
            "sankey = util.sankey:main",
            "profiler = util.profiler:main",
            "nwd = util.nwd:main",
            "nw = util.nw:main",
            "netd = util.netd:main",
            "lslog = util.lslog:main",
            "locate = util.io:readrow.locate",
            "maildump = util.mail:main",
            "findexec = util.findexec:main",
            "rmdotdir = util.findexec:main_rmdotdir",
            "rmdotfile = util.findexec:main_rmdotfile",
            "rmgit = util.findexec:main_rmgit",
            "rmsvn = util.findexec:main_rmsvn",
        ],
    }
)