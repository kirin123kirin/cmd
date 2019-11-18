import os, sys
from glob import glob
from setuptools import setup, Extension
from Cython.Distutils import build_ext


readme="""
"""

pkg = "util"

def read_requirements():
    r = os.path.join(os.path.dirname(sys.argv[0]), "requirements.txt")
    if os.path.exists(r):
        with open(r) as f:
            return [line.rstrip() for line in f]
    else:
        return []

cython_module = [
    Extension("util.libs.similar", sources=["libs/similar.pyx"])
]

setup(
    name=pkg,
    version="0.1.2",

    install_requires=read_requirements(),

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

    package_data={ pkg : list(glob("libs/*.xz")) },

    ext_modules=cython_module,
    cmdclass={'build_ext': build_ext},

    entry_points={
        "console_scripts":[
            "differ = util.differ:main",
            "dumper = util.io:main_row",
            "getinfo = util.io:main_info",
            "getsize = util.io:main_size",
            "lsdirf = util.io:main", #TODO lsdir
            "sankey = util.sankey:main",
            "profiler = util.profiler:main",
            "nwd = util.nwd:main",
            "nw = util.nw:main",
            "netd = util.netd:main",
            "lslog = util.lslog:main",
            "locate = util.locate:main",

        ],
    }
)