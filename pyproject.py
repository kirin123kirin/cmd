# -*- coding: utf-8 -*-
import sys, os, stat

usage = """

usage: {0} プロジェクト名
  -> テンプレートがカレントディレクトリに作成される

""".format(os.path.basename(__file__)[:-3])

if len(sys.argv) != 2:
    sys.stderr.write(usage)
    sys.exit(1)

wdir = os.path.abspath(os.getcwd())

pjname = sys.argv[1]

os.mkdir(os.path.join(wdir, pjname))
os.mkdir(os.path.join(wdir, pjname, "bin"))
os.mkdir(os.path.join(wdir, pjname, "lib"))

requirementstxt = """cython
pyinstaller
"""
with open(os.path.join(wdir, pjname, "requirements.txt"), "w") as f:
    f.write(requirementstxt)

setuppy = """#!/usr/bin/env python

from glob import glob
from setuptools import setup
from Cython.Build import cythonize

setup(
    name="{}",
    scripts=glob("bin/*"),
    ext_modules=cythonize("lib/*.pyx")
)
""".format(pjname)
with open(os.path.join(wdir, pjname, "setup.py"), "w") as f:
    f.write(setuppy)


buildscript = """
pip install -U -r requirements.txt
python setup.py develop
pyinstaller -F -y ./bin/{}.py

""".format(pjname)

if sys.platform == 'win32':
    bsf = os.path.join(wdir, pjname, "build.bat")
    with open(bsf, "w") as f:
        f.write("@echo off" + buildscript)
else:
    bsf = os.path.join(wdir, pjname, "build.sh")
    with open(bsf, "w") as f:
        f.write("#!/bin/sh" + buildscript)
    st = os.stat(bsf)
    os.chmod(bsf, st.st_mode | stat.S_IEXEC)


mainscript = """# -*- coding: utf-8 -*-
"""
with open(os.path.join(wdir, pjname, "bin", pjname + ".py"), "w") as f:
    f.write(mainscript)
