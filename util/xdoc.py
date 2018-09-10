#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""
from collections import namedtuple
from io import StringIO
import sys

#3rd party
try:
    from pptx import Presentation
except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install python-pptx\n")
    Presentation = ImportError

try:
    from docx import Document
except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install python-docx\n")
    Document = ImportError

try:
    import xlrd
except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install xlrd\n")
    xlrd = ImportError

try:
    from pdfminer.pdfinterp import PDFResourceManager, process_pdf
    from pdfminer.converter import TextConverter
    from pdfminer.layout import LAParams
except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install xlrd\n")
    PDFResourceManager = ImportError
    process_pdf = ImportError
    TextConverter = ImportError
    LAParams = ImportError

#my library
from util.core import Path


pinfo = namedtuple("OfficeDoc", ["path", "target", "value"])
def pptx(ppt_path_or_buffer):
    path = Path(ppt_path_or_buffer)

    for i, s in enumerate(Presentation(path).slides):
        for sp in s.shapes:

            if not sp.has_text_frame:
                continue

            for para in sp.text_frame.paragraphs:
                for run in para.runs:
                    txt = run.text
                    if txt:
                        yield pinfo(path, i, txt)

def docx(docx_path_or_buffer):
    path = Path(docx_path_or_buffer)
    doc = Document(path)

    for para in doc.paragraphs:
        txt = para.text
        if txt:
            yield pinfo(path, "#TODO", txt)

def xlsx(xlsx_path_or_buffer):
    path = Path(xlsx_path_or_buffer)
    with xlrd.open_workbook(path) as wb:
        for sheet in wb.sheets():
            nr = sheet.nrows
            if nr > 0:
                for i in range(nr):
                    yield pinfo(path, "{}:{}".format(sheet.name, i+1), sheet.row_values(i))

def pdf(pdf_path_or_buffer):
    path = Path(pdf_path_or_buffer)

    caching=True
    rsrcmgr = PDFResourceManager(caching=caching)
    pagenos = set()

    with StringIO() as outfp:
        device = TextConverter(rsrcmgr, outfp, laparams=LAParams())
        with path.open('rb') as fp:
            process_pdf(rsrcmgr, device, fp, pagenos, maxpages=0, password="",
                        caching=caching, check_extractable=True)

        device.close()

        yield pinfo(path, "#TODO", outfp.getvalue())

def txt(path_or_buffer):
    path = Path(path_or_buffer)
    with path.open() as r:
        for i, line in enumerate(r, 1):
            yield path, i, line.rstrip()

def test():
    from util.core import tdir

    def test_pptx():
        f = tdir+"test.pptx"
        for x in pptx(f):
            assert(type(x) == pinfo)

    def test_docx():
        f = tdir+"test.docx"
        for x in docx(f):
            assert(type(x) == pinfo)

    def test_xlsx():
        f = tdir+'diff1.xlsx'
        for x in xlsx(f):
            assert(type(x) == pinfo)

    def test_pdf():
        f = tdir + "test.pdf"
        for x in pdf(f):
            assert(type(x) == pinfo)

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()

if __name__ == "__main__":
    test()
