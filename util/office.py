#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""

__all__ = [
    "reader",
    "readlines",
    "iterlines",
]


from collections import namedtuple
from pathlib import Path
import sys, os

BASE_TYPE = [int, float, str, bytes, bytearray, bool]

def flatten(x):
    if x is None or type(x) in BASE_TYPE:
        return x
    return [z for y in x for z in ([y] if y is None or type(y) in BASE_TYPE else flatten(y))]

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
    from pdfminer.pdfparser import PDFParser, PDFDocument
    from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
    from pdfminer.converter import PDFPageAggregator
    from pdfminer.layout import LAParams, LTTextBox, LTTextLine
except ModuleNotFoundError:
    sys.stderr.write("** No module warning **\nPlease Install command: pip3 install pdfminer3k\n")
    PDFParser = ImportError
    PDFDocument = ImportError
    PDFResourceManager = ImportError
    PDFPageInterpreter = ImportError
    PDFPageAggregator = ImportError
    LAParams = ImportError
    LTTextBox = ImportError
    LTTextLine = ImportError


def splitfileobj(path_or_buffer, mode="rb"):
    try:
        path = Path(path_or_buffer)
        fp = open(path_or_buffer, mode)
    except TypeError:
        path = Path(path_or_buffer.name)
        fp = path_or_buffer
    return path, fp


pinfo = namedtuple("OfficeDoc", ["path", "target", "value"])
def pptx(path_or_buffer):
    path, fp = splitfileobj(path_or_buffer)

    for i, s in enumerate(Presentation(fp).slides):
        for t in (r.text for sp in s.shapes if sp.has_text_frame for p in sp.text_frame.paragraphs for r in p.runs if r.text):
            yield pinfo(path, i, t)

    if not hasattr(path_or_buffer, "close"):
        fp.close()

def docx(path_or_buffer):
    path, fp = splitfileobj(path_or_buffer)

    return (pinfo(path, "#TODO", txt) for txt in (p.text for p in Document(fp).paragraphs if p.text))

    if not hasattr(path_or_buffer, "close"):
        fp.close()

def xlsx(path_or_buffer):
    path, fp = splitfileobj(path_or_buffer)

    with xlrd.open_workbook(file_contents=fp.read()) as wb:
        for i, sh in ((r, sh) for sh in wb.sheets() for r in range(sh.nrows)):
            yield pinfo(path, sh.name, sh.row_values(i))

    if not hasattr(path_or_buffer, "close"):
        fp.close()


def pdf(path_or_buffer):
    path, fp = splitfileobj(path_or_buffer)

    ps = PDFParser(fp)
    doc = PDFDocument()
    ps.set_document(doc)
    doc.set_parser(ps)
    doc.initialize('')

    mgr = PDFResourceManager(caching=True)
    dev = PDFPageAggregator(mgr, laparams=LAParams())
    ip = PDFPageInterpreter(mgr, dev)

    for i, page in enumerate(doc.get_pages(), 1):
        ip.process_page(page)
        text = "".join(x.get_text() for x in dev.get_result() if isinstance(x, (LTTextBox, LTTextLine)))
        yield pinfo(path, i, text.rstrip("{}\n".format(i)))

    dev.close()
    if not hasattr(path_or_buffer, "close"):
        fp.close()


def reader(path_or_buffer):
    if hasattr(path_or_buffer, "name"):
        ext = os.path.splitext(path_or_buffer.name)[1]
    else:
        ext = os.path.splitext(path_or_buffer)[1]

    ext = ext.lower()[:4]

    if ext == ".ppt":
        return pptx(path_or_buffer)
    elif ext == ".doc":
        return docx(path_or_buffer)
    elif ext == ".xls":
        return xlsx(path_or_buffer)
    elif ext == ".pdf":
        return pdf(path_or_buffer)
    else:
        raise ValueError("Unknown office File")


def readlines(path_or_buffer, return_target=True):
    if return_target:
        return [flatten([r.target, r.value]) for r in reader(path_or_buffer)]
    else:
        return [r.value for r in reader(path_or_buffer)]

def iterlines(path_or_buffer, return_target=True):
    if return_target:
        return (flatten([r.target, r.value]) for r in reader(path_or_buffer))
    else:
        return (r.value for r in reader(path_or_buffer))

