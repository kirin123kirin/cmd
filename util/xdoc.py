#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""
from collections import namedtuple
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

def pdf(path_or_buffer):
    path = Path(path_or_buffer)

    with path.open('rb') as fp:
        parser = PDFParser(fp)
        doc = PDFDocument()
        parser.set_document(doc)
        doc.set_parser(parser)
        doc.initialize('')

        rsrcmgr = PDFResourceManager(caching=True)
        device = PDFPageAggregator(rsrcmgr, laparams=LAParams())
        interpreter = PDFPageInterpreter(rsrcmgr, device)

        for i, page in enumerate(doc.get_pages(), 1):
            interpreter.process_page(page)
            text = "".join(x.get_text() for x in device.get_result() if isinstance(x, (LTTextBox, LTTextLine)))
            yield pinfo(path, i, text.rstrip("{}\n".format(i)))

        device.close()

def txt(path_or_buffer):
    path = Path(path_or_buffer)
    with path.open() as r:
        for i, line in enumerate(r, 1):
            yield pinfo(path, i, line.rstrip())


_handler = {
    ".ppt" : pptx,
    ".doc" : docx,
    ".xls" : xlsx,
    ".pdf" : pdf,
    ".txt" : txt,
}
def any(path_or_buffer):
    path = Path(path_or_buffer)
    ext = path.ext.lower()[:4]
    func = _handler.get(ext, txt)
    return func(path_or_buffer)
    

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

    def test_txt():
        f = tdir + "test.csv"
        for x in txt(f):
            assert(type(x) == pinfo)

    def test_any():
        f = tdir+"test.pptx"
        for x in any(f):
            assert(type(x) == pinfo)

        f = tdir+"test.docx"
        for x in any(f):
            assert(type(x) == pinfo)

        f = tdir+'diff1.xlsx'
        for x in any(f):
            assert(type(x) == pinfo)
            
        f = tdir + "test.pdf"
        for x in any(f):
            assert(type(x) == pinfo)

        f = tdir + "test.csv"
        for x in any(f):
            assert(type(x) == pinfo)
    
    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()

if __name__ == "__main__":
    test()
