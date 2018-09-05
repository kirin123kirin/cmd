#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""

My very usefull tools library
require pandas and dask!!

MIT License

"""
from collections import namedtuple

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

def test():
    from util.core import tdir
    
    def test_pptx():
        ppt = tdir+"test.pptx"
        for x in pptx(ppt):
            print(x)

    def test_docx():
        doc = tdir+"test.docx"
        for x in docx(doc):
            print(x)

    for x, func in list(locals().items()):
        if x.startswith("test_") and callable(func):
            func()

if __name__ == "__main__":
    test()
