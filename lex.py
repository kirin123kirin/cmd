#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#-------------------------------------------------------------------------------
# lexer.py
#
# A generic regex-based Lexer/tokenizer tool.
# See the if __main__ section in the bottom for an example.
#
# Eli Bendersky (eliben@gmail.com)
# This code is in the public domain
# Last modified: August 2010
#-------------------------------------------------------------------------------
import re
from functools import lru_cache

__all__ = [
    "lexer",
    "Lexer",
    "LexerError",
    "RULES",
]

day = "\s*(?:[12][0-9]|3[01]|0?[1-9])\s*?(?:th\s*)?[日\-/\.,]?"
month = "\s*(?:(?:1[0-2]|0?[1-9])\s*?[月\-/\.,]?|(?:Jan(?:uary|\.)?|Feb(?:ruary|\.)?|Mar(?:ch|\.)?|Apr(?:il|\.)?|May\.?|Jun[e|\.]?|Jul[y|\.]?|Aug(?:ust|\.)?|Sep(?:tember|\.)?|Oct(?:ober|\.)?|Nov(?:ember|\.)?|Dec(?:ember|\.)?)\s*)"
year = "\s*(?:\d{4}|[\']?\d{2})\s*[年\-/\.,]?"
gengo = "\s*(?:[ABDEHIKM-PR-UWYZ]\.?|[万久乾享仁令保元和嘉大天安宝寛寿平康延建弘徳応慶承文斉昌明昭暦朱正永治白神至興観貞長霊養][万中久亀亨享仁保元化吉同和喜嘉国安宝寛寿平延弘徳応慶成承授政文明暦正武永治泰祚祥禄禎福老衡観護貞銅長雉雲養鳥][勝宝感景神]?[字宝護雲]?)\s*?(?:\d{1,2}|元)[年\-/\.,]?"
hour="\s*(?:1[0-9]|2[0-4]||0?[0-9])\s*?[\.:時]?"
minute="\s*(?:[1-5][0-9]|0?[0-9])\s*?[\.:分]?"
sec="\s*(?:[1-5][0-9]|0?[0-9])\s*?(?:秒|[Ss]ec(?:onds)?)?"
milsec="\s*(?:[,\.]?\d+)"
timezone = "\s*(?:[+\-]\d{4})"
timediff = "\s*\(?(?:[ABCDEFGHIJKLMNOPRSTUVWY][ABCDEFGHIJKLMNOPRSTUVWXYZ][ABCDGHKLMNORSTUVWZ][1DST][T])\)?"
tzinfo = f"(?:{timezone}{timediff}|{timezone}|{timediff})"
weekday = "\s*(?:\(?[日月火水木金土](?:曜日)?\)?|(?:Sun|Mon|Tues?|Wed(?:nes)?|Thu(?:rs)?|Fri|Sat(?:ur))(?:day)?[\.,]?)"
ampm = "\s*(?:[AaPp]\.?[Mm]\.?|午[前後])"
MONTHDAY = f"(?:{month}{day}|{day}{month})"
WDATE = f"{weekday}?(?:{year}?{MONTHDAY}|{MONTHDAY}{year})"
JDATE = f"(?:{year}|{gengo}){month}{day}{weekday}?"
_time = f"(?:{hour}{minute}{sec}?{milsec}?{tzinfo}?)"
TIME = f"(?:{ampm}?{hour}{minute}{sec}?{milsec}?{tzinfo}?{ampm}?)"
DATE = f"(?:{JDATE}|{WDATE})"
DATETIME = f"{DATE}(?:\s+|[T:]){TIME}"

RULES = [
    ('\d+',             'NUMBER'),
    ('[a-zA-Z_]\w+',    'IDENTIFIER'),
    ('\+',              'PLUS'),
    ('\-',              'MINUS'),
    ('\*',              'MULTIPLY'),
    ('\/',              'DIVIDE'),
    ('\(',              'LP'),
    ('\)',              'RP'),
    (',',               'COM'),
    ('=',               'EQUALS'),
    (DATE,         'DATE'),
    (TIME,         'TIME'),
    (DATETIME,  'DATETIME')
]

class Token(object):
    """ A simple Token structure.
        Contains the token type, value and position.
    """
    def __init__(self, type, val, pos):
        self.type = type
        self.val = val
        self.pos = pos

    def __str__(self):
        return '%s(%s) at %s' % (self.type, self.val, self.pos)


class LexerError(Exception):
    """ Lexer error exception.

        pos:
            Position in the input line where the error occurred.
    """
    def __init__(self, pos):
        self.pos = pos

recompile=lru_cache(maxsize=2)(re.compile)

class Lexer(object):
    """ A simple regex-based lexer/tokenizer.

        See below for an example of usage.
    """
    def __init__(self, rules, skip_whitespace=True):
        """ Create a lexer.

            rules:
                A list of rules. Each rule is a `regex, type`
                pair, where `regex` is the regular expression used
                to recognize the token and `type` is the type
                of the token to return when it's recognized.

            skip_whitespace:
                If True, whitespace (\s+) will be skipped and not
                reported by the lexer. Otherwise, you have to
                specify your rules for whitespace, or it will be
                flagged as an error.
        """
        # All the regexes are concatenated into a single one
        # with named groups. Since the group names must be valid
        # Python identifiers, but the token types used by the
        # user are arbitrary strings, we auto-generate the group
        # names and map them to token types.
        #
        idx = 1
        regex_parts = []
        self.group_type = {}

        for regex, type in rules:
            groupname = 'GROUP%s' % idx
            regex_parts.append('(?P<%s>%s)' % (groupname, regex))
            self.group_type[groupname] = type
            idx += 1

        self.regex = recompile('|'.join(regex_parts))
        self.skip_whitespace = skip_whitespace
        self.re_ws_skip = recompile('\S')

    def input(self, buf):
        """ Initialize the lexer with a buffer as input.
        """
        self.buf = buf
        self.pos = 0

    def token(self):
        """ Return the next token (a Token object) found in the
            input buffer. None is returned if the end of the
            buffer was reached.
            In case of a lexing error (the current chunk of the
            buffer matches no rule), a LexerError is raised with
            the position of the error.
        """
        if self.pos >= len(self.buf):
            return None
        else:
            if self.skip_whitespace:
                m = self.re_ws_skip.search(self.buf, self.pos)

                if m:
                    self.pos = m.start()
                else:
                    return None

            m = self.regex.match(self.buf, self.pos)
            if m:
                groupname = m.lastgroup
                tok_type = self.group_type[groupname]
                tok = Token(tok_type, m.group(groupname), self.pos)
                self.pos = m.end()
                return tok

            # if we're here, no rule matched
            raise LexerError(self.pos)

    def tokens(self):
        """ Returns an iterator to the tokens found in the buffer.
        """
        while 1:
            tok = self.token()
            if tok is None: break
            yield tok

def lexer(text, rules=RULES, skip_whitespace=True):
    rules.sort(key=lambda x: len(x[0]), reverse=True)
    lx = Lexer(rules, skip_whitespace=skip_whitespace)
    lx.input(text)
    for tok in lx.tokens():
        yield tok.type, tok.type(tok.val) if callable(tok.type) else tok.val

def test():
    from datetime import datetime

    x = " Oct 8 2020 13:11"
    y=",".join("xyz".split() * 2) + " "
    z="2021-01-10 13:50"

    for r in lexer(x+y+z):
        print(r)

    for r in lexer("2021-1-10 10:20 p.m."):
        print(r)

    for r in lexer("午後10:20"):
        print(r)

    for r in lexer("20:02"):
        print(r)

    for r in lexer("2019年10月1日"):
        print(r)

if __name__ == '__main__':
    test()
