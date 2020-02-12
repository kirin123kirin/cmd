#!/usr/bin/python
# -*- coding: utf-8 -*-

IMAP_SERVER = ""
PORT="993"
USERNAME = ""
PASSWORD = ""
THREAD = 10


__LICENSE__ = """
                                 Apache License
                           Version 2.0, January 2004
                        http://www.apache.org/licenses/

   TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION

   1. Definitions.

      "License" shall mean the terms and conditions for use, reproduction,
      and distribution as defined by Sections 1 through 9 of this document.

      "Licensor" shall mean the copyright owner or entity authorized by
      the copyright owner that is granting the License.

      "Legal Entity" shall mean the union of the acting entity and all
      other entities that control, are controlled by, or are under common
      control with that entity. For the purposes of this definition,
      "control" means (i) the power, direct or indirect, to cause the
      direction or management of such entity, whether by contract or
      otherwise, or (ii) ownership of fifty percent (50%) or more of the
      outstanding shares, or (iii) beneficial ownership of such entity.

      "You" (or "Your") shall mean an individual or Legal Entity
      exercising permissions granted by this License.

      "Source" form shall mean the preferred form for making modifications,
      including but not limited to software source code, documentation
      source, and configuration files.

      "Object" form shall mean any form resulting from mechanical
      transformation or translation of a Source form, including but
      not limited to compiled object code, generated documentation,
      and conversions to other media types.

      "Work" shall mean the work of authorship, whether in Source or
      Object form, made available under the License, as indicated by a
      copyright notice that is included in or attached to the work
      (an example is provided in the Appendix below).

      "Derivative Works" shall mean any work, whether in Source or Object
      form, that is based on (or derived from) the Work and for which the
      editorial revisions, annotations, elaborations, or other modifications
      represent, as a whole, an original work of authorship. For the purposes
      of this License, Derivative Works shall not include works that remain
      separable from, or merely link (or bind by name) to the interfaces of,
      the Work and Derivative Works thereof.

      "Contribution" shall mean any work of authorship, including
      the original version of the Work and any modifications or additions
      to that Work or Derivative Works thereof, that is intentionally
      submitted to Licensor for inclusion in the Work by the copyright owner
      or by an individual or Legal Entity authorized to submit on behalf of
      the copyright owner. For the purposes of this definition, "submitted"
      means any form of electronic, verbal, or written communication sent
      to the Licensor or its representatives, including but not limited to
      communication on electronic mailing lists, source code control systems,
      and issue tracking systems that are managed by, or on behalf of, the
      Licensor for the purpose of discussing and improving the Work, but
      excluding communication that is conspicuously marked or otherwise
      designated in writing by the copyright owner as "Not a Contribution."

      "Contributor" shall mean Licensor and any individual or Legal Entity
      on behalf of whom a Contribution has been received by Licensor and
      subsequently incorporated within the Work.

   2. Grant of Copyright License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      copyright license to reproduce, prepare Derivative Works of,
      publicly display, publicly perform, sublicense, and distribute the
      Work and such Derivative Works in Source or Object form.

   3. Grant of Patent License. Subject to the terms and conditions of
      this License, each Contributor hereby grants to You a perpetual,
      worldwide, non-exclusive, no-charge, royalty-free, irrevocable
      (except as stated in this section) patent license to make, have made,
      use, offer to sell, sell, import, and otherwise transfer the Work,
      where such license applies only to those patent claims licensable
      by such Contributor that are necessarily infringed by their
      Contribution(s) alone or by combination of their Contribution(s)
      with the Work to which such Contribution(s) was submitted. If You
      institute patent litigation against any entity (including a
      cross-claim or counterclaim in a lawsuit) alleging that the Work
      or a Contribution incorporated within the Work constitutes direct
      or contributory patent infringement, then any patent licenses
      granted to You under this License for that Work shall terminate
      as of the date such litigation is filed.

   4. Redistribution. You may reproduce and distribute copies of the
      Work or Derivative Works thereof in any medium, with or without
      modifications, and in Source or Object form, provided that You
      meet the following conditions:

      (a) You must give any other recipients of the Work or
          Derivative Works a copy of this License; and

      (b) You must cause any modified files to carry prominent notices
          stating that You changed the files; and

      (c) You must retain, in the Source form of any Derivative Works
          that You distribute, all copyright, patent, trademark, and
          attribution notices from the Source form of the Work,
          excluding those notices that do not pertain to any part of
          the Derivative Works; and

      (d) If the Work includes a "NOTICE" text file as part of its
          distribution, then any Derivative Works that You distribute must
          include a readable copy of the attribution notices contained
          within such NOTICE file, excluding those notices that do not
          pertain to any part of the Derivative Works, in at least one
          of the following places: within a NOTICE text file distributed
          as part of the Derivative Works; within the Source form or
          documentation, if provided along with the Derivative Works; or,
          within a display generated by the Derivative Works, if and
          wherever such third-party notices normally appear. The contents
          of the NOTICE file are for informational purposes only and
          do not modify the License. You may add Your own attribution
          notices within Derivative Works that You distribute, alongside
          or as an addendum to the NOTICE text from the Work, provided
          that such additional attribution notices cannot be construed
          as modifying the License.

      You may add Your own copyright statement to Your modifications and
      may provide additional or different license terms and conditions
      for use, reproduction, or distribution of Your modifications, or
      for any such Derivative Works as a whole, provided Your use,
      reproduction, and distribution of the Work otherwise complies with
      the conditions stated in this License.

   5. Submission of Contributions. Unless You explicitly state otherwise,
      any Contribution intentionally submitted for inclusion in the Work
      by You to the Licensor shall be under the terms and conditions of
      this License, without any additional terms or conditions.
      Notwithstanding the above, nothing herein shall supersede or modify
      the terms of any separate license agreement you may have executed
      with Licensor regarding such Contributions.

   6. Trademarks. This License does not grant permission to use the trade
      names, trademarks, service marks, or product names of the Licensor,
      except as required for reasonable and customary use in describing the
      origin of the Work and reproducing the content of the NOTICE file.

   7. Disclaimer of Warranty. Unless required by applicable law or
      agreed to in writing, Licensor provides the Work (and each
      Contributor provides its Contributions) on an "AS IS" BASIS,
      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
      implied, including, without limitation, any warranties or conditions
      of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A
      PARTICULAR PURPOSE. You are solely responsible for determining the
      appropriateness of using or redistributing the Work and assume any
      risks associated with Your exercise of permissions under this License.

   8. Limitation of Liability. In no event and under no legal theory,
      whether in tort (including negligence), contract, or otherwise,
      unless required by applicable law (such as deliberate and grossly
      negligent acts) or agreed to in writing, shall any Contributor be
      liable to You for damages, including any direct, indirect, special,
      incidental, or consequential damages of any character arising as a
      result of this License or out of the use or inability to use the
      Work (including but not limited to damages for loss of goodwill,
      work stoppage, computer failure or malfunction, or any and all
      other commercial damages or losses), even if such Contributor
      has been advised of the possibility of such damages.

   9. Accepting Warranty or Additional Liability. While redistributing
      the Work or Derivative Works thereof, You may choose to offer,
      and charge a fee for, acceptance of support, warranty, indemnity,
      or other liability obligations and/or rights consistent with this
      License. However, in accepting such obligations, You may act only
      on Your own behalf and on Your sole responsibility, not on behalf
      of any other Contributor, and only if You agree to indemnify,
      defend, and hold each Contributor harmless for any liability
      incurred by, or claims asserted against, such Contributor by reason
      of your accepting any such warranty or additional liability.

   END OF TERMS AND CONDITIONS

   APPENDIX: How to apply the Apache License to your work.

      To apply the Apache License to your work, attach the following
      boilerplate notice, with the fields enclosed by brackets "[]"
      replaced with your own identifying information. (Don't include
      the brackets!)  The text should be enclosed in the appropriate
      comment syntax for the file format. We also recommend that a
      file or class name and description of purpose be included on the
      same "printed page" as the copyright notice for easier
      identification within third-party archives.

   Copyright 2019 Kaukin Vladimir

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

   Copyright 2019 ikvk
   Inspire : https://github.com/ikvk/imap_tools

"""

import sys
import re
import os
import email
import base64
import imaplib
import inspect
from functools import lru_cache
from email.header import decode_header
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from hashlib import md5


short_month_names = ('Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', "Dec")


def cleaned_uid_set(uid_set: str or [str] or iter) -> str:
    if type(uid_set) is str:
        uid_set = uid_set.split(',')
    if inspect.isgenerator(uid_set) and getattr(uid_set, '__name__', None) == 'fetch':
        uid_set = tuple(msg.uid for msg in uid_set if msg.uid)
    try:
        uid_set_iter = iter(uid_set)
    except TypeError:
        raise ValueError('Wrong uid type: "{}"'.format(type(uid_set)))
    for uid in uid_set_iter:
        if type(uid) is not str:
            raise ValueError('uid "{}" is not string'.format(str(uid)))
        if not uid.strip().isdigit():
            raise ValueError('Wrong uid: "{}"'.format(uid))
    return ','.join((i.strip() for i in uid_set))


class UnexpectedCommandStatusError(Exception):
    """Unexpected status in response"""


def check_command_status(command, command_result, expected='OK'):
    typ, data = command_result[0], command_result[1]
    if typ != expected:
        raise UnexpectedCommandStatusError(
            'Response status for command "{command}" == "{typ}", "{exp}" expected, data: {data}'.format(
                command=command, typ=typ, data=str(data), exp=expected))


def decode_value(value: bytes or str, encoding=None) -> str:
    """Converts value to utf-8 encoding"""
    if isinstance(encoding, str):
        encoding = encoding.lower()
    if isinstance(value, bytes):
        try:
            return value.decode(encoding or 'utf-8', 'ignore')
        except LookupError:  # unknown encoding
            return value.decode('utf-8', 'ignore')
    return value


def parse_email_address(value: str) -> dict:
    address = ''.join(char for char in value if char.isprintable()).strip()
    address = re.sub('[\n\r\t]+', ' ', address)
    result = {'email': '', 'name': '', 'full': address}
    match = re.match('(?P<name>.*)?<(?P<email>.*@.*)>', address, re.UNICODE)
    if match:
        group = match.groupdict()
        result['name'] = group['name'].strip()
        result['email'] = group['email'].strip()
    else:
        result['email' if '@' in address else 'name'] = address
    return result


def parse_email_date(value: str) -> datetime.datetime:
    """Parsing the date described in rfc2822"""
    match = re.search(r'(?P<date>\d{1,2}\s+(' + '|'.join(short_month_names) + r')\s+\d{4})\s+' +
                      r'(?P<time>\d{1,2}:\d{1,2}(:\d{1,2})?)\s*' +
                      r'(?P<zone_sign>[+-])?(?P<zone>\d{4})?', value)
    if match:
        group = match.groupdict()
        day, month, year = group['date'].split()
        time_values = group['time'].split(':')
        zone_sign = int('{}1'.format(group.get('zone_sign') or '+'))
        zone = group['zone']
        return datetime.datetime(
            year=int(year),
            month=short_month_names.index(month) + 1,
            day=int(day),
            hour=int(time_values[0]),
            minute=int(time_values[1]),
            second=int(time_values[2]) if len(time_values) > 2 else 0,
            tzinfo=datetime.timezone(datetime.timedelta(
                hours=int(zone[:2]) * zone_sign,
                minutes=int(zone[2:]) * zone_sign
            )) if zone else None,
        )
    else:
        return datetime.datetime(1900, 1, 1)


def quote(value: str or bytes):
    if isinstance(value, str):
        return '"' + value.replace('\\', '\\\\').replace('"', '\\"') + '"'
    else:
        return b'"' + value.replace(b'\\', b'\\\\').replace(b'"', b'\\"') + b'"'


def pairs_to_dict(items: list) -> dict:
    """Example: ['MESSAGES', '3', 'UIDNEXT', '4'] -> {'MESSAGES': '3', 'UIDNEXT': '4'}"""
    if len(items) % 2 != 0:
        raise ValueError('An even-length array is expected')
    return dict((items[i * 2], items[i * 2 + 1]) for i in range(len(items) // 2))


class MailMessage:
    """The email message"""

    def __init__(self, fetch_data):
        raw_message_data, raw_uid_data, raw_flag_data = self._get_message_data_parts(fetch_data)
        self._raw_uid_data = raw_uid_data
        self._raw_flag_data = raw_flag_data
        self.obj = email.message_from_bytes(raw_message_data)

    @classmethod
    def from_bytes(cls, raw_message_data: bytes):
        """Alternative constructor"""
        return cls([(None, raw_message_data)])

    @staticmethod
    def _get_message_data_parts(fetch_data) -> (bytes, bytes, [bytes]):
        raw_message_data = b''
        raw_uid_data = b''
        raw_flag_data = []
        for fetch_item in fetch_data:
            # flags
            if type(fetch_item) is bytes and imaplib.ParseFlags(fetch_item):
                raw_flag_data.append(fetch_item)
            # data, uid
            if type(fetch_item) is tuple:
                raw_uid_data = fetch_item[0]
                raw_message_data = fetch_item[1]
        return raw_message_data, raw_uid_data, raw_flag_data

    def _parse_addresses(self, value: str) -> (dict,):
        if value not in self.obj:
            return ()
        return tuple(
            parse_email_address(''.join(decode_value(string, charset) for string, charset, in decode_header(address)))
            for address in self.obj[value].split(',')
        )

    @property
    @lru_cache()
    def uid(self) -> str or None:
        uid_match = re.search(r'UID\s+(?P<uid>\d+)', self._raw_uid_data.decode())
        if uid_match:
            return uid_match.group('uid')
        # mail.ru, ms exchange server
        for raw_flag_item in self._raw_flag_data:
            uid_flag_match = re.search(r'(^|\s+)UID\s+(?P<uid>\d+)($|\s+)', raw_flag_item.decode())
            if uid_flag_match:
                return uid_flag_match.group('uid')
        return None

    @property
    @lru_cache()
    def flags(self) -> (str,):
        result = []
        for raw_flag_item in self._raw_flag_data:
            result.extend(imaplib.ParseFlags(raw_flag_item))
        return tuple(i.decode().strip().replace('\\', '').upper() for i in result)

    @property
    @lru_cache()
    def subject(self) -> str:
        """Message subject"""
        if 'subject' in self.obj:
            msg_subject = decode_header(self.obj['subject'])
            return decode_value(msg_subject[0][0], msg_subject[0][1])
        return ''

    @property
    @lru_cache()
    def from_values(self) -> dict or None:
        """Sender (all data)"""
        result_set = self._parse_addresses('from')
        return result_set[0] if result_set else None

    @property
    @lru_cache()
    def from_(self) -> str:
        """Sender email"""
        return self.from_values['email'] if self.from_values else ''

    @property
    @lru_cache()
    def to_values(self) -> (dict,):
        """Recipients (all data)"""
        return self._parse_addresses('to')

    @property
    @lru_cache()
    def to(self) -> (str,):
        """Recipients emails"""
        return tuple(i['email'] for i in self.to_values)

    @property
    @lru_cache()
    def cc_values(self) -> (dict,):
        """Carbon copy (all data)"""
        return self._parse_addresses('cc')

    @property
    @lru_cache()
    def cc(self) -> (str,):
        """Carbon copy emails"""
        return tuple(i['email'] for i in self.cc_values)

    @property
    @lru_cache()
    def bcc_values(self) -> (dict,):
        """Blind carbon copy (all data)"""
        return self._parse_addresses('bcc')

    @property
    @lru_cache()
    def bcc(self) -> (str,):
        """Blind carbon copy emails"""
        return tuple(i['email'] for i in self.bcc_values)

    @property
    @lru_cache()
    def date_str(self) -> str:
        """Message sent date"""
        return str(self.obj['Date'] or '')

    @property
    @lru_cache()
    def date(self):
        return parse_email_date(self.date_str)

    @property
    @lru_cache()
    def text(self) -> str:
        """Plain text of the mail message"""
        for part in self.obj.walk():
            # multipart/* are containers
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get_content_type() in ('text/plain', 'text/'):
                return decode_value(part.get_payload(decode=True), part.get_content_charset())
        return ''

    @property
    @lru_cache()
    def html(self) -> str:
        """HTML text of the mail message"""
        for part in self.obj.walk():
            # multipart/* are containers
            if part.get_content_maintype() == 'multipart':
                continue
            if part.get_content_type() == 'text/html':
                return decode_value(part.get_payload(decode=True), part.get_content_charset())
        return ''

    @property
    @lru_cache()
    def headers(self) -> {str: (str,)}:
        """Message headers"""
        raw_headers = getattr(self.obj, '_headers', ())
        return {key: tuple(v for k, v in raw_headers if k == key) for key in set(i[0] for i in raw_headers)}

    @property
    @lru_cache()
    def attachments(self) -> ['Attachment']:
        results = []
        for part in self.obj.walk():
            if part.get_content_maintype() == 'multipart':
                # multipart/* are just containers
                continue
            if part.get('Content-Disposition') is None:
                continue
            filename = part.get_filename()
            if not filename:
                continue  # this is what happens when Content-Disposition = inline

            results.append(Attachment(part))
        return results


class Attachment:
    """An attachment for a MailMessage"""

    def __init__(self, part):
        self._part = part

    @property
    @lru_cache()
    def filename(self) -> str:
        filename = self._part.get_filename()
        return decode_value(*decode_header(filename)[0])

    @property
    @lru_cache()
    def content_type(self) -> str:
        return self._part.get_content_type()

    @property
    @lru_cache()
    def payload(self) -> bytes:
        payload = self._part.get_payload(decode=True)
        if payload:
            return payload
        # multipart payload, such as .eml (see get_payload)
        multipart_payload = self._part.get_payload()
        if isinstance(multipart_payload, list):
            for payload_item in multipart_payload:
                if hasattr(payload_item, 'as_bytes'):
                    payload_item_bytes = payload_item.as_bytes()
                    cte = str(self._part.get('content-transfer-encoding', '')).lower().strip()
                    if payload_item_bytes and cte:
                        if cte == 'base64':
                            return base64.b64decode(payload_item_bytes)
                        elif cte in ('7bit', '8bit', 'quoted-printable', 'binary'):
                            return payload_item_bytes  # quopri.decodestring
        # could not find payload
        return b''


ngword = ":;/\\?\"<>|\t\n\r\v"
mt = str.maketrans(ngword, "_" * len(ngword))
def mkname(msg):
    name = msg.subject
    mid = msg.headers.get("Message-Id", [""])[0]
    if mid:
        name += "_" + mid.strip("<>").split("@")[0]
    else:
        name = "{}__MD5__{}".format(name or "NONAME", md5(msg.obj.as_bytes()).hexdigest().upper())

    return name.translate(mt).replace("＼", "")


def download(folder, data):
    msg = MailMessage(data)
    fname = os.path.join(folder, mkname(msg))[:256] + ".eml"

    if os.path.exists(fname) and os.path.getsize(fname):
        return

    with open(fname, 'wb') as f:
        f.write(data[0][1])
    ts = msg.date.timestamp()
    os.utime(fname, (ts, ts))


def bkmailbox(server, uid, passwd,
        port = "993", outdir=None,
        include=[], exclude=[],threads=10):

    mail = imaplib.IMAP4_SSL(server, port)
    mail.login(uid, passwd)
    folders = [x.decode().split(' "/" ')[1] for x in mail.list()[1]]
    total = 0

    executor = ThreadPoolExecutor(max_workers = threads)
    futures = []

    outdir = outdir or ("bk_" + server)

    for folder in folders:
        if any(re.match(x.lower(), folder.lower()) for x in exclude):
            continue
        
        if include and not any(re.match(x.lower(), folder.lower()) for x in include):
            continue
        
        mail.select(folder)
        ids = mail.search(None, 'ALL')[1][0].split()

        cnt = len(ids)
        print("{}: total {} emails found.".format(folder, cnt))
        total += cnt

        fd = os.path.join(outdir, folder.strip("'\""))
        if not os.path.exists(fd):
            os.mkdir(fd)

        for i in ids:
            code, data = mail.fetch(i, '(RFC822)')

            if code != "OK":
                raise RuntimeError(f"Error Fetch Message {data}")

            futures.append(executor.submit(download, fd, data))


    for i, r in enumerate(as_completed(futures), 1):
        e = r.exception()
        if e:
            print(str(e.__class__.__name__), file=sys.stderr)
            print(str(e), file=sys.stderr)
        print(f"\t{i} / {total} ({(i/total)*100}%) DONE.")


def main():
    from argparse import ArgumentParser

    usage="""\n  backup mailbox programs\n  Ex: python {0} -u username -p password -s imap.hoge.co.jp ./backupdirectory

    """.format(os.path.basename(sys.argv[0]))

    ps = ArgumentParser(usage)
    padd = ps.add_argument

    padd('-u', '--user', type=str, default=USERNAME,
         help='IMAP username charactor.')
    padd('-p', '--password', type=str, default=PASSWORD,
         help='IMAP password character.')
    padd('-s', '--server', type=str, default=IMAP_SERVER,
         help='IMAP Server Address.')
    padd('--port', type=str, default=PORT,
         help='IMAP Connect port number')
    padd('-m', '--multithreading', type=int, default=THREAD,
         help=f'multithreading num (default {THREAD})')
    padd('-E', '--exclude', nargs="+", default=['Trash'],
         help='Exclude folder Names. (default Trash) regex ok.')
    padd('-O', '--only', nargs="+", default=[],
         help='Only folder Names. regex ok')
    
    padd("outdir",
         metavar="<outdir>",
         nargs=1,
         help="Backup Output Directory Path")

    args = ps.parse_args()
    od = args.outdir[0]

    if not os.path.exists(od):
        raise NotADirectoryError(f"Not Found Directory {od}")

    bkmailbox(
        server = args.server,
        uid = args.user,
        passwd = args.password,
        port = args.port,
        outdir = od,
        include = args.only,
        exclude = args.exclude,
        threads=10
    )

if __name__ == "__main__":
    main()
