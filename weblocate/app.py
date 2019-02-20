#!/usr/bin/env python3
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
import flask
from subprocess import Popen, PIPE
from flask import send_from_directory
from socket import gethostname
from pipes import quote
import re

app = flask.Flask(__name__)

def getitem(obj, item, default):
    if item not in obj:
        return default
    else:
        return obj[item]

re_path = re.compile("(/?.*)")
def geturi(cs, res):
    if "ls -l" in cs or "ll" in cs:
        res = res.split(" ")[-1]
    return res.replace("/home/admin/data/samba/", "//10.145.120.124/")

okcmd = ["locate", "grep", "ls", "ll", "la", "file"]
def denycmd(x):
    return not any(x.strip().startswith(oc) for oc in okcmd)

@app.route('/')
def main():
  return flask.redirect('/index')

@app.route('/index')
def index():
    # handle user args
    args = flask.request.args
    query = getitem(args, 'searchbox', '')
    cs = getitem(args, 'command', 'locate /samba')
    hostname = gethostname()
    if denycmd(cs):
        resultslist = '<h2>Access Deny Command!!!!!</h2>'
    elif query == '':
        resultslist = ''
    else:
        command = cs + ' ' + quote(query) + '| head -1000'
        command = command.encode('utf-8')
        with Popen(command, shell=True, stdout=PIPE) as proc:
            outs = proc.stdout.read()
        results = outs.splitlines()
        resultslist = ""
        for entry in results:
            res = entry.decode("utf-8")
            uri = geturi(cs, res)
            resultslist += '<li><a href="file://{}">{}</a></li>'.format(uri, res)

    html = flask.render_template(
        'index.html',
        resultslist=resultslist,
        hostname=hostname,
        command=command,
        query=query
         )
    return html

@app.route('/css/<path:path>')
def send_css(path):
    return send_from_directory('css', path)

if __name__ == '__main__':
    port = 5000
    app.run(debug=False,host='0.0.0.0',port=port)
