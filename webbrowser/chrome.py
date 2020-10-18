#!/usr/bin/env python3
# -*- coding: utf-8 -*-

__license__ = """
Licensed to the Software Freedom Conservancy (SFC) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The SFC licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

__author__  = 'm.yama'
__date__    = 'Sun Oct 11 19:39:02 2020'
__version__ = '0.0.1'

__all__     = [
    "ChromeSync"
]

__doc__ = """
Summary:
    selenuim Chromeは非同期処理として各find_elemnet*の関数は動作するが、
    これを同期処理として動くように拡張した。

Description:
    ref. https://qiita.com/kirin123kirin/items/ec5777e1977bd7414ece

* *1 sleepじゃうまくコントロールできない。標準的なChromeクラスの使い方ではsleepで待ち合わせを入れることが一般的だが、長くsleepし無駄な時間になったり、逆にsleepが早すぎてNoSuchElementExceptionエラーでストップしてしまったり腹立たしい。そもそも何秒待てば必ず処理が終わるのか？なんてわからない。
* *2 ファイルをダウンロードする処理の場合同じファイル名があると自動的に(1)とか連番をつけられ勝手に増えていく。
* *3 オプション設定がだるい。
* *4 find_element_by_xpathとか関数名が長すぎる。Java bindingを元に作られてるのだろうが、名前が冗長すぎるので、xpathしか使わない。
* *5 exeファイル等の実行ファイルをダウンロードする場合、警告が表示され自動的にダウンロードが始まらない
* *6 実行PCによっては'Bluetooth: bluetooth_adapter_winrt.cc:1074 Getting Default Adapter failed'が出てしまう
* *7 ファイルダウンロード途中で進まなくなった場合、タイムアウト設定秒を超えたら中断させダウンロード完了済みのものだけを残したい
* *8 PDFをダウンロードしたいが、Chromeブラウザ中に表示されてしまう。
* *9 新しいタブやウィンドウが開いたときにcurrent_windowが元のタブやウィンドウのままであり、イチイチ最後のタブやウィンドウを見つけてdriver.to_switch.windowをやらないといけない

:REQUIRES:
    selenium

"""

import selenium.webdriver
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    InvalidSessionIdException,
    )

import os
import time
import shutil
from urllib.request import urlretrieve
from glob import glob
from tempfile import TemporaryDirectory
from os.path import isdir, exists, join as pathjoin, basename
from collections import defaultdict

def direct_download(url, download_dir):
    bn = basename(url)
    return urlretrieve(url, filename=pathjoin(download_dir, bn))

class _tmpdir(TemporaryDirectory):
    def __del__(self):
        self.cleanup()

cls = selenium.webdriver.Chrome
class ChromeSync(cls):
    """
    selenium.webdriver.Chromeを同期型クラスに拡張したクラス
    インスタンス初期化後の使用方法は、selenium.webdriver.Chromeと全く同じ

    ChromeDriver のダウンロードは
    http://chromedriver.storage.googleapis.com/index.html

    主な拡張(selenium.webdriver.Chromeとの違い)

    * ページ表示や、ダウンロードが完全に終わるまで待ち合わせる
    * デフォルトダウンロードフォルダの任意変更できる
    * 同名のダウンロードファイルが存在している場合は、上書き保存する
    * ダウンロードファイルを保存しますか？等のユーザー確認プロンプトをスキップし、実行処理の邪魔をさせない
    * find_element...の長い関数名ではなく短縮形の使用ができる
        find_element_by_xpath -> xpath
        find_elements_by_xpath -> xpaths
    * インスタンス作成時に初期アクセスURLを一気に指定できる

    引数パラメータ
    ----------
    init_url : str, optional
        初期表示URL. The default is None.
    executable_path : str, optional (selenium.webdriver.Chrome original parameter)
        path to the executable. If the default is used it assumes the executable is in the $PATH. The default is "chromedriver".
    port : int, optional (selenium.webdriver.Chrome original parameter)
        port you would like the service to run, if left as 0, a free port will be found. The default is 0.
    options : ChromeOption, optional (selenium.webdriver.Chrome original parameter)
        this takes an instance of ChromeOptions. The default is None.
    service_args : TYPE, optional (selenium.webdriver.Chrome original parameter)
        List of args to pass to the driver service. The default is None.
    desired_capabilities : TYPE, optional (selenium.webdriver.Chrome original parameter)
        Dictionary object with non-browser specific. The default is None.
    service_log_path : str, optional (selenium.webdriver.Chrome original parameter)
        Where to log information from the driver. The default is None.
    keep_alive : bool, optional (selenium.webdriver.Chrome original parameter)
        Whether to configure ChromeRemoteConnection to use HTTP keep-alive. The default is True.
    download_dir : str, optional
        ファイルダウンロードするパス. The default is None.
    background : bool, optional
        ブラウザをバックグラウンドで起動するかどうか. The default is False.
    timeout : int, optional
        画面遷移時のタイムアウト秒数. The default is 300.
        0 か 負の値を設定した場合はブラウザエラーが発生しない限りずっと待ち合わせる。
    disable_extensions : bool, optional
        Chrome拡張を無効にするかどうか. The default is True.
    maximized : bool, optional
        ブラウザ画面を最大化するかどうか. The default is False.
    sync : bool, optional
        一つ一つの処理全てを同期処理するかどうか. The default is True.
    proxy_direct : bool, optional
        プロキシ経由せず直接接続するかどうか. The default is True.

    例外
    ------
    FileNotFoundError
        download_dir ディレクトリが、見つからない
    NotADirectoryError
        download_dir がディレクトリでない

    戻値
    -------
    None.

    使用例
    -------
    >>> url = "https://www.google.com"
    >>> with ChromeSync(url, download_dir="C:/temp/hoge",timeout=5) as driver:
            search = driver.xpath('//*[@name="q"]')
            search.send_keys("hoge")
            search.submit()
            time.sleep(3)
    """

    # *4
    xpath = cls.find_element_by_xpath
    xpaths = cls.find_elements_by_xpath
    byid = cls.find_element_by_id
    byids = cls.find_elements_by_id
    link_text = cls.find_element_by_link_text
    link_texts = cls.find_elements_by_link_text
    partial_link_text = cls.find_element_by_partial_link_text
    partial_link_texts = cls.find_elements_by_partial_link_text
    byname = cls.find_element_by_name
    bynames = cls.find_elements_by_name
    tag = cls.find_element_by_tag_name
    tags = cls.find_elements_by_tag_name
    byclass = cls.find_element_by_class_name
    byclasses = cls.find_elements_by_class_name
    cssselector = cls.find_element_by_css_selector
    cssselectors = cls.find_elements_by_css_selector


    def __init__(self, init_url=None, executable_path="chromedriver", port=0,
                 options=None, service_args=None, desired_capabilities=None,
                 service_log_path=None, keep_alive=True,
                 download_dir=None,
                 background=False,
                 timeout=300,
                 disable_extensions=True,
                 maximized=False,
                 sync=True,
                 proxy_direct=True):

        self.init_url = init_url
        self._tmpdir = None
        self.timeout = timeout
        self._cnttime = 0
        self._retry_interval = 0.5
        self.sync = sync
        self.dlhist = defaultdict(list) # *7
        self.options = options or selenium.webdriver.ChromeOptions() # *3
        addarg = self.options.add_argument
        addarg('--ignore-certificate-errors-spki-list')
        addarg('--ignore-certificate-errors')
        addarg('--ignore-ssl-errors')


        if background:
            addarg('--headless')
        if disable_extensions:
            addarg('--disable-extensions')
        if maximized:
            addarg('--start-maximized')

        if proxy_direct:
            addarg('--proxy-server="direct://"')
            addarg('--proxy-bypass-list=*')

        self.prefs = {
            "plugins.plugins_list":
                [{"enabled": False,
                  "name": "Chrome PDF Viewer"}],
            'download.extensions_to_open': '',
            "plugins.always_open_pdf_externally": True, # *8
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled" : True, # *5
        }

        self.download_dir = download_dir

        if self.download_dir:
            if not exists(self.download_dir):
                raise FileNotFoundError("Not Found Directory {}".format(self.download_dir))

            if not isdir(self.download_dir):
                raise NotADirectoryError("Not a Download directory {}".format(self.download_dir))

            self.prefs.update({"download.default_directory": self.tmpdir.name}) # *2

        self.options.add_experimental_option("prefs", self.prefs)
        self.options.add_experimental_option('excludeSwitches', ['enable-logging']) # *6

        super().__init__(
            executable_path=executable_path,
            port=port,
            options=self.options,
            service_args=service_args,
            desired_capabilities=desired_capabilities,
            service_log_path=service_log_path,
            keep_alive=keep_alive,
        )

        if timeout > 0:
            self.set_page_load_timeout(timeout + 1) # *1
            self.set_script_timeout(timeout + 1) # *1

        if self.init_url:
            self.get(self.init_url)


    @property
    def tmpdir(self):
        """
        一時ダウンロードディレクトリの取得
        """
        if self._tmpdir is None:
            self._tmpdir = _tmpdir()
        return self._tmpdir


    def pagetop(self):
        """
        画面先頭にスクロールする
        """
        self.execute_script("window.scrollTo(0, 0);")

    def pageend(self):
        """
        画面末尾にスクロールする
        """
        self.execute_script("window.scrollTo(0, document.body.scrollHeight);")

    def select_tab(self, n):
        """
        n番目のタブやWindowに制御を変更する
        """
        # time.sleep(0.1)
        self.switch_to.window(self.window_handles[n])

    def first_tab(self):
        """
        最初のタブやWindowに制御を変更する
        """
        self.select_tab(0)


    def last_tab(self): # *9
        """
        最後のタブやWindowに制御を変更する
        """
        self.select_tab(-1)

    # *7
    def _add_dlhist(self, f):
        """
        ダウンロード中のファイルサイズを記録する
        """
        self.dlhist[f].append(os.stat(f).st_size)

    def organize_download_files(self): # *2
        """
        ダウンロード完了したファイルをdownload_dirに移動する
        もしも既に同名のファイルが存在する場合は上書きする
        """
        pth = pathjoin(self.tmpdir.name, "*")
        for src in glob(pth):
            if not src.endswith(".crdownload"):
                self._add_dlhist(src) # *7
                dst = pathjoin(self.download_dir, basename(src))
                if exists(dst):
                    if isdir(dst):
                        shutil.rmtree(dst)
                    else:
                        os.remove(dst)
                shutil.move(src, dst)

    def is_downloading(self, f): # *7
        """
        ダウンロード中のファイル(f)のサイズが変わってるか監視する

        ファイルサイズが変わってる、またはタイムアウト前だったらTrueを返す
        タイムアウト秒経過してもファイルサイズが増えてなかったらFalseを返す
        """
        if self.timeout <= 0:
            return True

        n_hist = int(self.timeout / self._retry_interval)

        fd = self.dlhist[f]
        if len(fd) < n_hist:
            return True
        if fd[-n_hist] < fd[-1]:
            return True
        return False

    def wait_for_downloads(self): # *2
        pth = pathjoin(self.tmpdir.name, "*.crdownload")
        # ダウンロードが始まるまでちょっと待つ
        time.sleep(self._retry_interval)

        # ダウンロードが全く進行してなければループから抜ける
        while any(map(self.is_downloading, glob(pth))):
            time.sleep(self._retry_interval)
        else:
            # ダウンロード完了したファイルだけdownload_dirに移動する
            self.organize_download_files()


    def close(self):
        super().close()
        try:
            if self.download_dir:
                self.wait_for_downloads() # *2
            self.last_tab()
        except InvalidSessionIdException:
            return

    def quit(self):
        # 終了する前にダウンロード完了を待つ
        if self.download_dir:
            self.wait_for_downloads() # *2
        # 終了する前に一時ディレクトリを削除する
        if self._tmpdir:
            self._tmpdir.cleanup()
        super().quit()

    def __del__(self):
        # インスタンス削除時に何がなんでもquitを実行させる
        try:
            self.quit()
        except:
            pass


    def execute(self, driver_command, params=None): # *1(改)
        """
        sync引数がTrueの時は、処理が完全に終わるまで待ち合わせる
        """
        if not self.sync:
            return super().execute(driver_command, params)

        if self.timeout > 0 and self._cnttime < self.timeout:
            try:
                ret = super().execute(driver_command, params)
            except (TimeoutException, NoSuchElementException):
                time.sleep(self._retry_interval)
                # *1 再帰前に待機時間を計算しておく
                self._cnttime += self._retry_interval
                # *1 ここが肝の再帰
                return self.execute(driver_command, params)

        # 例外にするか悩んだが、timeout設定が0かマイナスだったら無限ループさせるようにしてみた
        elif self.timeout <= 0:
            try:
                ret = super().execute(driver_command, params)
            except (TimeoutException, NoSuchElementException):
                time.sleep(self._retry_interval)
                return self.execute(driver_command, params)

        else:
            self._cnttime = 0
            ret = super().execute(driver_command, params)

        if driver_command == "get":
            self.last_tab() # *9
            if self.page_source == "<html><head></head><body></body></html>":
                # ダウンロードする処理だった場合、次の処理に進んでしまうのでダウンロードが終わるまで待つことにした
                self.wait_for_downloads()

        return ret

    @classmethod
    def direct_download(cls, url, download_dir):
        return direct_download(url=url, download_dir=download_dir)

def test():
    from datetime import datetime as dt

    def __test_normal(url = "https://www.google.com"):
        with ChromeSync(url, download_dir="C:/temp/hoge",timeout=5) as driver:
            search = driver.xpath('//*[@name="q"]')
            search.send_keys("hoge")
            search.submit()

            time.sleep(1) # 画面がすぐに消えるので便宜上スリープさせている

    def __test_download(url, download_dir="C:/temp/hoge"):

        with ChromeSync(url, download_dir=download_dir,timeout=5) as d:
            # print(d.page_source)
            d
        target = pathjoin(download_dir, basename(url))
        assert(exists(target))
        os.remove(target)

    def __test_direct_download(url, download_dir="C:/temp/hoge"):
        direct_download(url, download_dir)
        target = pathjoin(download_dir, basename(url))
        assert(exists(target))
        os.remove(target)

    def test_1_normal_google():
        url = "https://www.google.com"
        __test_normal(url)

    def test_2_delay_google():
        url = "https://deelay.me/4000/https://www.google.com"
        __test_normal(url)

    def test_3_csv_download():
        url = "https://file-examples-com.github.io/uploads/2017/02/file_example_CSV_5000.csv"
        __test_download(url)

    def test_4_pdf_download():
        url = "https://helpx.adobe.com/jp/acrobat/kb/cq07071635/_jcr_content/main-pars/download-section/download-1/file.res/sample.pdf"
        __test_download(url)

    def test_5_xlsx_download():
        url = "https://file-examples-com.github.io/uploads/2017/02/file_example_XLSX_10.xlsx"
        __test_download(url)

    def test_6_json_download():
        url = "https://file-examples-com.github.io/uploads/2017/02/file_example_JSON_1kb.json"
        __test_direct_download(url)

    def test_7_xml_download():
        url = "https://file-examples-com.github.io/uploads/2017/02/file_example_XML_24kb.xml"
        __test_direct_download(url)

    def test_8_exe_download():
        url = "https://www.python.org/ftp/python/3.9.0/python-3.9.0-amd64.exe"
        __test_download(url)

    def test_9_new_tab():
        url = 'http://www.tagindex.com/html_tag/link/a_target.html'
        download_dir="C:/temp/hoge"

        with ChromeSync(url, download_dir=download_dir,timeout=5) as d:
            d.cssselector('[href="target_example.html"]').click()
            d.last_tab()
            assert(d.current_window_handle == d.window_handles[-1])
            assert(d.current_url == "https://www.tagindex.com/html_tag/link/target_example.html")

    for x, func in sorted(locals().items()):
        if x.startswith("test_") and callable(func):
            t1 = dt.now()
            func()
            t2 = dt.now()
            print("{} : time {}".format(x, t2-t1))

if __name__ == "__main__":
    test()