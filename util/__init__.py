

from util.core import *

# Lazy and self destructive containers for speeding up module import.
# Copyright 2015-2016, the xonsh developers. All rights reserved.
import os
import sys
import time
import types
import builtins
import threading
import importlib
import importlib.util
import collections.abc as cabc

__version__ = '0.1.4'


class LazyObject(object):

    def __init__(self, load, ctx, name):
        """Lazily loads an object via the load function the first time an
        attribute is accessed. Once loaded it will replace itself in the
        provided context (typically the globals of the call site) with the
        given name.

        For example, you can prevent the compilation of a regular expreession
        until it is actually used::

            DOT = LazyObject((lambda: re.compile('.')), globals(), 'DOT')

        Parameters
        ----------
        load : function with no arguments
            A loader function that performs the actual object construction.
        ctx : Mapping
            Context to replace the LazyObject instance in
            with the object returned by load().
        name : str
            Name in the context to give the loaded object. This *should*
            be the name on the LHS of the assignment.
        """
        self._lasdo = {
            'loaded': False,
            'load': load,
            'ctx': ctx,
            'name': name,
            }

    def _lazy_obj(self):
        d = self._lasdo
        if d['loaded']:
            obj = d['obj']
        else:
            obj = d['load']()
            d['ctx'][d['name']] = d['obj'] = obj
            d['loaded'] = True
        return obj

    def __getattribute__(self, name):
        if name == '_lasdo' or name == '_lazy_obj':
            return super().__getattribute__(name)
        obj = self._lazy_obj()
        return getattr(obj, name)

    def __bool__(self):
        obj = self._lazy_obj()
        return bool(obj)

    def __iter__(self):
        obj = self._lazy_obj()
        yield from obj

    def __getitem__(self, item):
        obj = self._lazy_obj()
        return obj[item]

    def __setitem__(self, key, value):
        obj = self._lazy_obj()
        obj[key] = value

    def __delitem__(self, item):
        obj = self._lazy_obj()
        del obj[item]

    def __call__(self, *args, **kwargs):
        obj = self._lazy_obj()
        return obj(*args, **kwargs)

    def __lt__(self, other):
        obj = self._lazy_obj()
        return obj < other

    def __le__(self, other):
        obj = self._lazy_obj()
        return obj <= other

    def __eq__(self, other):
        obj = self._lazy_obj()
        return obj == other

    def __ne__(self, other):
        obj = self._lazy_obj()
        return obj != other

    def __gt__(self, other):
        obj = self._lazy_obj()
        return obj > other

    def __ge__(self, other):
        obj = self._lazy_obj()
        return obj >= other

    def __hash__(self):
        obj = self._lazy_obj()
        return hash(obj)

    def __or__(self, other):
        obj = self._lazy_obj()
        return obj | other

    def __str__(self):
        return str(self._lazy_obj())

    def __repr__(self):
        return repr(self._lazy_obj())


def lazyobject(f):
    """Decorator for constructing lazy objects from a function."""
    return LazyObject(f, f.__globals__, f.__name__)


class LazyDict(cabc.MutableMapping):

    def __init__(self, loaders, ctx, name):
        """Dictionary like object that lazily loads its values from an initial
        dict of key-loader function pairs.  Each key is loaded when its value
        is first accessed. Once fully loaded, this object will replace itself
        in the provided context (typically the globals of the call site) with
        the given name.

        For example, you can prevent the compilation of a bunch of regular
        expressions until they are actually used::

            RES = LazyDict({
                    'dot': lambda: re.compile('.'),
                    'all': lambda: re.compile('.*'),
                    'two': lambda: re.compile('..'),
                    }, globals(), 'RES')

        Parameters
        ----------
        loaders : Mapping of keys to functions with no arguments
            A mapping of loader function that performs the actual value
            construction upon acces.
        ctx : Mapping
            Context to replace the LazyDict instance in
            with the the fully loaded mapping.
        name : str
            Name in the context to give the loaded mapping. This *should*
            be the name on the LHS of the assignment.
        """
        self._loaders = loaders
        self._ctx = ctx
        self._name = name
        self._d = type(loaders)()  # make sure to return the same type

    def _destruct(self):
        if len(self._loaders) == 0:
            self._ctx[self._name] = self._d

    def __getitem__(self, key):
        d = self._d
        if key in d:
            val = d[key]
        else:
            # pop will raise a key error for us
            loader = self._loaders.pop(key)
            d[key] = val = loader()
            self._destruct()
        return val

    def __setitem__(self, key, value):
        self._d[key] = value
        if key in self._loaders:
            del self._loaders[key]
            self._destruct()

    def __delitem__(self, key):
        if key in self._d:
            del self._d[key]
        else:
            del self._loaders[key]
            self._destruct()

    def __iter__(self):
        yield from (set(self._d.keys()) | set(self._loaders.keys()))

    def __len__(self):
        return len(self._d) + len(self._loaders)


def lazydict(f):
    """Decorator for constructing lazy dicts from a function."""
    return LazyDict(f, f.__globals__, f.__name__)


class LazyBool(object):

    def __init__(self, load, ctx, name):
        """Boolean like object that lazily computes it boolean value when it is
        first asked. Once loaded, this result will replace itself
        in the provided context (typically the globals of the call site) with
        the given name.

        For example, you can prevent the complex boolean until it is actually
        used::

            ALIVE = LazyDict(lambda: not DEAD, globals(), 'ALIVE')

        Parameters
        ----------
        load : function with no arguments
            A loader function that performs the actual boolean evaluation.
        ctx : Mapping
            Context to replace the LazyBool instance in
            with the the fully loaded mapping.
        name : str
            Name in the context to give the loaded mapping. This *should*
            be the name on the LHS of the assignment.
        """
        self._load = load
        self._ctx = ctx
        self._name = name
        self._result = None

    def __bool__(self):
        if self._result is None:
            res = self._ctx[self._name] = self._result = self._load()
        else:
            res = self._result
        return res


def lazybool(f):
    """Decorator for constructing lazy booleans from a function."""
    return LazyBool(f, f.__globals__, f.__name__)


#
# Background module loaders
#

class BackgroundModuleProxy(types.ModuleType):
    """Proxy object for modules loaded in the background that block attribute
    access until the module is loaded..
    """

    def __init__(self, modname):
        self.__dct__ = {
            'loaded': False,
            'modname': modname,
            }

    def __getattribute__(self, name):
        passthrough = frozenset({'__dct__', '__class__', '__spec__'})
        if name in passthrough:
            return super().__getattribute__(name)
        dct = self.__dct__
        modname = dct['modname']
        if dct['loaded']:
            mod = sys.modules[modname]
        else:
            delay_types = (BackgroundModuleProxy, type(None))
            while isinstance(sys.modules.get(modname, None), delay_types):
                time.sleep(0.001)
            mod = sys.modules[modname]
            dct['loaded'] = True
        # some modules may do construction after import, give them a second
        stall = 0
        while not hasattr(mod, name) and stall < 1000:
            stall += 1
            time.sleep(0.001)
        return getattr(mod, name)


class BackgroundModuleLoader(threading.Thread):
    """Thread to load modules in the background."""

    def __init__(self, name, package, replacements, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.daemon = True
        self.name = name
        self.package = package
        self.replacements = replacements
        self.start()

    def run(self):
        # wait for other modules to stop being imported
        # We assume that module loading is finished when sys.modules doesn't
        # get longer in 5 consecutive 1ms waiting steps
        counter = 0
        last = -1
        while counter < 5:
            new = len(sys.modules)
            if new == last:
                counter += 1
            else:
                last = new
                counter = 0
            time.sleep(0.001)
        # now import module properly
        modname = importlib.util.resolve_name(self.name, self.package)
        if isinstance(sys.modules[modname], BackgroundModuleProxy):
            del sys.modules[modname]
        mod = importlib.import_module(self.name, package=self.package)
        for targname, varname in self.replacements.items():
            if targname in sys.modules:
                targmod = sys.modules[targname]
                setattr(targmod, varname, mod)


def load_module_in_background(name, package=None, debug='DEBUG', env=None,
                              replacements=None):
    """Entry point for loading modules in background thread.

    Parameters
    ----------
    name : str
        Module name to load in background thread.
    package : str or None, optional
        Package name, has the same meaning as in importlib.import_module().
    debug : str, optional
        Debugging symbol name to look up in the environment.
    env : Mapping or None, optional
        Environment this will default to __xonsh_env__, if available, and
        os.environ otherwise.
    replacements : Mapping or None, optional
        Dictionary mapping fully qualified module names (eg foo.bar.baz) that
        import the lazily loaded moudle, with the variable name in that
        module. For example, suppose that foo.bar imports module a as b,
        this dict is then {'foo.bar': 'b'}.

    Returns
    -------
    module : ModuleType
        This is either the original module that is found in sys.modules or
        a proxy module that will block until delay attribute access until the
        module is fully loaded.
    """
    modname = importlib.util.resolve_name(name, package)
    if modname in sys.modules:
        return sys.modules[modname]
    if env is None:
        env = getattr(builtins, '__xonsh_env__', os.environ)
    if env.get(debug, None):
        mod = importlib.import_module(name, package=package)
        return mod
    proxy = sys.modules[modname] = BackgroundModuleProxy(modname)
    BackgroundModuleLoader(name, package, replacements or {})
    return proxy


@lazyobject
def is_bin():
    return getattr(__import__('util.filetype', fromlist=['is_bin']), 'is_bin')

@lazyobject
def is_ppt():
    return getattr(__import__('util.filetype', fromlist=['is_ppt']), 'is_ppt')

@lazyobject
def is_office():
    return getattr(__import__('util.filetype', fromlist=['is_office']), 'is_office')

@lazyobject
def is_lha():
    return getattr(__import__('util.filetype', fromlist=['is_lha']), 'is_lha')

@lazyobject
def is_html():
    return getattr(__import__('util.filetype', fromlist=['is_html']), 'is_html')

@lazyobject
def is_tar():
    return getattr(__import__('util.filetype', fromlist=['is_tar']), 'is_tar')

@lazyobject
def is_json():
    return getattr(__import__('util.filetype', fromlist=['is_json']), 'is_json')

@lazyobject
def is_csv():
    return getattr(__import__('util.filetype', fromlist=['is_csv']), 'is_csv')

@lazyobject
def is_xml():
    return getattr(__import__('util.filetype', fromlist=['is_xml']), 'is_xml')

@lazyobject
def is_doc():
    return getattr(__import__('util.filetype', fromlist=['is_doc']), 'is_doc')

@lazyobject
def is_xls():
    return getattr(__import__('util.filetype', fromlist=['is_xls']), 'is_xls')

@lazyobject
def guesstype():
    return getattr(__import__('util.filetype', fromlist=['guesstype']), 'guesstype')

@lazyobject
def is_dml():
    return getattr(__import__('util.filetype', fromlist=['is_dml']), 'is_dml')

@lazyobject
def is_text():
    return getattr(__import__('util.filetype', fromlist=['is_text']), 'is_text')

@lazyobject
def DBgrouprow():
    return getattr(__import__('util.io', fromlist=['DBgrouprow']), 'DBgrouprow')

@lazyobject
def getinfo():
    return getattr(__import__('util.io', fromlist=['getinfo']), 'getinfo')

@lazyobject
def readrow():
    return getattr(__import__('util.io', fromlist=['readrow']), 'readrow')

@lazyobject
def lsdir():
    return getattr(__import__('util.io', fromlist=['lsdir']), 'lsdir')

@lazyobject
def xmltodict():
    return getattr(__import__('util.io', fromlist=['xmltodict']), 'xmltodict')

@lazyobject
def DBrow():
    return getattr(__import__('util.io', fromlist=['DBrow']), 'DBrow')

@lazyobject
def getsize():
    return getattr(__import__('util.io', fromlist=['getsize']), 'getsize')

@lazyobject
def grouprow():
    return getattr(__import__('util.io', fromlist=['grouprow']), 'grouprow')

@lazyobject
def unicode_escape():
    return getattr(__import__('util.io', fromlist=['unicode_escape']), 'unicode_escape')

@lazyobject
def Path():
    return getattr(__import__('util.io', fromlist=['Path']), 'Path')

@lazyobject
def kwtolist():
    return getattr(__import__('util.utils', fromlist=['kwtolist']), 'kwtolist')

@lazyobject
def isnamedtuple():
    return getattr(__import__('util.utils', fromlist=['isnamedtuple']), 'isnamedtuple')

@lazyobject
def decompressor():
    return getattr(__import__('util.utils', fromlist=['decompressor']), 'decompressor')

@lazyobject
def isdataframe():
    return getattr(__import__('util.utils', fromlist=['isdataframe']), 'isdataframe')

@lazyobject
def islarge():
    return getattr(__import__('util.utils', fromlist=['islarge']), 'islarge')

@lazyobject
def which():
    return getattr(__import__('util.utils', fromlist=['which']), 'which')

@lazyobject
def iterhead():
    return getattr(__import__('util.utils', fromlist=['iterhead']), 'iterhead')

@lazyobject
def to_datetime():
    return getattr(__import__('util.utils', fromlist=['to_datetime']), 'to_datetime')

@lazyobject
def vmfree():
    return getattr(__import__('util.utils', fromlist=['vmfree']), 'vmfree')

@lazyobject
def sorter():
    return getattr(__import__('util.utils', fromlist=['sorter']), 'sorter')

@lazyobject
def iterrows():
    return getattr(__import__('util.utils', fromlist=['iterrows']), 'iterrows')

@lazyobject
def compressor():
    return getattr(__import__('util.utils', fromlist=['compressor']), 'compressor')

@lazyobject
def timestamp2date():
    return getattr(__import__('util.utils', fromlist=['timestamp2date']), 'timestamp2date')

@lazyobject
def sniffer():
    return getattr(__import__('util.utils', fromlist=['sniffer']), 'sniffer')

@lazyobject
def csvreader():
    return getattr(__import__('util.utils', fromlist=['csvreader']), 'csvreader')

@lazyobject
def listlike():
    return getattr(__import__('util.utils', fromlist=['listlike']), 'listlike')

@lazyobject
def sortedrows():
    return getattr(__import__('util.utils', fromlist=['sortedrows']), 'sortedrows')

@lazyobject
def isposkey():
    return getattr(__import__('util.utils', fromlist=['isposkey']), 'isposkey')

@lazyobject
def Counter():
    return getattr(__import__('util.utils', fromlist=['Counter']), 'Counter')

@lazyobject
def command():
    return getattr(__import__('util.utils', fromlist=['command']), 'command')

@lazyobject
def values_at():
    return getattr(__import__('util.utils', fromlist=['values_at']), 'values_at')

@lazyobject
def geturi():
    return getattr(__import__('util.utils', fromlist=['geturi']), 'geturi')

@lazyobject
def values_not():
    return getattr(__import__('util.utils', fromlist=['values_not']), 'values_not')

@lazyobject
def path_norm():
    return getattr(__import__('util.utils', fromlist=['path_norm']), 'path_norm')

@lazyobject
def is1darray():
    return getattr(__import__('util.utils', fromlist=['is1darray']), 'is1darray')

@lazyobject
def back_to_path():
    return getattr(__import__('util.utils', fromlist=['back_to_path']), 'back_to_path')

@lazyobject
def compute_object_size():
    return getattr(__import__('util.utils', fromlist=['compute_object_size']), 'compute_object_size')

@lazyobject
def in_glob():
    return getattr(__import__('util.utils', fromlist=['in_glob']), 'in_glob')

@lazyobject
def is2darray():
    return getattr(__import__('util.utils', fromlist=['is2darray']), 'is2darray')

@lazyobject
def getdialect():
    return getattr(__import__('util.utils', fromlist=['getdialect']), 'getdialect')

@lazyobject
def lazydate():
    return getattr(__import__('util.utils', fromlist=['lazydate']), 'lazydate')

@lazyobject
def lslR():
    return getattr(__import__('util.lslog', fromlist=['lslR']), 'lslR')

@lazyobject
def lsl():
    return getattr(__import__('util.lslog', fromlist=['lsl']), 'lsl')

@lazyobject
def isin_nw():
    return getattr(__import__('util.nw', fromlist=['isin_nw']), 'isin_nw')

@lazyobject
def formatip():
    return getattr(__import__('util.nw', fromlist=['formatip']), 'formatip')

@lazyobject
def getipinfo():
    return getattr(__import__('util.nw', fromlist=['getipinfo']), 'getipinfo')

@lazyobject
def tokenip():
    return getattr(__import__('util.nw', fromlist=['tokenip']), 'tokenip')
