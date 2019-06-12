
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
def listlike():
    return getattr(__import__('util.core', fromlist=['listlike']), 'listlike')

@lazyobject
def isnamedtuple():
    return getattr(__import__('util.core', fromlist=['isnamedtuple']), 'isnamedtuple')

@lazyobject
def getsize():
    return getattr(__import__('util.core', fromlist=['getsize']), 'getsize')

@lazyobject
def iterhead():
    return getattr(__import__('util.core', fromlist=['iterhead']), 'iterhead')

@lazyobject
def is1darray():
    return getattr(__import__('util.core', fromlist=['is1darray']), 'is1darray')

@lazyobject
def RarArchiveWraper():
    return getattr(__import__('util.core', fromlist=['RarArchiveWraper']), 'RarArchiveWraper')

@lazyobject
def ZLibArchiveWraper():
    return getattr(__import__('util.core', fromlist=['ZLibArchiveWraper']), 'ZLibArchiveWraper')

@lazyobject
def ZipExtFile():
    return getattr(__import__('util.core', fromlist=['ZipExtFile']), 'ZipExtFile')

@lazyobject
def sortedrows():
    return getattr(__import__('util.core', fromlist=['sortedrows']), 'sortedrows')

@lazyobject
def sorter():
    return getattr(__import__('util.core', fromlist=['sorter']), 'sorter')

@lazyobject
def LhaArchiveWraper():
    return getattr(__import__('util.core', fromlist=['LhaArchiveWraper']), 'LhaArchiveWraper')

@lazyobject
def values_at():
    return getattr(__import__('util.core', fromlist=['values_at']), 'values_at')

@lazyobject
def LhaFile():
    return getattr(__import__('util.core', fromlist=['LhaFile']), 'LhaFile')

@lazyobject
def LZMAFile():
    return getattr(__import__('util.core', fromlist=['LZMAFile']), 'LZMAFile')

@lazyobject
def kwtolist():
    return getattr(__import__('util.core', fromlist=['kwtolist']), 'kwtolist')

@lazyobject
def vmfree():
    return getattr(__import__('util.core', fromlist=['vmfree']), 'vmfree')

@lazyobject
def is_compress():
    return getattr(__import__('util.core', fromlist=['is_compress']), 'is_compress')

@lazyobject
def islarge():
    return getattr(__import__('util.core', fromlist=['islarge']), 'islarge')

@lazyobject
def lsdir():
    return getattr(__import__('util.core', fromlist=['lsdir']), 'lsdir')

@lazyobject
def Path():
    return getattr(__import__('util.core', fromlist=['Path']), 'Path')

@lazyobject
def BZ2File():
    return getattr(__import__('util.core', fromlist=['BZ2File']), 'BZ2File')

@lazyobject
def GzipFile():
    return getattr(__import__('util.core', fromlist=['GzipFile']), 'GzipFile')

@lazyobject
def RarFile():
    return getattr(__import__('util.core', fromlist=['RarFile']), 'RarFile')

@lazyobject
def LhaInfo():
    return getattr(__import__('util.core', fromlist=['LhaInfo']), 'LhaInfo')

@lazyobject
def sniffer():
    return getattr(__import__('util.core', fromlist=['sniffer']), 'sniffer')

@lazyobject
def isposkey():
    return getattr(__import__('util.core', fromlist=['isposkey']), 'isposkey')

@lazyobject
def isdataframe():
    return getattr(__import__('util.core', fromlist=['isdataframe']), 'isdataframe')

@lazyobject
def DirSniff():
    return getattr(__import__('util.core', fromlist=['DirSniff']), 'DirSniff')

@lazyobject
def binopen():
    return getattr(__import__('util.core', fromlist=['binopen']), 'binopen')

@lazyobject
def zopen_recursive():
    return getattr(__import__('util.core', fromlist=['zopen_recursive']), 'zopen_recursive')

@lazyobject
def is2darray():
    return getattr(__import__('util.core', fromlist=['is2darray']), 'is2darray')

@lazyobject
def DirExProp():
    return getattr(__import__('util.core', fromlist=['DirExProp']), 'DirExProp')

@lazyobject
def flatten():
    return getattr(__import__('util.core', fromlist=['flatten']), 'flatten')

@lazyobject
def FileProp():
    return getattr(__import__('util.core', fromlist=['FileProp']), 'FileProp')

@lazyobject
def getencoding():
    return getattr(__import__('util.core', fromlist=['getencoding']), 'getencoding')

@lazyobject
def values_not():
    return getattr(__import__('util.core', fromlist=['values_not']), 'values_not')

@lazyobject
def TarFile():
    return getattr(__import__('util.core', fromlist=['TarFile']), 'TarFile')

@lazyobject
def TarArchiveWraper():
    return getattr(__import__('util.core', fromlist=['TarArchiveWraper']), 'TarArchiveWraper')

@lazyobject
def opener():
    return getattr(__import__('util.core', fromlist=['opener']), 'opener')

@lazyobject
def geturi():
    return getattr(__import__('util.core', fromlist=['geturi']), 'geturi')

@lazyobject
def in_glob():
    return getattr(__import__('util.core', fromlist=['in_glob']), 'in_glob')

@lazyobject
def timestamp2date():
    return getattr(__import__('util.core', fromlist=['timestamp2date']), 'timestamp2date')

@lazyobject
def path_norm():
    return getattr(__import__('util.core', fromlist=['path_norm']), 'path_norm')

@lazyobject
def ZipArchiveWraper():
    return getattr(__import__('util.core', fromlist=['ZipArchiveWraper']), 'ZipArchiveWraper')

@lazyobject
def zopen():
    return getattr(__import__('util.core', fromlist=['zopen']), 'zopen')

@lazyobject
def ZipFile():
    return getattr(__import__('util.core', fromlist=['ZipFile']), 'ZipFile')

@lazyobject
def FileExProp():
    return getattr(__import__('util.core', fromlist=['FileExProp']), 'FileExProp')

@lazyobject
def which():
    return getattr(__import__('util.core', fromlist=['which']), 'which')

@lazyobject
def iterrows():
    return getattr(__import__('util.core', fromlist=['iterrows']), 'iterrows')

@lazyobject
def getdialect():
    return getattr(__import__('util.core', fromlist=['getdialect']), 'getdialect')

@lazyobject
def compute_object_size():
    return getattr(__import__('util.core', fromlist=['compute_object_size']), 'compute_object_size')

@lazyobject
def logger():
    return getattr(__import__('util.core', fromlist=['logger']), 'logger')

@lazyobject
def FileSniff():
    return getattr(__import__('util.core', fromlist=['FileSniff']), 'FileSniff')

@lazyobject
def read_json():
    return getattr(__import__('util.dfutil', fromlist=['read_json']), 'read_json')

@lazyobject
def hdf():
    return getattr(__import__('util.dfutil', fromlist=['hdf']), 'hdf')

@lazyobject
def read_excel():
    return getattr(__import__('util.dfutil', fromlist=['read_excel']), 'read_excel')

@lazyobject
def read_csv():
    return getattr(__import__('util.dfutil', fromlist=['read_csv']), 'read_csv')

@lazyobject
def vdf():
    return getattr(__import__('util.dfutil', fromlist=['vdf']), 'vdf')

@lazyobject
def read_any():
    return getattr(__import__('util.dfutil', fromlist=['read_any']), 'read_any')

@lazyobject
def df_cast():
    return getattr(__import__('util.dfutil', fromlist=['df_cast']), 'df_cast')

@lazyobject
def dflines():
    return getattr(__import__('util.dfutil', fromlist=['dflines']), 'dflines')

@lazyobject
def differ():
    return getattr(__import__('util.differ', fromlist=['differ']), 'differ')

@lazyobject
def Profile():
    return getattr(__import__('util.profiler', fromlist=['Profile']), 'Profile')

@lazyobject
def profiler_df():
    return getattr(__import__('util.profiler', fromlist=['profiler_df']), 'profiler_df')

@lazyobject
def profiler():
    return getattr(__import__('util.profiler', fromlist=['profiler']), 'profiler')

@lazyobject
def read_db():
    return getattr(__import__('util.dbutil', fromlist=['read_db']), 'read_db')

@lazyobject
def read_dbsrv():
    return getattr(__import__('util.dbutil', fromlist=['read_dbsrv']), 'read_dbsrv')

@lazyobject
def read_sql():
    return getattr(__import__('util.dbutil', fromlist=['read_sql']), 'read_sql')

@lazyobject
def dml_tolist():
    return getattr(__import__('util.dmltocsv', fromlist=['dml_tolist']), 'dml_tolist')

@lazyobject
def locate():
    return getattr(__import__('util.locate', fromlist=['locate']), 'locate')

@lazyobject
def to_csv():
    return getattr(__import__('util.locate', fromlist=['to_csv']), 'to_csv')

@lazyobject
def to_excel():
    return getattr(__import__('util.locate', fromlist=['to_excel']), 'to_excel')

@lazyobject
def eachlocate():
    return getattr(__import__('util.locate', fromlist=['eachlocate']), 'eachlocate')

@lazyobject
def to_datetime():
    return getattr(__import__('util.lslog', fromlist=['to_datetime']), 'to_datetime')

@lazyobject
def lstab():
    return getattr(__import__('util.lslog', fromlist=['lstab']), 'lstab')

@lazyobject
def parse_date():
    return getattr(__import__('util.lslog', fromlist=['parse_date']), 'parse_date')

@lazyobject
def render_sankey():
    return getattr(__import__('util.sankey', fromlist=['render_sankey']), 'render_sankey')

@lazyobject
def tsvsankey():
    return getattr(__import__('util.sankey', fromlist=['tsvsankey']), 'tsvsankey')
