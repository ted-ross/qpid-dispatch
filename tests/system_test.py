#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

"""System test library, provides tools for tests that start multiple processes,
with special support for qdrouter processes.

Features:
- Create separate directories for each test.
- Save logs, sub-process output, core files etc.
- Automated clean-up after tests: kill sub-processes etc.
- Tools to manipulate qdrouter configuration files.
- Sundry other tools.
"""

from __future__ import unicode_literals
from __future__ import division
from __future__ import absolute_import
from __future__ import print_function

import errno, os, time, socket, random, subprocess, shutil, unittest, __main__, re, sys
from copy import copy
try:
    import queue as Queue   # 3.x
except ImportError:
    import Queue as Queue   # 2.7
from threading import Thread

import proton
from proton import Message, Timeout
from proton.utils import BlockingConnection
from qpid_dispatch.management.client import Node
from qpid_dispatch_internal.compat import dict_iteritems

# Optional modules
MISSING_MODULES = []

try:
    import qpidtoollibs
except ImportError as err:
    qpidtoollibs = None         # pylint: disable=invalid-name
    MISSING_MODULES.append(str(err))

try:
    import qpid_messaging as qm
except ImportError as err:
    qm = None                   # pylint: disable=invalid-name
    MISSING_MODULES.append(str(err))

def find_exe(program):
    """Find an executable in the system PATH"""
    def is_exe(fpath):
        """True if fpath is executable"""
        return os.path.isfile(fpath) and os.access(fpath, os.X_OK)
    mydir = os.path.split(program)[0]
    if mydir:
        if is_exe(program):
            return program
    else:
        for path in os.environ["PATH"].split(os.pathsep):
            exe_file = os.path.join(path, program)
            if is_exe(exe_file):
                return exe_file
    return None

# The directory where this module lives. Used to locate static configuration files etc.
DIR = os.path.dirname(__file__)

def _check_requirements():
    """If requirements are missing, return a message, else return empty string."""
    missing = MISSING_MODULES
    required_exes = ['qdrouterd']
    missing += ["No exectuable %s"%e for e in required_exes if not find_exe(e)]

    if missing:
        return "%s: %s"%(__name__, ", ".join(missing))

MISSING_REQUIREMENTS = _check_requirements()

def retry_delay(deadline, delay, max_delay):
    """For internal use in retry. Sleep as required
    and return the new delay or None if retry should time out"""
    remaining = deadline - time.time()
    if remaining <= 0:
        return None
    time.sleep(min(delay, remaining))
    return min(delay*2, max_delay)

# Valgrind significantly slows down the response time of the router, so use a
# long default timeout
TIMEOUT = float(os.environ.get("QPID_SYSTEM_TEST_TIMEOUT", 60))

def retry(function, timeout=TIMEOUT, delay=.001, max_delay=1):
    """Call function until it returns a true value or timeout expires.
    Double the delay for each retry up to max_delay.
    Returns what function returns or None if timeout expires.
    """
    deadline = time.time() + timeout
    while True:
        ret = function()
        if ret:
            return ret
        else:
            delay = retry_delay(deadline, delay, max_delay)
            if delay is None:
                return None

def retry_exception(function, timeout=TIMEOUT, delay=.001, max_delay=1, exception_test=None):
    """Call function until it returns without exception or timeout expires.
    Double the delay for each retry up to max_delay.
    Calls exception_test with any exception raised by function, exception_test
    may itself raise an exception to terminate the retry.
    Returns what function returns if it succeeds before timeout.
    Raises last exception raised by function on timeout.
    """
    deadline = time.time() + timeout
    while True:
        try:
            return function()
        except Exception as e:    # pylint: disable=broad-except
            if exception_test:
                exception_test(e)
            delay = retry_delay(deadline, delay, max_delay)
            if delay is None:
                raise

def get_local_host_socket(protocol_family='IPv4'):
    if protocol_family == 'IPv4':
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = '127.0.0.1'
    elif protocol_family == 'IPv6':
        s = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
        host = '::1'

    return s, host

def port_available(port, protocol_family='IPv4'):
    """Return true if connecting to host:port gives 'connection refused'."""
    s, host = get_local_host_socket(protocol_family)

    try:
        s.connect((host, port))
        s.close()
    except socket.error as e:
        return e.errno == errno.ECONNREFUSED
    except:
        pass
    return False

def wait_port(port, protocol_family='IPv4', **retry_kwargs):
    """Wait up to timeout for port (on host) to be connectable.
    Takes same keyword arguments as retry to control the timeout"""
    def check(e):
        """Only retry on connection refused"""
        if not isinstance(e, socket.error) or not e.errno == errno.ECONNREFUSED:
            raise
    s, host = get_local_host_socket(protocol_family)
    try:
        retry_exception(lambda: s.connect((host, port)), exception_test=check,
                        **retry_kwargs)
    except Exception as e:
        raise Exception("wait_port timeout on host %s port %s: %s"%(host, port, e))

    finally: s.close()

def wait_ports(ports, **retry_kwargs):
    """Wait up to timeout for all ports (on host) to be connectable.
    Takes same keyword arguments as retry to control the timeout"""
    for port, protocol_family in dict_iteritems(ports):
        wait_port(port=port, protocol_family=protocol_family, **retry_kwargs)

def message(**properties):
    """Convenience to create a proton.Message with properties set"""
    m = Message()
    for name, value in dict_iteritems(properties):
        getattr(m, name)        # Raise exception if not a valid message attribute.
        setattr(m, name, value)
    return m

class Process(subprocess.Popen):
    """
    Popen that can be torn down at the end of a TestCase and stores its output.
    Use $TEST_RUNNER as a prefix to the executable if it is defined in the environment.
    """

    # Expected states of a Process at teardown
    RUNNING = -1                # Still running
    EXIT_OK = 0                 # Exit status 0
    EXIT_FAIL = 1               # Exit status 1

    unique_id = 0
    @classmethod
    def unique(cls, name):
        cls.unique_id += 1
        return "%s-%s" % (name, cls.unique_id)

    def __init__(self, args, name=None, expect=EXIT_OK, **kwargs):
        """
        Takes same arguments as subprocess.Popen. Some additional/special args:
        @param expect: Raise error if process staus not as expected at end of test:
            L{RUNNING} - expect still running.
            L{EXIT_OK} - expect proces to have terminated with 0 exit status.
            L{EXIT_FAIL} - expect proces to have terminated with exit status 1.
            integer    - expected return code
        @keyword stdout: Defaults to the file name+".out"
        @keyword stderr: Defaults to be the same as stdout
        """
        self.name = name or os.path.basename(args[0])
        self.args, self.expect = args, expect
        self.outdir = os.getcwd()
        self.outfile = os.path.abspath(self.unique(self.name))
        self.torndown = False
        args = os.environ.get('QPID_DISPATCH_TEST_RUNNER', '').split() + args
        with open(self.outfile + '.out', 'w') as out:
            kwargs.setdefault('stdout', out)
            kwargs.setdefault('stderr', subprocess.STDOUT)
            try:
                super(Process, self).__init__(args, **kwargs)
                with open(self.outfile + '.cmd', 'w') as f:
                    f.write("%s\npid=%s\n" % (' '.join(args), self.pid))
            except Exception as e:
                raise Exception("subprocess.Popen(%s, %s) failed: %s: %s" %
                                (args, kwargs, type(e).__name__, e))

    def assert_running(self):
        """Assert that the proces is still running"""
        assert self.poll() is None, "%s: exited" % ' '.join(self.args)

    def teardown(self):
        """Check process status and stop the process if necessary"""
        if self.torndown:
            return
        self.torndown = True

        def error(msg):
            with open(self.outfile + '.out') as f:
                raise RuntimeError("Process %s error: %s\n%s\n%s\n>>>>\n%s<<<<" % (
                    self.pid, msg, ' '.join(self.args),
                    self.outfile + '.cmd', f.read()));

        status = self.poll()
        if status is None:      # Still running
            self.terminate()
            if self.expect != None and  self.expect != Process.RUNNING:
                error("still running")
            self.expect = 0     # Expect clean exit after terminate
            status = self.wait()
        if self.expect != None and self.expect != status:
            error("exit code %s, expected %s" % (status, self.expect))


class Config(object):
    """Base class for configuration objects that provide a convenient
    way to create content for configuration files."""

    def write(self, name, suffix=".conf"):
        """Write the config object to file name.suffix. Returns name.suffix."""
        name = name+suffix
        with open(name, 'w') as f:
            f.write(str(self))
        return name

class Qdrouterd(Process):
    """Run a Qpid Dispatch Router Daemon"""

    class Config(list, Config):
        """
        List of ('section', {'name':'value', ...}).

        Fills in some default values automatically, see Qdrouterd.DEFAULTS
        """

        DEFAULTS = {
            'listener': {'host':'0.0.0.0', 'saslMechanisms':'ANONYMOUS', 'idleTimeoutSeconds': '120',
                         'authenticatePeer': 'no', 'role': 'normal'},
            'connector': {'host':'127.0.0.1', 'saslMechanisms':'ANONYMOUS', 'idleTimeoutSeconds': '120'},
            'router': {'mode': 'standalone', 'id': 'QDR', 'debugDumpFile': 'qddebug.txt'}
        }

        def sections(self, name):
            """Return list of sections named name"""
            return [p for n, p in self if n == name]

        @property
        def router_id(self): return self.sections("router")[0]["id"]

        def defaults(self):
            """Fill in default values in gconfiguration"""
            for name, props in self:
                if name in Qdrouterd.Config.DEFAULTS:
                    for n,p in dict_iteritems(Qdrouterd.Config.DEFAULTS[name]):
                        props.setdefault(n,p)

        def __str__(self):
            """Generate config file content. Calls default() first."""
            def tabs(level):
                return "    " * level

            def sub_elem(l, level):
                return "".join(["%s%s: {\n%s%s}\n" % (tabs(level), n, props(p, level + 1), tabs(level)) for n, p in l])

            def child(v, level):
                return "{\n%s%s}" % (sub_elem(v, level), tabs(level - 1))

            def props(p, level):
                return "".join(
                    ["%s%s: %s\n" % (tabs(level), k, v if not isinstance(v, list) else child(v, level + 1)) for k, v in
                     dict_iteritems(p)])

            self.defaults()
            return "".join(["%s {\n%s}\n"%(n, props(p, 1)) for n, p in self])

    def __init__(self, name=None, config=Config(), pyinclude=None, wait=True, perform_teardown=True, cl_args=[]):
        """
        @param name: name used for for output files, default to id from config.
        @param config: router configuration
        @keyword wait: wait for router to be ready (call self.wait_ready())
        """
        self.config = copy(config)
        self.perform_teardown = perform_teardown
        if not name: name = self.config.router_id
        assert name
        default_log = [l for l in config if (l[0] == 'log' and l[1]['module'] == 'DEFAULT')]
        if not default_log:
            config.append(
                ('log', {'module':'DEFAULT', 'enable':'trace+', 'includeSource': 'true', 'outputFile':name+'.log'}))
        args = ['qdrouterd', '-c', config.write(name)] + cl_args
        env_home = os.environ.get('QPID_DISPATCH_HOME')
        if pyinclude:
            args += ['-I', pyinclude]
        elif env_home:
            args += ['-I', os.path.join(env_home, 'python')]
        super(Qdrouterd, self).__init__(args, name=name, expect=Process.RUNNING)
        self._management = None
        self._wait_ready = False
        if wait:
            self.wait_ready()

    @property
    def management(self):
        """Return a management agent proxy for this router"""
        if not self._management:
            self._management = Node.connect(self.addresses[0], timeout=TIMEOUT)
        return self._management

    def teardown(self):
        if self._management:
            try: self._management.close()
            except: pass

        if not self.perform_teardown:
            return

        super(Qdrouterd, self).teardown()

    @property
    def ports_family(self):
        """
        Return a dict of listener ports and the respective port family
        Example -
        { 23456: 'IPv4', 243455: 'IPv6' }
        """
        ports_fam = {}
        for l in self.config.sections('listener'):
            if l.get('protocolFamily'):
                ports_fam[l['port']] = l['protocolFamily']
            else:
                ports_fam[l['port']] = 'IPv4'

        return ports_fam

    @property
    def ports(self):
        """Return list of configured ports for all listeners"""
        return [l['port'] for l in self.config.sections('listener')]

    def _cfg_2_host_port(self, c):
        host = c['host']
        port = c['port']
        protocol_family = c.get('protocolFamily', 'IPv4')
        if protocol_family == 'IPv6':
            return "[%s]:%s" % (host, port)
        elif protocol_family == 'IPv4':
            return "%s:%s" % (host, port)
        raise Exception("Unknown protocol family: %s" % protocol_family)

    @property
    def addresses(self):
        """Return amqp://host:port addresses for all listeners"""
        cfg = self.config.sections('listener')
        return ["amqp://%s" % self._cfg_2_host_port(l) for l in cfg]

    @property
    def connector_addresses(self):
        """Return list of amqp://host:port for all connectors"""
        cfg = self.config.sections('connector')
        return ["amqp://%s" % self._cfg_2_host_port(c) for c in cfg]

    @property
    def hostports(self):
        """Return host:port for all listeners"""
        return [self._cfg_2_host_port(l) for l in self.config.sections('listener')]

    def is_connected(self, port, host='127.0.0.1'):
        """If router has a connection to host:port:identity return the management info.
        Otherwise return None"""
        try:
            ret_val = False
            response = self.management.query(type="org.apache.qpid.dispatch.connection")
            index_host = response.attribute_names.index('host')
            for result in response.results:
                outs = '%s:%s' % (host, port)
                if result[index_host] == outs:
                    ret_val = True
            return ret_val
        except:
            return False

    def wait_address(self, address, subscribers=0, remotes=0, **retry_kwargs):
        """
        Wait for an address to be visible on the router.
        @keyword subscribers: Wait till subscriberCount >= subscribers
        @keyword remotes: Wait till remoteCount >= remotes
        @param retry_kwargs: keyword args for L{retry}
        """
        def check():
            # TODO aconway 2014-06-12: this should be a request by name, not a query.
            # Need to rationalize addresses in management attributes.
            # endswith check is because of M0/L/R prefixes
            addrs = self.management.query(
                type='org.apache.qpid.dispatch.router.address',
                attribute_names=[u'name', u'subscriberCount', u'remoteCount']).get_entities()

            addrs = [a for a in addrs if a['name'].endswith(address)]

            return addrs and addrs[0]['subscriberCount'] >= subscribers and addrs[0]['remoteCount'] >= remotes
        assert retry(check, **retry_kwargs)

    def get_host(self, protocol_family):
        if protocol_family == 'IPv4':
            return '127.0.0.1'
        elif protocol_family == 'IPv6':
            return '::1'
        else:
            return '127.0.0.1'

    def wait_ports(self, **retry_kwargs):
        wait_ports(self.ports_family, **retry_kwargs)

    def wait_connectors(self, **retry_kwargs):
        """
        Wait for all connectors to be connected
        @param retry_kwargs: keyword args for L{retry}
        """
        for c in self.config.sections('connector'):
            assert retry(lambda: self.is_connected(port=c['port'], host=self.get_host(c.get('protocolFamily'))),
                         **retry_kwargs), "Port not connected %s" % c['port']

    def wait_ready(self, **retry_kwargs):
        """Wait for ports and connectors to be ready"""
        if not self._wait_ready:
            self._wait_ready = True
            self.wait_ports(**retry_kwargs)
            self.wait_connectors(**retry_kwargs)
        return self

    def is_router_connected(self, router_id, **retry_kwargs):
        try:
            self.management.read(identity="router.node/%s" % router_id)
            # TODO aconway 2015-01-29: The above check should be enough, we
            # should not advertise a remote router in managment till it is fully
            # connected. However we still get a race where the router is not
            # actually ready for traffic. Investigate.
            # Meantime the following actually tests send-thru to the router.
            node = Node.connect(self.addresses[0], router_id, timeout=1)
            return retry_exception(lambda: node.query('org.apache.qpid.dispatch.router'))
        except:
            return False

    def wait_router_connected(self, router_id, **retry_kwargs):
        retry(lambda: self.is_router_connected(router_id), **retry_kwargs)


class Tester(object):
    """Tools for use by TestCase
- Create a directory for the test.
- Utilities to create processes and servers, manage ports etc.
- Clean up processes on teardown"""

    # Top level directory above any Tester directories.
    # CMake-generated configuration may be found here.
    top_dir = os.getcwd()

    # The root directory for Tester directories, under top_dir
    root_dir = os.path.abspath(__name__+'.dir')

    def __init__(self, id):
        """
        @param id: module.class.method or False if no directory should be created
        """
        self.directory = os.path.join(self.root_dir, *id.split('.'))
        self.cleanup_list = []

    def rmtree(self):
        """Remove old test class results directory"""
        shutil.rmtree(os.path.dirname(self.directory), ignore_errors=True)

    def setup(self):
        """Called from test setup and class setup."""
        os.makedirs(self.directory)
        os.chdir(self.directory)

    def teardown(self):
        """Clean up (tear-down, stop or close) objects recorded via cleanup()"""
        self.cleanup_list.reverse()
        errors = []
        for obj in self.cleanup_list:
            try:
                for method in ["teardown", "tearDown", "stop", "close"]:
                    cleanup = getattr(obj, method, None)
                    if cleanup:
                        cleanup()
                        break
            except Exception as exc:
                errors.append(exc)
        if errors:
            raise RuntimeError("Errors during teardown: \n\n%s" % "\n\n".join([str(e) for e in errors]))


    def cleanup(self, x):
        """Record object x for clean-up during tear-down.
        x should have on of the methods teardown, tearDown, stop or close"""
        self.cleanup_list.append(x)
        return x

    def popen(self, *args, **kwargs):
        """Start a Process that will be cleaned up on teardown"""
        return self.cleanup(Process(*args, **kwargs))

    def qdrouterd(self, *args, **kwargs):
        """Return a Qdrouterd that will be cleaned up on teardown"""
        return self.cleanup(Qdrouterd(*args, **kwargs))

    port_range = (20000, 30000)
    next_port = random.randint(port_range[0], port_range[1])

    @classmethod
    def get_port(cls, protocol_family='IPv4'):
        """Get an unused port"""
        def advance():
            """Advance with wrap-around"""
            cls.next_port += 1
            if cls.next_port >= cls.port_range[1]:
                cls.next_port = cls.port_range[0]
        start = cls.next_port
        while not port_available(cls.next_port, protocol_family):
            advance()
            if cls.next_port == start:
                raise Exception("No available ports in range %s", cls.port_range)
        p = cls.next_port
        advance()
        return p


class TestCase(unittest.TestCase, Tester): # pylint: disable=too-many-public-methods
    """A TestCase that sets up its own working directory and is also a Tester."""

    def __init__(self, test_method):
        unittest.TestCase.__init__(self, test_method)
        Tester.__init__(self, self.id())

    @classmethod
    def setUpClass(cls):
        cls.tester = Tester('.'.join([cls.__module__, cls.__name__, 'setUpClass']))
        cls.tester.rmtree()
        cls.tester.setup()

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls, 'tester'):
            cls.tester.teardown()
            del cls.tester

    def setUp(self):
        # Python < 2.7 will call setUp on the system_test.TestCase class
        # itself as well as the subclasses. Ignore that.
        if self.__class__ is TestCase: return
        # Hack to support setUpClass on older python.
        # If the class has not already been set up, do it now.
        if not hasattr(self.__class__, 'tester'):
            try:
                self.setUpClass()
            except:
                if hasattr(self.__class__, 'tester'):
                    self.__class__.tester.teardown()
                raise
        Tester.setup(self)

    def tearDown(self):
        # Python < 2.7 will call tearDown on the system_test.TestCase class
        # itself as well as the subclasses. Ignore that.
        if self.__class__ is TestCase: return
        Tester.teardown(self)
        # Hack to support tearDownClass on older versions of python.
        if hasattr(self.__class__, '_tear_down_class'):
            self.tearDownClass()

    def skipTest(self, reason):
        """Workaround missing unittest.TestCase.skipTest in python 2.6.
        The caller must return in order to end the test"""
        if hasattr(unittest.TestCase, 'skipTest'):
            unittest.TestCase.skipTest(self, reason)
        else:
            print("Skipping test %s: %s" % (self.id(), reason))

    # Hack to support tearDownClass on older versions of python.
    # The default TestLoader sorts tests alphabetically so we insert
    # a fake tests that will run last to call tearDownClass.
    # NOTE: definitely not safe for a parallel test-runner.
    if not hasattr(unittest.TestCase, 'tearDownClass'):
        def test_zzzz_teardown_class(self):
            """Fake test to call tearDownClass"""
            if self.__class__ is not TestCase:
                self.__class__._tear_down_class = True

    def assert_fair(self, seq):
        avg = sum(seq)/len(seq)
        for i in seq:
            assert i > avg/2, "Work not fairly distributed: %s"%seq

    def assertIn(self, item, items):
        assert item in items, "%s not in %s" % (item, items)

    if not hasattr(unittest.TestCase, 'assertRegexpMatches'):
        def assertRegexpMatches(self, text, regexp, msg=None):
            """For python < 2.7: assert re.search(regexp, text)"""
            assert re.search(regexp, text), msg or "Can't find %r in '%s'" %(regexp, text)


class SkipIfNeeded(object):
    """
    Decorator class that can be used along with test methods
    to provide skip test behavior when using both python2.6 or
    a greater version.
    This decorator can be used in test methods and a boolean
    condition must be provided (skip parameter) to define whether
    or not the test will be skipped.
    """
    def __init__(self, skip, reason):
        """
        :param skip: if True the method wont be called
        :param reason: reason why test was skipped
        """
        self.skip = skip
        self.reason = reason

    def __call__(self, f):

        def wrap(*args, **kwargs):
            """
            Wraps original test method's invocation and dictates whether or
            not the test will be executed based on value (boolean) of the
            skip parameter.
            When running test with python < 2.7, if the "skip" parameter is
            true, the original method won't be called. If running python >= 2.7, then
            skipTest will be called with given "reason" and original method
            will be invoked.
            :param args:
            :return:
            """
            instance = args[0]
            if self.skip:
                if sys.version_info < (2, 7):
                    print("%s -> skipping (python<2.7) [%s] ..." % (f.__name__, self.reason))
                    return
                else:
                    instance.skipTest(self.reason)
            return f(*args, **kwargs)

        return wrap


def main_module():
    """
    Return the module name of the __main__ module - i.e. the filename with the
    path and .py extension stripped. Useful to run the tests in the current file but
    using the proper module prefix instead of '__main__', as follows:
        if __name__ == '__main__':
            unittest.main(module=main_module())
    """
    return os.path.splitext(os.path.basename(__main__.__file__))[0]


class AsyncTestReceiver(object):
    """
    A simple receiver that runs in the background and queues any received
    messages.  Messages can be retrieved from this thread via the queue member
    """
    Empty = Queue.Empty

    def __init__(self, address, source, credit=100, timeout=0.1,
                 conn_args=None, link_args=None):
        """
        Runs a BlockingReceiver in a separate thread.

        :param address: address of router (URL)
        :param source: the node address to consume from
        :param credit: max credit for receiver
        :param timeout: receive poll frequency in seconds
        :param conn_args: map of BlockingConnection arguments
        :param link_args: map of BlockingReceiver arguments
        """
        super(AsyncTestReceiver, self).__init__()
        self.queue = Queue.Queue()
        kwargs = {'url': address}
        if conn_args:
            kwargs.update(conn_args)
        self._conn = BlockingConnection(**kwargs)
        kwargs = {'address': source,
                  'credit': credit}
        if link_args:
            kwargs.update(link_args)
        self._rcvr = self._conn.create_receiver(**kwargs)
        self._thread = Thread(target=self._poll)
        self._run = True
        self._timeout = timeout
        self._thread.start()

    def _poll(self):
        """
        Thread main loop
        """

        while self._run:
            try:
                msg = self._rcvr.receive(timeout=self._timeout)
            except Timeout:
                continue
            try:
                self._rcvr.accept()
            except IndexError:
                # PROTON-1743
                pass
            self.queue.put(msg)
        self._rcvr.close()
        self._conn.close()

    def stop(self, timeout=10):
        """
        Called to terminate the AsyncTestReceiver
        """
        self._run = False
        self._thread.join(timeout=timeout)


