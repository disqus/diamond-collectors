import os
import re

from collections import defaultdict
from fnmatch import fnmatch
from time import sleep

import diamond.collector

__all__ = ['ProcessCpuCollector']

_CLOCK_RATE = os.sysconf(os.sysconf_names['SC_CLK_TCK'])
_NUM_CPUS = os.sysconf('SC_NPROCESSORS_ONLN')


class Filter(object):
    """Base Filter

    All Filters need to implement at least:
        _filter
        is_compatible

    """
    def __init__(self, filter):
        self.filter = filter

    def match(self, on):
        """Calls _filter which is overloaded by a subclass"""
        return self._filter(on)

    def _filter(self, on):
        """Return True/False if this filter matches"""
        raise NotImplementedError()

    @staticmethod
    def is_compatible(filter_string):
        """Is the defined filter_string compatible with the filter?"""
        raise NotImplementedError()


class PIDFileFilter(Filter):
    """PID file Filter

    Given a PID file, match on the PID(s) it contains.

    [[haproxy]]
    pidfile = /var/run/haproxy.pid

    """
    def __init__(self, *args, **kwargs):
        super(PIDFileFilter, self).__init__(*args, **kwargs)

        self._last_mtime = 0

    @staticmethod
    def is_compatible(filter_string):
        """Is it a basestring and the path to a readable file?"""
        return (isinstance(filter_string, basestring)
                and os.access(filter_string, os.R_OK))

    def _filter(self, on):
        return on.split(os.sep)[2] in self.pids

    @property
    def pids(self):
        """Cache the pids from the pid file based on the
        modified time of the file.

        It expects up to 1 pid per line.

        """
        try:
            st_mtime = os.stat(self.filter).st_mtime
        except OSError:
            return []

        if st_mtime > self._last_mtime:
            try:
                with open(self.filter) as fp:
                    pids = fp.readlines()
                    self._last_mtime = st_mtime
            except IOError:
                return []
            else:
                self._pids = [pid.strip() for pid in pids]

        return self._pids


class EXEFilter(Filter):
    """/proc/<pid>/exe filter

    This uses fnmatch to check if the filter_string matches the exe.

    [[haproxy]]
    exe = /usr/local/bin/haproxy

    """
    @staticmethod
    def is_compatible(filter_string):
        return isinstance(filter_string, basestring)

    def _filter(self, on):
        try:
            return fnmatch(os.path.realpath(os.path.join(on, 'exe')),
                           self.filter)
        except OSError:
            return False


class CMDLineFilter(Filter):
    """/proc/<pid>/cmdline filter

    Provide a regex to match on the cmdline on.

    [[haproxy]]
    cmdline = /usr/local/bin/haproxy.* -f /etc/haproxy/haproxy_misc.cfg.*

    """
    def __init__(self, filter):
        super(CMDLineFilter, self).__init__(filter)

        self.filter_re = re.compile(self.filter)

    @staticmethod
    def is_compatible(filter_string):
        try:
            re.compile(filter_string)
        except re.error:
            return False
        else:
            return True

    def _filter(self, on):
        with open(os.path.join(on, 'cmdline')) as fp:
            cmdline = fp.read().strip()

        # convert to traditional space separated line
        cmdline = ' '.join(cmdline.split('\x00'))
        return self.filter_re.search(cmdline) is not None


def get_proc_cputime(pid):
    """Sum proc user jiffies and system jiffies and return total time."""
    with open('/proc/%s/stat' % pid) as proc_stat:
        stat = proc_stat.read().strip()

    stats = stat[stat.find(')') + 2:].split()

    user_cputime = float(stats[11]) / _CLOCK_RATE
    system_cputime = float(stats[12]) / _CLOCK_RATE

    return user_cputime + system_cputime


def get_system_cputime():
    """Retrun total system cputime."""
    with open('/proc/stat') as proc_stat:
        stats = proc_stat.read().split()

    return sum([float(s) / _CLOCK_RATE for s in stats[1:8]])


class ProcessCpuCollector(diamond.collector.Collector):

    __cache = defaultdict(dict)

    FILTERS = {
        'pidfile': PIDFileFilter,
        'exe': EXEFilter,
        'cmdline': CMDLineFilter,
    }

    def get_default_config_help(self):
        config_help = super(ProcessCpuCollector,
                            self).get_default_config_help()
        config_help.update({
            'process': ("A subcategory of settings inside of which each "
                        "collected process has it's configuration")
        })
        return config_help

    def get_default_config(self):
        """
        Default settings are:
            path: 'processcpu'
        """
        config = super(ProcessCpuCollector, self).get_default_config()
        config.update({
            'path':     'processcpu',
            'process':  '',
            'method':   'Threaded',
        })
        return config

    @classmethod
    def calc_deltas(cls, proc):
        """Calculate deltas of proc and sys cputime from previous run.

        If this is the first run, get times and cache, then sleep 100ms
        before calculating the delta.

        """
        if proc not in cls.__cache:
            cls.__cache[proc]['proc'] = get_proc_cputime(proc)
            cls.__cache[proc]['sys'] = get_system_cputime()
            sleep(0.1)

        proc_cputime = get_proc_cputime(proc)
        sys_cputime = get_system_cputime()

        delta_proc = proc_cputime - cls.__cache[proc]['proc']
        delta_sys = sys_cputime - cls.__cache[proc]['sys']

        cls.__cache[proc]['proc'] = proc_cputime
        cls.__cache[proc]['sys'] = sys_cputime

        return delta_proc, delta_sys

    def get_processes(self):
        """Instantiate process filters from config file."""
        processes = defaultdict(list)

        for process, cfg in self.config['process'].iteritems():
            for filter_type in self.FILTERS:
                if filter_type not in cfg:
                    continue

                filter_string = cfg[filter_type]

                if self.FILTERS[filter_type].is_compatible(filter_string):
                    proc_filter = self.FILTERS[filter_type](filter_string)
                    processes[process].append(proc_filter)
                else:
                    self.log.error("Incompatible with filter %s: %s",
                                   filter_type,
                                   filter_string)

        return processes

    def collect(self):
        """Crawl /proc for any processes that match a filter and
            generate the data dict.

        If no processes are defined, return immediately.

        """
        processes = self.get_processes()

        if not processes:
            return

        data = defaultdict(float)

        for proc in os.listdir('/proc'):
            proc_path = os.path.join('/proc', proc)
            if not os.path.isdir(proc_path) or not proc.isdigit():
                continue

            for name, filters in processes.iteritems():
                for proc_filter in filters:
                    if not proc_filter.match(proc_path):
                        continue

                    delta_proc, delta_sys = self.calc_deltas(proc)

                    # If the PID changed the delta will most likely be negative
                    # So delete the proc from the cache and re-calculate
                    if delta_proc < 0.0:
                        del self.__cache[proc]
                        delta_proc, delta_sys = self.calc_deltas(proc)

                    try:
                        # Turn into total percentage 100% * num of CPUs
                        data[name] += ((delta_proc / delta_sys) * 100.0
                                       * _NUM_CPUS)
                    except ZeroDivisionError:
                        data[name] += 0.0

        for metric, value in data.iteritems():
            self.publish(metric, value)
