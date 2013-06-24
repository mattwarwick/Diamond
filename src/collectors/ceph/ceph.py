# coding=utf-8

"""
The CephCollector collects utilization info from the Ceph storage system.

Documentation for ceph perf counters:
http://ceph.com/docs/master/dev/perf_counters/

#### Dependencies

 * ceph [http://ceph.com/]

"""

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import glob
import os
import subprocess

import diamond.collector


def flatten_dictionary(input, sep='.', prefix=None):
    """Produces iterator of pairs where the first value is
    the joined key names and the second value is the value
    associated with the lowest level key. For example::

      {'a': {'b': 10},
       'c': 20,
       }

    produces::

      [('a.b', 10), ('c', 20)]
    """
    for name, value in sorted(input.items()):
        fullname = sep.join(filter(None, [prefix, name]))
        if isinstance(value, dict):
            for result in flatten_dictionary(value, sep, fullname):
                yield result
        else:
            yield (fullname, value)


class CephCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(CephCollector, self).get_default_config_help()
        config_help.update({
            'socket_path': 'The location of the ceph monitoring sockets.'
                           ' Defaults to "/var/run/ceph"',
            'socket_prefix': 'The first part of all socket names.'
                             ' Defaults to "ceph-"',
            'socket_ext': 'Extension for socket filenames.'
                          ' Defaults to "asok"',
            'ceph_binary': 'Path to "ceph" executable. '
                           'Defaults to /usr/bin/ceph.',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(CephCollector, self).get_default_config()
        config.update({
            'socket_path': '/var/run/ceph',
            'socket_prefix': 'ceph-',
            'socket_ext': 'asok',
            'ceph_binary': '/usr/bin/ceph',
        })
        return config

    def _get_socket_paths(self):
        """Return a sequence of paths to sockets for communicating
        with ceph daemons.
        """
        socket_pattern = os.path.join(self.config['socket_path'],
                                      (self.config['socket_prefix']
                                       + '*.' + self.config['socket_ext']))
        return glob.glob(socket_pattern)

    def _get_counter_prefix_from_socket_name(self, name):
        """Given the name of a UDS socket, return the prefix
        for counters coming from that source.
        """
        base = os.path.splitext(os.path.basename(name))[0]
        if base.startswith(self.config['socket_prefix']):
            base = base[len(self.config['socket_prefix']):]
        return 'ceph.' + base

    def _popen_check_output(self, *popenargs):
        """
        Collect Popen output and check for errors.

        This is inspired by subprocess.check_output, added in Python 2.7. This
        method provides similar functionality but will work with Python 2.6.
        """
        process = subprocess.Popen(*popenargs, stdout=subprocess.PIPE)
        output, unused_err = process.communicate()
        retcode = process.poll()
        if retcode:
            msg = "Command '%s' exited with non-zero status %d" % \
                    (popenargs[0], retcode)
            raise Exception(msg)
        return output

    def _get_admin_socket_json(self, name, args):
        """Read and parse JSON from Ceph daemon admin socket"""
        bin = self.config['ceph_binary']
        cmd = [bin, '--admin-daemon', name] + args.split()
        json_str = self._popen_check_output(cmd)
        try:
            return json.loads(json_str)
        except Exception:
            self.log.error('Could not parse JSON output from %s', name)
            raise

    def _get_perf_counters(self, name):
        """Return perf counters from admin socket."""
        counters = self._get_admin_socket_json(name, "perf dump")
        return counters

    def _publish_stats(self, counter_prefix, stats):
        """Given a stats dictionary from _get_perf_counters,
        publish the individual values.
        """
        for stat_name, stat_value in flatten_dictionary(
            stats,
            prefix=counter_prefix,
        ):
            self.publish_gauge(stat_name, stat_value)

    def collect(self):
        """
        Collect stats
        """
        for path in self._get_socket_paths():
            self.log.debug('checking %s', path)
            counter_prefix = self._get_counter_prefix_from_socket_name(path)
            stats = self._get_perf_counters(path)
            self._publish_stats(counter_prefix, stats)
        return
