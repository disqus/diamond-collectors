# coding=utf-8

"""
Collect stats from civet

#### Dependencies

 * socket
 * json (or simplejson)

"""

import socket

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import diamond.collector


class CivetCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(CivetCollector, self).get_default_config_help()
        config_help.update({
            'host': "",
            'port': "",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(CivetCollector, self).get_default_config()
        config.update({
            'host':     '127.0.0.1',
            'port':     7201,
            'path':     'civet',
        })
        return config

    def get_json(self):
        json_string = ''

        address = (self.config['host'], int(self.config['port']))

        try:
            s = socket.create_connection(address, timeout=1)

            s.sendall('sample\n')

            while 1:
                data = s.recv(4096)
                if not data:
                    break
                json_string += data
        except socket.error:
            self.log.exception("Error when talking to civet")
            return ''
        finally:
            if s:
                s.close()

        return json_string

    def get_data(self):
        json_string = self.get_json()

        try:
            data = json.loads(json_string)
        except (ValueError, TypeError):
            self.log.exception("Error parsing json from civet")
            return None

        return data

    def collect(self):
        data = self.get_data()

        if not data:
            return

        for handler, stats in data.iteritems():
            for stat, value in stats.iteritems():
                metric = '.'.join([handler, stat])
                self.publish(metric, value)
