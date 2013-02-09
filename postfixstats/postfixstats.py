# coding=utf-8

"""
Collect stats from postfix-stats. postfix-stats is a simple threaded stats
aggregator for Postfix. When running as a syslog destination, it can be used to
get realtime cumulative stats.

#### Dependencies

 * socket
 * json (or simeplejson)
 * [postfix-stats](https://github.com/disqus/postfix-stats)

"""

import socket

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import diamond.collector

DOTS_TO_UNDERS = {ord(u'.'): u'_'}
DOTS_TO_NONE = {ord(u'.'): None}


class PostfixStatsCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(PostfixStatsCollector,
                            self).get_default_config_help()
        config_help.update({
            'host':             'Hostname to coonect to',
            'port':             'Port to connect to',
            'include_clients':  'Include client connection stats',
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(PostfixStatsCollector, self).get_default_config()
        config.update({
            'path':             'postfixstats',
            'host':             'localhost',
            'port':             7777,
            'include_clients':  False,
        })
        return config

    def get_json(self):
        json_string = ''

        address = (self.config['host'], int(self.config['port']))

        try:
            s = socket.create_connection(address, timeout=1)

            s.sendall('stats\n')

            while 1:
                data = s.recv(4096)
                if not data:
                    break
                json_string += data
        except socket.error:
            self.log.exception("Error talking to postfix-stats")
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
            self.log.exception("Error parsing json from postfix-stats")
            return None

        return data

    def collect(self):
        data = self.get_data()

        if not data:
            return

        if self.config['include_clients'] and u'clients' in data:
            for client, value in data['clients'].iteritems():
                # translate dots to underscores in client names
                metric = u'.'.join(['clients',
                                    client.translate(DOTS_TO_UNDERS)])

                dvalue = self.derivative(metric, value)

                self.publish(metric, value)

        for action in (u'in', u'recv', u'send'):
            if action not in data:
                continue

            for sect, stats in data[action].iteritems():
                for status, value in stats.iteritems():
                    metric = '.'.join([action,
                                       sect,
                                       status.translate(DOTS_TO_NONE)])

                    dvalue = self.derivative(metric, value)

                    self.publish(metric, dvalue)

        if u'local' in data:
            for key, value in data[u'local'].iteritems():
                metric = '.'.join(['local', key])

                dvalue = self.derivative(metric, value)

                self.publish(metric, dvalue)
