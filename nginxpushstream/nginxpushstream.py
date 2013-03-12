# coding=utf-8

"""
Collect the stats from the nginx-push-stream module

#### Dependencies

 * urlib2
 * json (or simplejson)

"""

import socket
import urllib2

try:
    import json
    json  # workaround for pyflakes issue #13
except ImportError:
    import simplejson as json

import diamond.collector


class NginxPushStreamCollector(diamond.collector.Collector):
    METRIC_KEYS = frozenset(['channels', 'broadcast_channels',
                            'published_messages', 'subscribers', 'uptime'])

    def get_default_config_help(self):
        config_help = super(NginxPushStreamCollector,
                            self).get_default_config_help()
        config_help.update({
            'host': '',
            'port': '',
            'location': "Location with push_stream_channel_statistics enabled",
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NginxPushStreamCollector, self).get_default_config()
        config.update({
            'host':     '127.0.0.1',
            'port':     80,
            'location': '/push-stream-status',
            'path':     'nginxpushstream',
            'method':   'Threaded',
        })
        return config

    def get_json(self):
        url = ''.join(['http://',
                       self.config['host'],
                       ':',
                       str(self.config['port']),
                       self.config['location']])

        try:
            response = urllib2.urlopen(url).read()
        except urllib2.HTTPError, err:
            self.log.error("%s %s: %s", url, err.code, err.read())
            return ''
        except urllib2.URLError, err:
            self.log.error("%s: %s", url, err.reason)
            return ''
        except socket.error, err:
            self.logg.error("%s %s: %s", url, err.errno, err.message)
            return ''

        return response

    def get_data(self):
        json_string = self.get_json()

        try:
            data = json.loads(json_string)
        except (TypeError, ValueError):
            self.log.exception("Unable to parse response from push-stream")
            return None

        return data

    def collect(self):
        data = self.get_data()

        if not data:
            return

        for key, stat in data.iteritems():
            if key in self.METRIC_KEYS:
                self.publish(key, stat)
