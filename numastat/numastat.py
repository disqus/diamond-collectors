# coding=utf-8

import os
import diamond.collector


class NumastatCollector(diamond.collector.Collector):

    NODE = '/sys/devices/system/node'

    def get_default_config_help(self):
        config_help = super(NumastatCollector,
                            self).get_default_config_help()
        config_help.update({
        })
        return config_help

    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NumastatCollector, self).get_default_config()
        config.update({
            'path': 'numastat'
        })
        return config

    def get_data(self, path):
        numastat = open(path, 'r')

        data = dict([line.split() for line in numastat])

        return data

    def find_paths(self, path):
        paths = []
        for d in os.listdir(path):
            if d.startswith('node'):
                p = os.path.join(path, d, 'numastat')
                if os.access(p, os.R_OK):
                    paths.append(p)

        return paths

    def collect(self):

        if not os.access(self.NODE, os.R_OK):
            self.log.error('Unable to read: ' + self.NODE)
            return None

        data = {}
        for path in self.find_paths(self.NODE):
            node = path.split(os.path.sep)[5]

            for k, v in self.get_data(path).items():
                self.publish(node + '.' + k, long(v))

        return True
