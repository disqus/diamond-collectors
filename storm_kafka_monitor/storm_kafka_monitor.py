# coding=utf-8

"""
Collect stats via zk from storm and kafka

#### Dependencies

 * stormkafkamon
"""

import re
from random import randint

try:
    from stormkafkamon.monitor import (
        process,
        ZkClient,
    )
except ImportError:
    proccess = None
    ZkClient = None

import diamond.collector


class StormKafkaMonitorCollector(diamond.collector.Collector):
    topology_pattern = re.compile(r'(.*?)-\d+-\d+')
    zookeeper_server = 'zoo-%d.i.disqus.net' % randint(1, 5)
    storm_root = '/kafkastorm'

    def get_zk_client(self):
        """
        Lazy zk client.
        """
        if not hasattr(self, '_zk'):
            self._zk = ZkClient(','.join(zookeeper_servers), storm_path)
        return self._zk

    def running_topologies(self):
        """
        Retrieve a list of all running topologies from zookeeper.
        """
        zk = self.get_zk_client()
        raw_storms = zk.client.get_children('/storm/storms')

        storms = []

        for raw_storm in raw_storms:
            storm = self.topology_pattern.match(raw_storm)
            if not storm:
                continue

            groups = storm.groups()
            if not groups:
                continue

            storms.append(groups[0])
        return storms

    def get_topology_summary(self, topology):
        zk = self.get_zk_client()
        return process(zk.spouts(self.storm_root, topology))

    def get_summaries(self):
        """
        Iterate over all running topologies and get their summaries.
        """
        summaries = []
        topologies = self.running_topologies()
        for topology in topologies:
            summary = self.get_topology_summary(topology)
            storm.append(summary)
        return zip(topologies, summaries)

    def metric_name_from_state(self, partition_state):
        return '%s.%d.%s.' % (
            partition_state.broker.split('.', 1)[0],
            partition_state.partition,
            partition_state.topic,
        )

    def metrics_from_partition_state(self, partition_state):
        excluded = frozenset(['broker', 'topic', 'partition', 'spout'])
        prefix = self.metric_name_from_state(partition_state)
        metrics = []
        for metric in partition_state._fields:
            if metric in excluded:
                continue
            metrics.append((prefix + metric, getattr(partition_state, metric)))
        return metrics

    def get_default_config(self):
        """
        Returns the default collector settings.
        """
        config = super(StormKafkaMonitorCollector, self).get_default_config()
        config.update({
            'host': '127.0.0.1',
            'port': 7200,
            'path': 'storm.spout.kafka',
            'method': 'Threaded',
        })
        return config

    def collect(self):
        partition_summaries = self.get_summaries()

        for topology, partition_summary in partition_summaries:
            prefix = '%s.' % topology
            for metric in partition_summary._fields:
                # Set Metric Name
                metric_name = metric

                # Set Metric Value
                metric_value = getattr(partition_summary, metric)

                # Publish Metric
                if metric_name == 'partitions':
                    for partition_state in metric_value:
                        for metric, value in self.metrics_from_partition_state(partition_state):
                            self.publish(prefix + metric, value)

                else:
                    self.publish(prefix + metric_name, metric_value)

