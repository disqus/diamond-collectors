# coding=utf-8

import sys

import diamond.collector
try:
    from kazoo.client import KazooClient
except ImportError:
    KazooClient = None


class KafkaConsumerOffsetsCollector(diamond.collector.Collector):
    def get_default_config(self):
        """
        Returns the default collector settings
        """
        config = super(NginxPushStreamCollector, self).get_default_config()
        config.update({
            'zk_host': '127.0.0.1:2181/kafka',
            'group_regex': '',
            'consumer_regex': ''
        })
        return config

    @property
    def zk(self):
        if not self._zk:
            self._zk = KazooClient(self.config['zk_host'])
        return self._zk

    def collect(self):
        self.zk.start()

        consumer_group_names = self.zk.get_children('/consumers')
        for group in consumer_group_names:

            if not re.search(self.config['group_regex'], group):
                self.log.info(
                    "Skipping '%s' because it doesn't match /%s/",
                    group,
                    self.config['group_regex']
                )
                continue
            ids = self.zk.get_children('/consumers/%s/ids' % group)
            if not all(
                partial(re.search, self.config['consumer_regex']),
                ids
            ):
                self.log.warn(
                    "Skipping '%s' because not all consumers match /%s/",
                    group,
                    self.config['consumer_regex']
                )
                continue

            topic_names = self.zk_get_children(
                '/consumers/%s/offsets' % group
            )
            for topic in topic_names:
                self.log.info(
                    "Will publish metrics for group '%s' and topic '%s'",
                    group,
                    topic
                )
                partition_ids = self.zk.get_children(
                    '/consumers/%s/offsets/%s' % (group, topic)
                )
                for partition in partition_ids:
                    offset = self.zk.get(
                        '/consumers/%s/offsets/%s/%s'
                        % (group, topic, partition)
                    )
                    self.publish('%s.%s.%s' % (
                        group,
                        topic,
                        partition
                    ), offset)

        self.zk.stop()

