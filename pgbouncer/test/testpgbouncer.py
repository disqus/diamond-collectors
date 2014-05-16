#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config
from test import unittest
from mock import patch

from pgbouncer import PgbouncerCollector

################################################################################


class TestPgbouncerCollector(CollectorTestCase):
    def setUp(self):
        config = get_collector_config('PgbouncerCollector', {})

        self.collector = PgbouncerCollector(config, None)

    def test_import(self):
        self.assertTrue(PgbouncerCollector)

    @patch.object(PgbouncerCollector, '_get_stats_by_database')
    @patch.object(PgbouncerCollector, 'publish')
    def test_simple(self, publish, _get_stats_by_database):
        _get_stats_by_database.return_value = {'foo': {'bar': 42}}

        self.collector.collect()

        self.assertPublished(publish, 'localhost_6432.foo.bar', 42)

    @patch.object(PgbouncerCollector, '_get_stats_by_database')
    @patch.object(PgbouncerCollector, 'publish')
    def test_single_instance(self, publish, _get_stats_by_database):
        _get_stats_by_database.return_value = {'foo': {'bar': 42}}

        config = get_collector_config('PgbouncerCollector', {'instances': '127.0.0.1:6433'})
        collector = PgbouncerCollector(config, None)
        collector.collect()

        self.assertPublished(publish, '127_0_0_1_6433.foo.bar', 42)

    @patch.object(PgbouncerCollector, '_get_stats_by_database')
    @patch.object(PgbouncerCollector, 'publish')
    def test_multiple_instances(self, publish, _get_stats_by_database):
        _get_stats_by_database.return_value = {'foo': {'bar': 42}}

        config = get_collector_config('PgbouncerCollector', {'instances': '127.0.0.1:6432, localhost:6433'})
        collector = PgbouncerCollector(config, None)
        collector.collect()

        self.assertPublished(publish, '127_0_0_1_6432.foo.bar', 42)
        self.assertPublished(publish, 'localhost_6433.foo.bar', 42)

    @patch.object(PgbouncerCollector, '_get_stats_by_database')
    @patch.object(PgbouncerCollector, 'publish')
    def test_instance_names(self, publish, _get_stats_by_database):
        def side_effect(host, port):
            if (host, port) == ('127.0.0.1', '6432'):
                return {'foo': {'bar': 42}}
            elif (host, port) == ('localhost', '6433'):
                return {'foo': {'baz': 24}}

        _get_stats_by_database.side_effect = side_effect

        config = get_collector_config('PgbouncerCollector', {
            'instances': '127.0.0.1:6432, localhost:6433',
            'instance_names': 'alpha, beta',
        })
        collector = PgbouncerCollector(config, None)
        collector.collect()

        self.assertPublished(publish, 'alpha.foo.bar', 42)
        self.assertPublished(publish, 'beta.foo.baz', 24)


################################################################################
if __name__ == "__main__":
    unittest.main()
