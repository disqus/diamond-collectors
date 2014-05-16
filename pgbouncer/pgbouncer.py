# coding=utf-8

"""
Collect metrics from postgresql

#### Dependencies

 * psycopg2

"""

from collections import defaultdict
from itertools import izip

import diamond.collector

try:
    import psycopg2
    import psycopg2.extras
    psycopg2  # workaround for pyflakes issue #13
except ImportError:
    psycopg2 = None

STATS_QUERIES = ['SHOW POOLS', 'SHOW STATS']
IGNORE_COLUMNS = ['user']


class PgbouncerCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(PgbouncerCollector, self).get_default_config_help()
        config_help.update({
            'user': 'Username',
            'password': 'Password',
            'instances': 'PgBouncer addresses, comma separated '
                         '(e.g: "localhost:6432,localhost:6433")',
            'instance_names': 'Pretty names to use for metric names, comma separated. '
                              'Must map to instances (e.g: "master,replicas")',
        })

        return config_help

    def get_default_config(self):
        config = super(PgbouncerCollector, self).get_default_config()
        config.update({
            'path': 'pgbouncer',
            'user': 'postgres',
            'password': '',
            'instances': 'localhost:6432',
            'instance_names': '',
        })

        return config

    def collect(self):
        if psycopg2 is None:
            self.log.error('Unable to import module psycopg2.')
            return {}

        instances = self.config['instances']
        if isinstance(instances, basestring):
            instances = [instances]

        instance_names = self.config['instance_names']

        if not instance_names:
            instance_names = instances
        elif isinstance(instance_names, basestring):
            instance_names = [instance_names]

        if len(instances) != len(instance_names):
            self.log.error('Must provide same number of `instance_names` as `instances`.')
            return {}

        for instance, name in izip(instances, instance_names):
            instance = instance.strip()
            host, port = instance.split(':')

            for database, stats in self._get_stats_by_database(host, port).iteritems():
                for stat_name, stat_value in stats.iteritems():
                    self.publish(self._get_metric_name(name, database, stat_name),
                                 stat_value)

    def _get_metric_name(self, name, database, stat_name):
        name = name.replace('.', '_').replace(':', '_').strip()
        return '.'.join([name, database, stat_name])

    def _get_stats_by_database(self, host, port):
        # Mapping of database name -> stats.
        databases = defaultdict(dict)
        conn = psycopg2.connect(database='pgbouncer',
                                user=self.config['user'],
                                password=self.config['password'],
                                host=host,
                                port=port)

        # Avoid using transactions, set isolation level to autocommit
        conn.set_isolation_level(0)

        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        for query in STATS_QUERIES:
            cursor.execute(query)
            for row in cursor.fetchall():
                stats = row.copy()
                database = stats.pop('database')

                for ignore in IGNORE_COLUMNS:
                    if ignore in stats:
                        stats.pop(ignore)

                databases[database].update(stats)

        return databases
