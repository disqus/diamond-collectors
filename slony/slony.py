# coding=utf-8

"""
Collect slony metrics from postgresql

#### Dependencies

 * psycopg2

"""

import diamond.collector
from postgres import QueryStats

try:
    import psycopg2
    import psycopg2.extensions
    psycopg2  # workaround for pyflakes issue #13
except ImportError:
    psycopg2 = None


class SlonyCollector(diamond.collector.Collector):

    def get_default_config_help(self):
        config_help = super(SlonyCollector, self).get_default_config_help()
        config_help.update({
            'host': 'Hostname',
            'user': 'Username',
            'password': 'Password',
            'port': 'Port number',
            'slony_db': 'Name of replicated database',
            'slony_node_string': 'Regex for SQL SUBSTRING to pull hostname from sl_node.no_comment',
            'slony_schema': 'Slony schemaname',
            'underscore': 'Convert _ to .',
            'pg_version':
            "The version of postgres that you'll be monitoring eg in format 9.3",
        })
        return config_help

    def get_default_config(self):
        """
        Return default config.
        """
        config = super(SlonyCollector, self).get_default_config()
        config.update({
            'path': 'postgres',
            'host': 'localhost',
            'user': 'postgres',
            'password': 'postgres',
            'port': 5432,
            'slony_db': 'postgres',
            'slony_node_string': 'Node [0-9]+ - postgres@localhost',
            'slony_schema': '_slony',
            'underscore': False,
            'method': 'Threaded',
            'pg_version': 9.3,
        })
        return config

    def collect(self):
        if psycopg2 is None:
            self.log.error('Unable to import module psycopg2')
            return {}

        db = self.config['slony_db']
        self.connections = {}
        self.connections[db] = self._connect(database=db)

        params = (
            self.config['slony_node_string'],
            psycopg2.extensions.AsIs(self.config['slony_schema']),
            psycopg2.extensions.AsIs(self.config['slony_schema']),
        )

        stat = SlonyStats(self.connections, parameters=params,
                          underscore=self.config['underscore'])

        stat.fetch(self.config['pg_version'])
        [self.publish(metric, value) for metric, value in stat]

        # Cleanup
        [conn.close() for conn in self.connections.itervalues()]

    def _connect(self, database=''):
        conn = psycopg2.connect(
            host=self.config['host'],
            user=self.config['user'],
            password=self.config['password'],
            port=self.config['port'],
            database=database)

        # Avoid using transactions, set isolation level to autocommit
        conn.set_isolation_level(0)
        return conn


class SlonyStats(QueryStats):
    """
    Slony replication stats
    """
    path = "slony.%(datname)s.%(metric)s.lag_events"
    multi_db = False
    query = """
        SELECT SUBSTRING(sl.no_comment FROM %s) AS node,
               st.st_lag_num_events AS lag_events
        FROM %s.sl_status AS st, %s.sl_node AS sl
        WHERE sl.no_id = st.st_received
    """
