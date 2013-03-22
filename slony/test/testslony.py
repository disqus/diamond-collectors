#!/usr/bin/python
# coding=utf-8
################################################################################

from test import CollectorTestCase
from test import get_collector_config

from slony import SlonyCollector


class TestSlonyCollector(CollectorTestCase):
    def setUp(self, allowed_names=None):
        if not allowed_names:
            allowed_names = []
        config = get_collector_config('SlonyCollector', {
        })
        self.collector = SlonyCollector(config, None)

    def test_import(self):
        self.assertTrue(SlonyCollector)
