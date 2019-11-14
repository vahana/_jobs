# from pyramid.paster import bootstrap
from os import environ
from random import randint
from unittest import TestCase

from pymongo import MongoClient
import webtest
import mock

from pyramid import testing
from pyramid.paster import get_appsettings

from slovar.strings import split_strip
from slovar import slovar


class PyramidTestCase(TestCase):
    def setUp(self):
        test_ini_file = environ.get('INI_FILE', 'test.ini')
        self.settings = get_appsettings(test_ini_file, name='main')
        self.c = MongoClient(
            self.settings.get('mongodb.host', 'localhost'),
            self.settings.get('mongodb.port', 27017),
        )
        if self.settings.get('mongodb.db'):
            self.c.drop_database(self.settings.get('mongodb.db'))
        for nsdb in split_strip(self.settings.get('dataset.namespaces', '')):
            if nsdb:
                self.c.drop_database(nsdb)

        self.request = testing.DummyRequest()
        self.config = testing.setUp(
            request=self.request, settings=self.settings)
        self.config.include('prf')
        self.config.include('prf.mongodb')
        self.config.include('jobs')
        self._setup(self.config)

        patcher = mock.patch('standards.includeme', lambda c: slovar())
        patcher.start()

    def tearDown(self):
        if self.settings.get('mongodb.db'):
            self.c.drop_database(self.settings.get('mongodb.db'))
        self.config = testing.tearDown()
        self._teardown()

    def _setup(self, config):
        """Override this function to use the parent's setUp"""
        pass

    def _teardown(self):
        """Override this function to use the parent's tearDown"""
        pass


class FunctionalTestCase(PyramidTestCase):
    """
    Runs functional tests
    """
    def setUp(self):
        PyramidTestCase.setUp(self)

        from jobs import main
        app = main({}, **self.settings)
        self.testapp = webtest.TestApp(app)

    def tearDown(self):
        super(PyramidTestCase, self).tearDown()
