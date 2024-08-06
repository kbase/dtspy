# unit tests for the dts.client package

import dts
import os
import unittest

class TestClient(unittest.TestCase):
    """Unit tests for dts.client.Client"""

    def setUp(self):
        self.token = os.getenv('DTS_KBASE_DEV_TOKEN')
        if not self.token:
            raise ValueError('Environment variable DTS_KBASE_DEV_TOKEN must be set!')
        self.server = "https://lb-dts.staging.kbase.us"

    def test_ctor(self):
        client = dts.Client(api_key = self.token, server = self.server)
        self.assertTrue(client.uri)
        self.assertTrue(client.name)
        self.assertTrue(client.version)

    def test_connect(self):
        client = dts.Client()
        self.assertFalse(client.uri)
        self.assertFalse(client.name)
        self.assertFalse(client.version)
        client.connect(api_key = self.token, server = self.server)
        self.assertTrue(client.uri)
        self.assertTrue(client.name)
        self.assertTrue(client.version)
        client.disconnect()
        self.assertFalse(client.uri)
        self.assertFalse(client.name)
        self.assertFalse(client.version)

    def test_databases(self):
        client = dts.Client(api_key = self.token, server = self.server)
        dbs = client.databases()
        self.assertTrue(isinstance(dbs, list))
        self.assertEqual(2, len(dbs))
        self.assertTrue(any([db.id == 'jdp' for db in dbs]))
        self.assertTrue(any([db.id == 'kbase' for db in dbs]))

    def test_basic_jdp_search(self):
        client = dts.Client(api_key = self.token, server = self.server)
        results = client.search(database = 'jdp', query = '3300047546')
        self.assertTrue(isinstance(results, list))
        self.assertTrue(len(results) > 0)
        self.assertTrue(all([result.to_dict()['id'].startswith('JDP:')
                             for result in results]))

    def test_jdp_search_for_taxon_oid(self):
        client = dts.Client(api_key = self.token, server = self.server)
        taxon_oid = '2582580701'
        params = {'f': 'img_taxon_oid', 'extra': 'img_taxon_oid'}
        results = client.search(database = 'jdp',
                                query = taxon_oid,
                                specific = params)
        self.assertTrue(isinstance(results, list))
        self.assertTrue(len(results) > 0)
        self.assertTrue(any([result.to_dict()['extra']['img_taxon_oid'] == int(taxon_oid)
                             for result in results]))

    def test_fetch_jdp_metadata(self):
        client = dts.Client(api_key = self.token, server = self.server)
        resources = client.fetch_metadata(database = 'jdp',
                                          ids = ['JDP:6101cc0f2b1f2eeea564c978',
                                                 'JDP:613a7baa72d3a08c9a54b32d',
                                                 'JDP:61412246cc4ff44f36c8913d'])
        self.assertTrue(isinstance(resources, list))
        self.assertTrue(len(resources) == 3)
        resources = [r.to_dict() for r in resources]
        self.assertEqual('JDP:6101cc0f2b1f2eeea564c978', resources[0]['id'])
        self.assertEqual('JDP:613a7baa72d3a08c9a54b32d', resources[1]['id'])
        self.assertEqual('JDP:61412246cc4ff44f36c8913d', resources[2]['id'])

if __name__ == '__main__':
    unittest.main()
