import unittest
import logging
import os
from collections import OrderedDict

from src.filters.catalog_source.source_list import SourceList
from tests.asset.mock.mock_abstract_filter import MockFilter
from tests.asset.mock.mock_sink import MockSinkFilter
from src.utils.to_dict import yaml_to_dict

from src.filters.factory import config_parser, pipeline_factory


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFactory(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.catalog_filters = {'SourceList': SourceList,
                                'MockFilter': MockFilter,
                                'MockSink': MockSinkFilter}
        self.d_conf = yaml_to_dict('tests/asset/mock/mock_conf.yml')
        self.output_path = self.d_conf.get('Sink_A').get('parms').get('output_path')

        self.clean = True

    def test_config_parser(self):
        source_config, filters_config, sink_config = config_parser(self.d_conf)

        self.assertDictEqual(source_config, {'type': 'SourceList', 'parms': {'l_data': [1, 8, 6, 9, 4]}})
        self.assertDictEqual(sink_config, {'type': 'MockSink', 'parms': {'output_path': 'tests/asset/out/my_data.txt'}})

        gt = OrderedDict()
        gt['MockFilter'] = {'type': 'MockFilter'}
        self.assertDictEqual(filters_config, gt)

    def test_pipeline_factory(self):
        pipeline = pipeline_factory(conf=self.d_conf, catalog_filter=self.catalog_filters)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True'})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file has been created at the end

    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.output_path):
            os.remove(self.output_path)
