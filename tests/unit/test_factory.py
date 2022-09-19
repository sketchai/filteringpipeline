import unittest
import logging
import os
import copy
from collections import OrderedDict
from typing import Dict, List

from tests.asset.mock.mock_subpipeline_filter import MockSubPipelineFilter
from tests.asset.mock.mock_abstract_filter import MockFilter
from tests.asset.mock.mock_sink import MockSinkFilter

from filtering_pipeline.filters.catalog_source.source_list import SourceList
from filtering_pipeline.utils.to_dict import yaml_to_dict
from filtering_pipeline.factory import config_parser, pipeline_factory



logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFactory(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.catalog_filters = {'SourceList': SourceList,
                                'MockFilter': MockFilter,
                                'MockSink': MockSinkFilter,
                                'MockSubPipelineFilter': MockSubPipelineFilter}
        self.d_conf = yaml_to_dict('tests/asset/mock/mock_conf.yml')
        self.output_path = self.d_conf.get('Sink_A').get('parms').get('output_path')

        self.clean = True

    def test_config_parser(self):
        source_config, filters_config, sink_config = config_parser(self.d_conf)

        self.assertDictEqual(source_config, {'type': 'SourceList', 'parms': {'l_data': [{'a': 1}, {'a': 2, 'b': 4, 'c': 5}, {'a': 2}, {'a': 2}]}})
        self.assertDictEqual(sink_config, {'type': 'MockSink', 'parms': {'output_path': 'tests/asset/out/my_data.txt'}})

        gt = OrderedDict()
        gt['MockFilter'] = {'type': 'MockFilter', 'parms': {'l_label': ['a', 'b', 'c']}}
        self.assertDictEqual(filters_config, gt)

    def test_pipeline_factory_1(self):
        # Test 1 : No KO PIPELINE TAG
        conf = copy.deepcopy(self.d_conf)
        conf['MySource']['parms']['l_data'] = [{'a': 1}, {'a': 2, 'b': 4}, {'a': 5}, {'a': 6}]

        pipeline = pipeline_factory(conf=conf, catalog_filter=self.catalog_filters)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True', 'count': {'a': 4, 'b': 1, 'c': 0}})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file has been created at the end

        # Test 2 : KO PIPELINE TAG
        conf = copy.deepcopy(self.d_conf)
        conf['MySource']['parms']['l_data'] = [{'a': 1}, {'a': 2, 'b': 4, 'c': 5}, {'a': 2}, {'a': 2}]

        pipeline = pipeline_factory(conf=conf, catalog_filter=self.catalog_filters)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True', 'count': {'a': 3, 'b': 0, 'c': 0}})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file has been created at the end

    def test_pf_subpipeline(self):
        d_conf = yaml_to_dict('tests/asset/mock/mock_conf_subpipeline.yml')
        d_conf['Source_A']['parms']['l_data'] = [{'data' : [{'a' : 1}, {'a' : 1, 'b' : 1}, {'a' : 1}]}, {'data' :[{'b' : 1}, {'a' : 1, 'b' : 1}, {'b' : 1}]}]
        pipeline = pipeline_factory(conf=d_conf, catalog_filter=self.catalog_filters)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True'})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file has been created at the end

    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.output_path):
            os.remove(self.output_path)
