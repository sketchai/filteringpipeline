import unittest
import logging

from src.filters.catalog_filter.pipeline_filter import PipelineFilter
from src.filters.catalog_source.source_list import SourceList
from tests.asset.mock.mock_abstract_filter import MockFilter
from src.utils.to_dict import yaml_to_dict

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def message_generator(l_data):
    return {'Source_A': {'parms': {'l_data': l_data}}}


class TestPipelineFilter(unittest.TestCase):

    @classmethod
    def setUp(self):
        conf_filter = yaml_to_dict('tests/asset/mock/mock_conf.yml')
        conf_filter['PipelineFilter']['sink'] = None

        conf = {'conf_filter': conf_filter,
                'catalog_filter': {'SourceList': SourceList,
                                   'MockFilter': MockFilter}}
        self.pipeline_filter = PipelineFilter(conf)

    def test_update_conf_pipeline(self):
        new_l = ['a', 'b', 'c']
        in_message = message_generator(new_l)
        updated_conf = self.pipeline_filter.update_conf_pipeline(in_message)
        self.assertListEqual(updated_conf.get('Source_A').get('parms').get('l_data'), new_l)

        # Health check
        # -- Check that the dict has not been fully override
        self.assertIn('type', updated_conf.get('Source_A').keys())

        # -- Check that the pipeline is still the same
        self.assertListEqual(updated_conf.get('PipelineFilter').get('l_filters'), ['MockFilter'])

    def test_process(self):
        l_out = []
        l_data = [0, 1, 2, 'a', 'b', 'c', 4, 5, 6]
        for i in range(3):
            in_message = message_generator(l_data[3 * i:(i + 1) * 3])
            out_message = self.pipeline_filter.process(in_message)
            l_out.append(out_message)

        self.assertListEqual(l_out, [{'END_SOURCE': 'True'}, {'END_SOURCE': 'True'}, {'END_SOURCE': 'True'}])
