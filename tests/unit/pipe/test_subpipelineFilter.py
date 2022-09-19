import unittest
import logging
import copy

from tests.asset.mock.mock_subpipeline_filter import MockSubPipelineFilter
from filtering_pipeline.filters.catalog_source.source_list import SourceList
from filtering_pipeline.utils.to_dict import yaml_to_dict

from tests.asset.mock.mock_abstract_filter import MockFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSubPipelineFilter(unittest.TestCase):

    @classmethod
    def setUp(self):
        conf_filter = yaml_to_dict('tests/asset/mock/mock_conf.yml')
        conf_filter['General']['sink'] = None

        conf = {'conf_filter': conf_filter,
                'catalog_filter': {'SourceList': SourceList,
                                   'MockFilter': MockFilter}}
        self.subpipeline_filter = MockSubPipelineFilter(conf)

    def test_update_conf_pipeline(self):
        in_message = {'data' : ['a', 'b', 'c']}
        updated_conf = self.subpipeline_filter._update_conf_pipeline(in_message)
        self.assertListEqual(updated_conf.get('MySource').get('parms').get('l_data'), in_message.get('data'))

        # Health check
        # -- Check that the dict has not been fully override
        self.assertIn('type', updated_conf.get('MySource').keys())

        # -- Check that the pipeline is still the same
        self.assertListEqual(updated_conf.get('General').get('l_filters'), ['MockFilter'])

    def test_process_1(self):
        l_out = []
        l_data = [{'a': 1}, {'a': 1, 'b': 1}, {'a': 2}, {'b': 2}]
        for i in range(2):
            out_message = self.subpipeline_filter.process({'data' : copy.deepcopy(l_data[i:(i + 3)])})
            l_out.append(out_message)
            logger.info(f'message: {out_message}')

        self.assertDictEqual(l_out[0], {'count': {'a': 3, 'b': 1, 'c': 0}, 'data': [{'a': 2}, {'a': 2, 'b': 1}, {'a': 3}]})
        self.assertDictEqual(l_out[1], {'count': {'a': 2, 'b': 2, 'c': 0}, 'data': [{'a': 2, 'b': 1}, {'a': 3}, {'b': 2}]})

    def test_process_2(self):
        l_out = []
        l_data = [{'a': 1}, {'a': 1, 'c': 1}, {'a': 2}, {'a': 3}]
        for i in range(2):
            in_message = {'data' : [copy.deepcopy(d) for d in l_data[i:(i + 3)]]}
            out_message = self.subpipeline_filter.process(in_message)
            l_out.append(out_message)
            logger.info(f'message: {out_message}')

        self.assertDictEqual(l_out[0], {'data': [{'a': 2}, {'a': 1, 'c': 1}, {'a': 2}], 'KO_FILTER': 'MockFilter'})
        self.assertDictEqual(l_out[1], {'data': [{'a': 1, 'c': 1}, {'a': 2}, {'a': 3}], 'KO_FILTER': 'MockFilter'})
