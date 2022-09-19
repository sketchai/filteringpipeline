import unittest
import logging
import os


from filtering_pipeline.pipe import Pipeline
from filtering_pipeline.filters.catalog_source.source_list import SourceList
from filtering_pipeline.filters.catalog_filter.subpipeline_filter import SubPipelineFilter


from tests.asset.mock.mock_abstract_filter import MockFilter
from tests.asset.mock.mock_sink import MockSinkFilter
from tests.asset.mock.mock_subpipeline_filter import MockSubPipelineFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestPipeline(unittest.TestCase):

    @classmethod
    def setUp(self):
        folder_path = "tests/asset/out"
        self.output_path = os.path.join(folder_path,'my_data.txt')
        os.makedirs(folder_path, exist_ok=True)
        self.clean = True

    def test_execute_1(self):
        logger.info('-- Test in normal condition')
        # Test 1 : No KO
        pipeline = Pipeline()
        filter_1 = MockFilter(conf={'l_label': ['a', 'b', 'c']})
        sink = MockSinkFilter(conf={'output_path': self.output_path})
        source = SourceList(conf={'l_data': [{'a': 1}, {'a': 1, 'b': 1}, {'a': 2}, {'a': 2}]})

        # Test execution - Case 1: up to the end of the data source
        pipeline.add_source(source)
        pipeline.add_filter(filter_1)
        pipeline.add_sink(sink)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True', 'count': {'a': 4, 'b': 1, 'c': 0}})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file has been created at the end

    def test_execute_2(self):
        logger.info('-- Test with a KO element')
        # Test 2 : KO
        pipeline = Pipeline()
        filter_1 = MockFilter(conf={'l_label': ['a', 'b', 'c']})
        sink = MockSinkFilter(conf={'output_path': self.output_path})
        source = SourceList(conf={'l_data': [{'a': 1}, {'a': 2, 'b': 4, 'c': 5}, {'a': 2}, {'a': 2}]})

        # Test execution - Case 1: up to the end of the data source
        pipeline.add_source(source)
        pipeline.add_filter(filter_1)
        pipeline.add_sink(sink)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True', 'count': {'a': 3, 'b': 0, 'c': 0}})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file hasn't been created at the end

    def test_execute_3(self):
        logger.info('-- Test with a PipelineFilter element')
        # Test 2 : KO
        pipeline = Pipeline()

        # Create Pipeline sub-filters
        conf = {
            'conf_filter': {'General': {'l_filters': ['MockFilter'], 'source': 'MySource'},
                            'MockFilter': {'type': 'MockFilter', 'parms': {'l_label': ['a', 'b', 'c']}},
                            'MySource': {'type': 'SourceList', 'parms': {'l_data': []}}},
            'catalog_filter': {'SourceList': SourceList,
                               'MockFilter': MockFilter}}
        pipelineSubFilter = MockSubPipelineFilter(conf)

        # Create Sink and sources
        sink = MockSinkFilter(conf={'output_path': self.output_path})
        source = SourceList(conf={'l_data': [{'data' : [{'a': 1}, {'a': 3, 'b': 4}, {'a': 3}]}, {'data' :[{'b': 1}, {'a': 4, 'b': 4}, {'b': 3}]}]})

        # Test execution - Case 1: up to the end of the data source
        pipeline.add_source(source)
        pipeline.add_filter(pipelineSubFilter)
        pipeline.add_sink(sink)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True'})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file has been created at the end

    def test_execute_4(self):
        logger.info('-- Test with a PipelineFilter element')
        # Test 2 : KO
        pipeline = Pipeline()

        # Create Pipeline sub-filters
        conf = {
            'conf_filter': {'General': {'l_filters': ['MockFilter'], 'source': 'MySource'},
                            'MockFilter': {'type': 'MockFilter', 'parms': {'l_label': ['a', 'b', 'c']}},
                            'MySource': {'type': 'SourceList', 'parms': {'l_data': []}}},
            'catalog_filter': {'SourceList': SourceList,
                               'MockFilter': MockFilter}}
        pipelineSubFilter = MockSubPipelineFilter(conf)

        # Create Sink and sources
        sink = MockSinkFilter(conf={'output_path': self.output_path})
        source = SourceList(conf={'l_data': [{'data' : [{'a': 1}, {'a': 3, 'b': 4}, {'c': 3}]}, {'data' : [{'b': 1}, {'a': 4, 'c': 4}, {'b': 3}]}]})

        # Test execution - Case 1: up to the end of the data source
        pipeline.add_source(source)
        pipeline.add_filter(pipelineSubFilter)
        pipeline.add_sink(sink)
        last_message = pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True'})
        self.assertFalse(os.path.exists(self.output_path))  # Check that the file hasn't been created at the end

    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.output_path):
            os.remove(self.output_path)
