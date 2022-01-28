import unittest
import logging
import os


from src.filters.pipeline import Pipeline
from src.filters.catalog_source.source_list import SourceList

from tests.asset.mock.mock_abstract_filter import MockFilter
from tests.asset.mock.mock_sink import MockSinkFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestPipeline(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.output_path = 'tests/asset/out/my_data.txt'
        self.pipeline = Pipeline()
        self.filter_1 = MockFilter()
        self.sink = MockSinkFilter(conf={'output_path': self.output_path})
        self.source = SourceList(conf={'l_data': [1, 2, 5, 6]})

        self.clean = True

    def test_execute(self):
        # Test execution - Case 1: up to the end of the data source
        self.pipeline.add_source(self.source)
        self.pipeline.add_filter(self.filter_1)
        self.pipeline.add_sink(self.sink)
        last_message = self.pipeline.execute()

        self.assertDictEqual(last_message, {'END_SOURCE': 'True'})
        self.assertTrue(os.path.exists(self.output_path))  # Check that the file has been created at the end

    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.output_path):
            os.remove(self.output_path)
