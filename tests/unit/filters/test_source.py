import unittest
import logging

from src.filters import END_SOURCE_PIPELINE
from src.filters.catalog_source.source_list import SourceList

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSource(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.l_data = [1, 4, 5, 6]
        self.mock_source = SourceList(conf={'l_data': self.l_data})

    def test_generator(self):
        # Test the generator - classic
        gen = self.mock_source.generator()
        out = []
        while True:
            try:
                m = next(gen)
                out.append(m.get('data'))
            except StopIteration:
                logger.debug('Stop generator')
                break
        self.assertListEqual(out, self.l_data)

    def test_process(self):
        # Test process
        out = []
        message = self.mock_source.process()

        while END_SOURCE_PIPELINE not in message:
            out.append(message.get('data'))
            message = self.mock_source.process()

        self.assertListEqual(out, self.l_data)
