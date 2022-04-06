import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from src.filters.filter_sequenceorderencoding import FilterSequenceOrderEncoding
from src import OPS_ENCODING_TAG, SEQUENCE_ENCODING_TAG
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterSequenceOrderEncoding(unittest.TestCase):

    def test_process(self):
        filter1 = FilterSequenceOrderEncoding()
        message = {OPS_ENCODING_TAG: [1, 0, 10, 100, 1, 0]}
        message = filter1.process(message)
        result = message[SEQUENCE_ENCODING_TAG]
        self.assertEqual(result, '[1, 0, 10, 100, 1, 0]')
