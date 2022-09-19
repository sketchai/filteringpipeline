import unittest
import logging

from tests.asset.mock.mock_abstract_filter import MockFilter
from filtering_pipeline import KO_FILTER_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestAbstractFilter(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.mock_filter = MockFilter(conf={'l_label': ['a', 'b', 'c']})

    def test_process(self):
        logger.debug('Test process')

        # Message 1 : {'a': 1, 'd': 2}
        message_in = {'a': 1, 'd': 2}
        message_out = self.mock_filter.process(message_in)
        self.assertDictEqual(message_out, {'a': 2, 'd': 2})
        self.assertDictEqual(self.mock_filter.label_counter, {'a': 1, 'b': 0, 'c': 0})

        # Message 2 : {'b': 1, 'c': 2}
        message_in = {'b': 1, 'c': 2}
        message_out = self.mock_filter.process(message_in)
        self.assertDictEqual(message_out, {'b': 1, 'c': 2, KO_FILTER_TAG: self.mock_filter.name})
        self.assertDictEqual(self.mock_filter.label_counter, {'a': 1, 'b': 0, 'c': 0})

    def test_last_process(self):
        logger.debug('Test last_process')

        # Message 1 : {'a': 1, 'd': 2}
        l_messages = [{'a': 1, 'd': 2}, {'b': 1, 'c': 2}]
        for message_in in l_messages:
            _ = self.mock_filter.process(message_in)

        last_message = {'a': 1, 'b': 1}
        message_out = self.mock_filter.last_process(last_message)
        self.assertDictEqual(message_out, {'a': 1, 'b': 1, 'count': {'a': 1, 'b': 0, 'c': 0}})
