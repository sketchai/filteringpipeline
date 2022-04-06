import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from filtering_pipeline import KO_FILTER_TAG
from sketchgraphs.data.sequence import EdgeOp, NodeOp
from src.filters.filter_count import FilterCount
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterCount(unittest.TestCase):

    def test_counter(self):
        # n <= max nodes, should not send KO
        n_nodes = 5
        self.mock_sequence_1 = [
            NodeOp(label=0),
            EdgeOp(label=1, references=None),
            EdgeOp(label=3, references=None),
            NodeOp(label=2),
            NodeOp(label=3),
            NodeOp(label=1),
            EdgeOp(label=2, references=None),
            EdgeOp(label=0, references=None),
            NodeOp(label=1),
            EdgeOp(label=2, references=None),
        ]
        conf_dict_1 = {'min': None, 'max': 5, 'type': 'node'}
        filter1 = FilterCount(conf_filter=conf_dict_1)
        for op in self.mock_sequence_1:
            message = {'op': op}
            filter1.process(message)
        self.assertEqual(filter1.count, n_nodes)
        answer = filter1.last_process(message)

        self.assertIsNone(answer.get(KO_FILTER_TAG))

        # n < min edges, should send KO
        self.mock_sequence_2 = [
            NodeOp(label=2),
            NodeOp(label=3),
            NodeOp(label=1),
            NodeOp(label=2),
            NodeOp(label=3),
            NodeOp(label=1),
            EdgeOp(label=2, references=None),
        ]
        conf_dict_2 = {'min': 4, 'max': None, 'type': 'edge'}
        filter2 = FilterCount(conf_filter=conf_dict_2)
        for op in self.mock_sequence_2:
            message = {'op': op}
            filter2.process(message)
        self.assertEqual(filter2.count, 1)
        answer = filter2.last_process(message)
        tag = answer.get(KO_FILTER_TAG)

        self.assertEqual(tag, filter2.name)
