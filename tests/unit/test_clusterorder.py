import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from src.filters.filter_clustersequences import FilterClusterSequences
from src import SEQUENCE_ENCODING_TAG, CLUSTER_DICT_TAG
import logging
import unittest

from sketchgraphs.data.sequence import NodeOp, EdgeOp

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterClusterSequences(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.filter1 = FilterClusterSequences()
        self.messages = [
            {
                'sequence': [NodeOp(0), EdgeOp(0,(0,)), NodeOp(1),],
                SEQUENCE_ENCODING_TAG: '[0,10,20,1]'
            },
            {
                'sequence': [NodeOp(1), EdgeOp(0,(0,)), NodeOp(1),],
                SEQUENCE_ENCODING_TAG: '[1,10,20,1]'
            },
            {
                'sequence': [NodeOp(0), EdgeOp(0,(0,)), NodeOp(1),],
                SEQUENCE_ENCODING_TAG: '[0,10,20,1]'
            }
        ]
    
    def test_process(self):
        
        self.filter1.process(self.messages[0])
        self.assertEqual(len(self.filter1.cluster_dict['[0,10,20,1]']),1)
        self.filter1.process(self.messages[1])
        self.assertEqual(len(self.filter1.cluster_dict['[1,10,20,1]']),1)
        self.filter1.process(self.messages[2])
        self.assertEqual(len(self.filter1.cluster_dict['[0,10,20,1]']),2)

    def test_last_process(self):
        message = {}
        for message in self.messages:
            self.filter1.process(message)

        answer = self.filter1.last_process(message)
        self.assertEqual(len(answer[CLUSTER_DICT_TAG].get('[0,10,20,1]')), 2)
        self.assertEqual(len(answer[CLUSTER_DICT_TAG].get('[1,10,20,1]')), 1)
        