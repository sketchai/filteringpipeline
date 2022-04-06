import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from src.sources.source_fromdict import SourceDict
from sketchgraphs.data.sequence import NodeOp
import json
import os
import logging
import unittest
import tempfile


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSourceDict(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.clean = True
        self.dict_indexes = {
            "[4, 2, 8, 24, 27, 26, 10, 27, 25, 5]": [1, 2],
            "[4, 1, 6, 24, 27, 26, 7, 24, 28, 26, 1, 5]": [0],
        }
        self.indexes_path = tempfile.mkstemp()[1] # temp json
        with open(self.indexes_path, 'w') as jsonfile:
            json.dump(self.dict_indexes, jsonfile)

        self.data_path = 'tests/asset/sg_t16_mini.npy'
        

    def test_generator(self):
        # from python dict
        source = SourceDict({'indexes': self.dict_indexes, 'data': self.data_path})
        generator = source.generator()
        
        for message in generator:
            list_of_sequences = message['list_of_sequences']
            self.assertIsInstance(list_of_sequences, list)
            self.assertIsInstance(list_of_sequences[0], list)
            self.assertIsInstance(list_of_sequences[0][0], NodeOp)

        # from json path
        source = SourceDict({'indexes': self.indexes_path, 'data': self.data_path})
        generator = source.generator()
        
        for message in generator:
            list_of_sequences = message['list_of_sequences']
            self.assertIsInstance(list_of_sequences, list)
            self.assertIsInstance(list_of_sequences[0], list)
            self.assertIsInstance(list_of_sequences[0][0], NodeOp)

    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.indexes_path):
            os.remove(self.indexes_path)
