import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

from src.filters.sink_dict import SinkDict
import json
import os
import logging
import unittest


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSinkDict(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.input_tag = 'dict_to_dump'
        self.output_path = 'tests/asset/out/dictionnary.json'
        self.clean = True

        self.dict_to_dump = {
            '[1,2,3,4,5]': [1, 2, 3, 4],
            '[1,2,3]': [12,34,56]
        }

    def test_last_process(self):
        sink = SinkDict(conf_filter={'output_path': self.output_path, 'input_tag': self.input_tag})
        _ = sink.last_process(message={self.input_tag: self.dict_to_dump})

        with open(self.output_path, 'r') as json_file:
            loaded_dict = json.load(json_file)
        self.assertDictEqual(loaded_dict, self.dict_to_dump)
        
    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.output_path):
            os.remove(self.output_path)
