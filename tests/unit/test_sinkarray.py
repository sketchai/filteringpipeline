import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import logging
import unittest
import numpy as np
import os

from src.filters.sink_array import SinkArray

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestSinkArray(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.input_tag = 'weights'
        self.output_path = 'tests/asset/out/test_sinkarray_output.npy'
        self.clean = True

    def tearDown(self):
        # Clean and remove created files
        if self.clean and os.path.exists(self.output_path):
            os.remove(self.output_path)

    def test_last_process(self):
        filter1 = SinkArray({'input_tag': self.input_tag, 'output_path': self.output_path})
        lists = [[0.], [1., 2.], [3., 4., 5., 6.], [7., 8., 9.]]

        for l in lists:
            message = {self.input_tag: l}
            filter1.process(message)

        _ = filter1.last_process({})
        loaded_array = np.load(self.output_path, allow_pickle=False)
        self.assertIsNone(np.testing.assert_allclose(loaded_array, np.arange(10)))
