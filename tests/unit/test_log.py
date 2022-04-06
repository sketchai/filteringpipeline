import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import logging
import unittest
import os

from src.filters.utils.filter_log import FilterLog

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class TestFilterLog(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.output_path = 'tests/asset/out/logfile'
        self.clean = True
        self.frequency = 5
        self.n_messages = 10

    def test_process(self):
        filter1 = FilterLog({
            'output_path': self.output_path,
            'frequency': self.frequency,
            'clean_up': self.clean})
        for _ in range(self.n_messages):
            filter1.process({})

        with open(filter1.log_file, 'r') as logfile:
            lines = logfile.readlines()
            self.assertEqual(len(lines), self.n_messages // self.frequency)

        _ = filter1.last_process({})

        if self.clean:
            self.assertFalse(os.path.exists(filter1.log_file))
