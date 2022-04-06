import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline')

import unittest
import logging

from experiments.experiment_coarse import ExperimentCoarse
from tests import MOCK_COARSE_PATH, PATH_TO_MINI_SEQUENCE_DATA

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class TestExperimentCoarse(unittest.TestCase):

    def test_run_pipeline(self):
        experiment = ExperimentCoarse()
        experiment.d_conf['SourceFromFlatArray']['parms']['file_path'] = PATH_TO_MINI_SEQUENCE_DATA
        experiment.d_conf['SinkSlices']['parms']['output_path'] = MOCK_COARSE_PATH

        experiment.run_pipeline()
