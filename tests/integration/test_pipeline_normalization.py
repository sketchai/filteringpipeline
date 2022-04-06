import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline')

import unittest
import logging

from experiments.experiment_normalization import ExperimentNormalization
from tests import MOCK_NORMALIZATION_PATH, MOCK_COARSE_PATH

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class TestExperimentNormalization(unittest.TestCase):

    def test_run_pipeline(self):
        experiment = ExperimentNormalization()
        experiment.d_conf['SourceFromFlatArray']['parms']['file_path'] = MOCK_COARSE_PATH
        experiment.d_conf['SinkSlices']['parms']['output_path'] = MOCK_NORMALIZATION_PATH

        experiment.run_pipeline()
