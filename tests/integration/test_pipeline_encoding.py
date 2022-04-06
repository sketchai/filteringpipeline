import sys
sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline')

import unittest
import logging
from tests import MOCK_ENCODING_PATH, MOCK_NORMALIZATION_PATH
from experiments.experiment_encoding import ExperimentEncoding

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class TestExperimentEncoding(unittest.TestCase):

    def test_run_pipeline(self):
        experiment = ExperimentEncoding()
        experiment.d_conf['SourceFromFlatArray']['parms']['file_path'] = MOCK_NORMALIZATION_PATH
        experiment.d_conf['SinkDictFlat']['parms']['output_path'] = MOCK_ENCODING_PATH
        experiment.run_pipeline()
