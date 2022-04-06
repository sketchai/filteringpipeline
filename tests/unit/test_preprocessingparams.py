import os
import sys

sys.path.append('src/sketchgraphs/')
sys.path.append('src/filtering-pipeline/')

import logging
import unittest
import pickle
from experiments import DOF_MAX, L_MAX, L_MIN, N_BINS
from experiments.preprocessing_params import export_parameters
from sketchgraphs.data.sequence import EntityType, ConstraintType, SubnodeType


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()

class TestPreprocessingParams(unittest.TestCase):

    @classmethod
    def setUp(self):
        self.output_path = 'tests/asset/out/mock_params.pkl'
        self.clean = True

    def test_preprocessing_params(self):

        export_parameters(fo=self.output_path)

        with open(self.output_path,'rb') as myfile:
            d_params = pickle.load(myfile)
        
        self.assertEqual(d_params['lMax'],L_MAX)
        self.assertEqual(d_params['lMin'],L_MIN)
        self.assertEqual(d_params['dof_max'],DOF_MAX)
        self.assertEqual(d_params['n_bins'],N_BINS)

        for key, d in d_params['node_feature_dimensions'].items():
            self.assertTrue(key in EntityType or key in SubnodeType)
            self.assertIsInstance(d, dict)
            for k,v in d.items():
                self.assertIsInstance(k,str)
                self.assertIsInstance(v,int)

        for key, d in d_params['edge_feature_dimensions'].items():
            self.assertIn(key, ConstraintType)
            self.assertIsInstance(d, dict)
            for k,v in d.items():
                self.assertIsInstance(k,str)
                self.assertIsInstance(v,int)
            
        for key, n in d_params['node_idx_map'].items():
            if key == 'void':
                pass
            else:
                self.assertTrue(key in EntityType or key in SubnodeType)
            self.assertIsInstance(n, int)
        
        for key, n in d_params['edge_idx_map'].items():
            self.assertIn(key, ConstraintType)
            self.assertIsInstance(n, int)

        self.assertEqual(d_params['padding_idx'], len(d_params['node_idx_map'])-1)
        

    def tearDown(self) -> None:
        if self.clean and os.path.isfile(self.output_path):
            os.remove(self.output_path)

