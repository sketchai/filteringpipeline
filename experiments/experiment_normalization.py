import argparse
import sys
import os
import logging

if __name__ == '__main__':
    # Add paths for packages
    sys.path.append('src/sketchgraphs/')
    sys.path.append('src/filtering-pipeline/')
    cur_path = os.path.abspath(os.path.dirname(__file__))
    sys.path.insert(0, cur_path + "/..")
    logging.basicConfig(level=logging.WARNING)
else:
    logging.basicConfig(level=logging.DEBUG)

from sketchgraphs.data import flat_array
from sketchgraphs.data.sequence import ConstraintType, EntityType, SubnodeType
from src.filters.filter_recenterline import FilterRecenterLine
from src.filters.filter_convertmetrics import FilterConvertMetrics
from src.filters.filter_divbymax import FilterDivByMax
from src.filters.filter_barycenter import FilterBarycenter
from src.filters.sink_slices import SinkSlices
from src.filters.filter_on_op import OpSubPipelineFilter
from src.filters.filter_moduloangle import FilterModuloAngle
from src.filters.utils.filter_log import FilterLog
from src.sources.source_fromlist import SourceList
from src.sources.source_fromflatarray import SourceFromFlatArray
from src.utils.to_dict import yaml_to_dict
from filtering_pipeline.factory import pipeline_factory
from filtering_pipeline.filters.catalog_filter.subpipeline_filter import SubPipelineFilter
from experiments import COARSE_PATH, NORMALIZATION_PATH
import numpy as np
import logging

logger = logging.getLogger()

class ExperimentNormalization():

    def __init__(self, dataset='train'):
        self.catalog_filters = {'SourceFromFlatArray': SourceFromFlatArray,
                                'OpSubPipelineFilter': OpSubPipelineFilter,
                                'SourceList': SourceList,
                                'FilterModuloAngle': FilterModuloAngle,
                                'FilterBarycenter': FilterBarycenter,
                                'FilterDivByMax': FilterDivByMax,
                                'FilterConvertMetrics': FilterConvertMetrics,
                                'FilterRecenterLine': FilterRecenterLine,
                                'SinkSlices': SinkSlices,
                                'FilterLog': FilterLog,
                                }
        self.d_conf = yaml_to_dict('config/conf_normalizationpip.yml')
        self.d_conf['FilterBarycenter_X']['parms']['request'] = {
            ('node', EntityType.Line): 'pntX',
            ('node', EntityType.Point): 'x',
            ('node', EntityType.Circle): 'xCenter',
            ('node', EntityType.Arc): 'xCenter',
        }
        self.d_conf['FilterBarycenter_Y']['parms']['request'] = {
            ('node', EntityType.Line): 'pntY',
            ('node', EntityType.Point): 'y',
            ('node', EntityType.Circle): 'yCenter',
            ('node', EntityType.Arc): 'yCenter',
        }

        self.d_conf['FilterDivByMax']['parms']['request'] = {
            ('node', EntityType.Point): ['x', 'y'],
            ('node', EntityType.Line): ['pntX', 'pntY', 'startParam', 'endParam'],
            ('node', EntityType.Circle): ['xCenter', 'yCenter', 'radius'],
            ('node', EntityType.Arc): ['xCenter', 'yCenter', 'radius'],
            ('edge', ConstraintType.Distance): 'length',
            ('edge', ConstraintType.Length): 'length',
            ('edge', ConstraintType.Diameter): 'length',
            ('edge', ConstraintType.Radius): 'length',
        }

        NB_RGX = r'[-+]?(?:\d*\.\d+|\d+)'

        self.d_conf['FilterConvertMetrics']['parms']['request'] = {
            ('edge', ConstraintType.Distance): {'length': {f'{NB_RGX} METER': 1., }},
            ('edge', ConstraintType.Length): {'length': {f'{NB_RGX} METER': 1.}},
            ('edge', ConstraintType.Diameter): {'length': {f'{NB_RGX} METER': 1.}},
            ('edge', ConstraintType.Radius): {'length': {f'{NB_RGX} METER': 1.}},
            ('edge', ConstraintType.Angle): {'angle': {f'{NB_RGX} DEGREE': np.pi / 180}},
            ('edge', ConstraintType.Angle): {'angle': {f'{NB_RGX} DEGREE': np.pi / 180}},
        }

        self.d_conf['FilterModuloAngle']['parms']['request'] = {
            ('node', EntityType.Arc): ["startParam", "endParam"],
            ('edge', ConstraintType.Angle): "angle",
        }

        self.d_conf['SourceFromFlatArray']['parms']['file_path'] = COARSE_PATH.format(dataset)
        self.d_conf['SinkSlices']['parms']['output_path'] = NORMALIZATION_PATH.format(dataset)


    def run_pipeline(self):
        pipeline = pipeline_factory(conf=self.d_conf, catalog_filter=self.catalog_filters)
        last_message = pipeline.execute()

        input_path = self.d_conf['SourceFromFlatArray']['parms']['file_path']
        input_data = flat_array.load_flat_array(input_path)
        output_path = self.d_conf['SinkSlices']['parms']['output_path']
        output_data = flat_array.load_flat_array(output_path)

        logger.info(f"Pipeline input is of length {len(input_data)}")
        logger.info(f"Pipeline output is of length {len(output_data)}")

        print(f"Pipeline input is of length {len(input_data)}")
        print(f"Pipeline output is of length {len(output_data)}")

def main():
    parser = argparse.ArgumentParser(description='Run Coarse pipeline')
    parser.add_argument('--dataset', help='train, validation or test')
    args = parser.parse_args()
    ExperimentNormalization(dataset=args.dataset).run_pipeline()

if __name__ == '__main__':
    main()