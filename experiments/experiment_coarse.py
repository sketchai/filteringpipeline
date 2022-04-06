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

# Initialization
import argparse
from src.filters.sink_slices import SinkSlices
from src.filters.filter_constraintrefs import FilterConstraintRefs
from src.filters.filter_count import FilterCount
from src.filters.filter_checkparamsmetrics import FilterCheckParamsMetrics
from src.filters.filter_on_op import OpSubPipelineFilter
from src.filters.filter_checklabel import FilterCheckLabel
from src.filters.utils.filter_log import FilterLog
from src.sources.source_fromlist import SourceList
from src.sources.source_fromflatarray import SourceFromFlatArray
from src.utils.to_dict import yaml_to_dict
from filtering_pipeline.factory import pipeline_factory
from sketchgraphs.data import flat_array
from sketchgraphs.data.sequence import ConstraintType, EntityType, SubnodeType
from experiments import SKETCHGRAPHS_PATH, COARSE_PATH, L_KEEP_EDGE, L_KEEP_NODE

logger = logging.getLogger()

class ExperimentCoarse():
    
    def __init__(self, dataset='train'):
        self.catalog_filters = {
            'SourceFromFlatArray': SourceFromFlatArray,
            'OpSubPipelineFilter': OpSubPipelineFilter,
            'FilterCheckLabel': FilterCheckLabel,
            'FilterCount': FilterCount,
            'SourceList': SourceList,
            'FilterConstraintRefs': FilterConstraintRefs,
            'FilterCheckParamsMetrics': FilterCheckParamsMetrics,
            'SinkSlices': SinkSlices,
            'FilterLog': FilterLog,
            }
        # Update conf
        # the nodes and edges that are considered
        l_keep_edge = L_KEEP_EDGE
        l_keep_node = L_KEEP_NODE

        self.d_conf = yaml_to_dict('config/conf_coarsegrainedpip.yml')
        self.d_conf['FilterCheckLabel']['parms']['edge_label_list'] = l_keep_edge
        self.d_conf['FilterCheckLabel']['parms']['node_label_list'] = l_keep_node
        length_format = {'length': r'[-+]?(?:\d*\.\d+|\d+) METER'}
        self.d_conf['FilterCheckParamsMetrics_Length']['parms']['request'] = {
            ('edge', ConstraintType.Distance): length_format,
            ('edge', ConstraintType.Length): length_format,
            ('edge', ConstraintType.Diameter): length_format,
            ('edge', ConstraintType.Radius): length_format,
        }

        self.d_conf['FilterCheckParamsMetrics_Angle']['parms']['request'] = {
            ('edge', ConstraintType.Angle): {'angle': r'[-+]?(?:\d*\.\d+|\d+) DEGREE'},
        }
        self.d_conf['SourceFromFlatArray']['parms']['file_path'] = SKETCHGRAPHS_PATH.format(dataset)
        self.d_conf['SinkSlices']['parms']['output_path'] = COARSE_PATH.format(dataset)

    def run_pipeline(self):
        pipeline = pipeline_factory(conf=self.d_conf, catalog_filter=self.catalog_filters)
        last_message = pipeline.execute()

        input_path = self.d_conf['SourceFromFlatArray']['parms']['file_path']
        input_data = flat_array.load_dictionary_flat(input_path)['sequences']
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
    ExperimentCoarse(dataset=args.dataset).run_pipeline()

if __name__ == '__main__':
    main()