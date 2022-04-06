import argparse
import sys
import os
import logging

if __name__ == '__main__':
    sys.path.append('src/sketchgraphs/')
    sys.path.append('src/filtering-pipeline/')
    cur_path = os.path.abspath(os.path.dirname(__file__))
    sys.path.insert(0, cur_path + "/..")
    logging.basicConfig(level=logging.WARNING)
else:
    logging.basicConfig(level=logging.DEBUG)

from sketchgraphs.data import flat_array
from src.sources.source_fromflatarray import SourceFromFlatArray
from src.filters.filter_encodeedgefeatures import FilterEncodeEdgeFeatures
from src.filters.filter_encodenodefeatures import FilterEncodeNodeFeatures
from src.filters.filter_encodegraphconnections import FilterEncodeGraphConnections
from src.filters.sink_dictflat import SinkDictFlat
from src.filters.utils.filter_log import FilterLog
from src.utils.to_dict import yaml_to_dict
from filtering_pipeline.factory import pipeline_factory
from experiments import ENCODING_PATH, L_KEEP_EDGE, L_KEEP_NODE, NORMALIZATION_PATH

logger = logging.getLogger()

class ExperimentEncoding():

    def __init__(self, dataset='train'):
        self.catalog_filters = {
            'SourceFromFlatArray': SourceFromFlatArray,
            'FilterEncodeEdgeFeatures': FilterEncodeEdgeFeatures,
            'FilterEncodeNodeFeatures': FilterEncodeNodeFeatures,
            'FilterEncodeGraphConnections': FilterEncodeGraphConnections,
            'SinkDictFlat': SinkDictFlat,
            'FilterLog': FilterLog,
            }
        self.d_conf = yaml_to_dict('config/conf_encodingpip.yml')

        self.d_conf['FilterEncodeEdgeFeatures']['parms']['l_keep_edge'] = L_KEEP_EDGE
        self.d_conf['FilterEncodeNodeFeatures']['parms']['l_keep_node'] = L_KEEP_NODE
        
        self.d_conf['SourceFromFlatArray']['parms']['file_path'] = NORMALIZATION_PATH.format(dataset)
        self.d_conf['SinkDictFlat']['parms']['output_path'] = ENCODING_PATH.format(dataset)

    def run_pipeline(self):
        pipeline = pipeline_factory(conf=self.d_conf, catalog_filter=self.catalog_filters)
        last_message = pipeline.execute()

        logger.debug(f'Pipeline finished and returned {last_message}')

        input_path = self.d_conf['SourceFromFlatArray']['parms']['file_path']
        input_data = flat_array.load_flat_array(input_path)

        output_path = self.d_conf['SinkDictFlat']['parms']['output_path']
        output_data = flat_array.load_flat_array(output_path)

        logger.info(f'Pipeline input is of length {len(input_data)}')
        logger.info(f'Pipeline output is of length {len(output_data)}')

        print(f'Pipeline input is of length {len(input_data)}')
        print(f'Pipeline output is of length {len(output_data)}')


def main():
    parser = argparse.ArgumentParser(description='Run Weights pipeline')
    parser.add_argument('--dataset', help='train, validation or test')
    args = parser.parse_args()
    ExperimentEncoding(dataset=args.dataset).run_pipeline()

if __name__ == '__main__':
    main()