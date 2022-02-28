from typing import Dict, List
from collections import OrderedDict
import copy

from .pipe import AbstractPipeline, Pipeline, SubPipeline, SUB_PIPELINE

from .filters.abstract_filter import SourceFilter, AbstractFilter
from . import GENERAL_CONF_PIPELINE

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def construct_conf_subpipeline(conf: Dict, l_filters: List) -> Dict:
    conf_subpipeline = {}
    l_filters_type = []
    for filter_name in l_filters:
        conf_filter = conf.get(filter_name)
        conf_subpipeline[filter_name] = conf_filter
        l_filters_type.append(conf_filter.get('type'))
    return conf_subpipeline, l_filters_type


def create_filter(conf: Dict, catalog_filter: Dict, conf_global: Dict = None) -> AbstractFilter:
    _type = conf.get('type')
    _parms = conf.get('parms')
    try:
        if 'SubPipelineFilter' in _type:
            l_all_filters = copy.deepcopy(_parms.get('l_filters'))
            l_all_filters.append(conf.get('source'))
            conf_subpipeline, l_filters_type = construct_conf_subpipeline(conf_global, l_all_filters)
            _parms['conf_filter'] = conf_subpipeline
            _parms['catalog_filter'] = {filter_type: catalog_filter[filter_type] for filter_type in l_filters_type}
            _parms['conf_filter'][GENERAL_CONF_PIPELINE] = {'source': conf.get('source'),
                                                            'l_filters': _parms.get('l_filters'),
                                                            'sink': conf.get('sink')}
        return catalog_filter[_type](_parms)
    except KeyError as e:
        logger.info(f'Filter type {_type} does not exist. {e}')


def config_parser(conf: Dict) -> object:
    general_config = conf.get(GENERAL_CONF_PIPELINE)

    # Create source config dict
    source_name = general_config.get('source')
    source_config = conf.get(source_name)

    # Create sink config dict
    sink_name = general_config.get('sink')
    sink_config = conf.get(sink_name)

    # Create l_filters (ordered) config dict
    l_filters_name = general_config.get('l_filters')
    filters_config = OrderedDict()
    for filter_name in l_filters_name:
        filters_config[filter_name] = conf.get(filter_name)

    return source_config, filters_config, sink_config


def pipeline_factory(conf: Dict, catalog_filter: Dict = None, pipeline_type : str = None) -> AbstractPipeline:
    # Parse the conf file and create the Pipeline
    source_config, filters_config, sink_config = config_parser(conf)
    if pipeline_type == SUB_PIPELINE :
        pipeline = SubPipeline()
    else :
        pipeline = Pipeline()

    # Add source
    source = create_filter(source_config, catalog_filter)
    pipeline.add_source(source)

    # Add filters
    for _, conf_filter in filters_config.items():
        _filter = create_filter(conf_filter, catalog_filter, conf)
        pipeline.add_filter(_filter)

    # Add sink
    if sink_config is not None:
        sink = create_filter(sink_config, catalog_filter)
        pipeline.add_sink(sink)

    return pipeline
