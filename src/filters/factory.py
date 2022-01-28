from typing import Dict, List
from collections import OrderedDict

from src.filters.pipeline import Pipeline
from src.filters.abstract_filter import SourceFilter, AbstractFilter

import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


def create_filter(conf: Dict, catalog_filter: Dict) -> AbstractFilter:
    _type = conf.get('type')
    _parms = conf.get('parms')
    try:
        return catalog_filter[_type](_parms)
    except KeyError:
        logger.info(f'Filter type {_type} does not exist.')


def config_parser(conf: Dict) -> object:
    general_config = conf.get('PipelineFilter')

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


def pipeline_factory(conf: Dict, catalog_filter: Dict = None) -> Pipeline:
    if catalog_filter is None:
        from src.filters import CATALOG_FILTERS
        catalog_filter = CATALOG_FILTERS
    # Parse the conf file and create the Pipeline
    source_config, filters_config, sink_config = config_parser(conf)
    pipeline = Pipeline()

    # Add source
    source = create_filter(source_config, catalog_filter)
    pipeline.add_source(source)

    # Add filters
    for _, conf_filter in filters_config.items():
        _filter = create_filter(conf_filter, catalog_filter)
        pipeline.add_filter(_filter)

    # Add sink
    if sink_config is not None:
        sink = create_filter(sink_config, catalog_filter)
        pipeline.add_sink(sink)

    return pipeline
