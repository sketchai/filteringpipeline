from typing import List, Dict
import logging

from ..filters.abstract_filter import AbstractFilter, SourceFilter
from .. import END_SOURCE_PIPELINE, KO_FILTER_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class AbstractPipeline(object):

    def __init__(self) -> None:
        self._filters: List = []
        self._source: SourceFilter = None
        self._sink: AbstractFilter = None
        self._pipeline_status: bool = False

    def stop_pipeline(self) -> None :
        self._pipeline_status = False 

    def add_source(self, source: AbstractFilter) -> bool:
        if source is not None:
            self._source = source
            self._pipeline_status = True
            return True
        else:
            return False

    def add_sink(self, sink: AbstractFilter) -> bool:
        if sink is not None:
            self._sink = sink
            return True
        else:
            return False

    def add_filter(self, filter: AbstractFilter) -> bool:
        if filter is not None:
            self._filters.append(filter)
            return True
        else:
            return False

    def _process_sink(self, message: Dict) -> None:
        if self._sink is not None:
            if END_SOURCE_PIPELINE in message:
                self._sink.last_process(message)
            elif not KO_FILTER_TAG in message:
                self._sink.process(message)


    def execute(self) -> Dict:
        raise NotImplementedError('Needs to be implemented')

    def generate_last_message(self, message: Dict = {}) -> Dict:
        raise NotImplementedError('Needs to be implemented')