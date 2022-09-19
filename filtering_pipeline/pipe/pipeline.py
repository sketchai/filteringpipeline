from typing import List, Dict
import logging

from .abstractpipeline import AbstractPipeline
from .. import END_SOURCE_PIPELINE, KO_FILTER_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class Pipeline(AbstractPipeline):

    def __init__(self) -> None:
        super().__init__()


    def execute(self) -> Dict:
        while self._pipeline_status:
            message = self._source.process()
            logger.debug(f'message: {message}')
            if END_SOURCE_PIPELINE in message:  # No more data in the source
                self.stop_pipeline()
                break
            else:
                for filter in self._filters:
                    message = filter.process(message)
                    if KO_FILTER_TAG in message:
                        break
                if self._pipeline_status :
                    self._process_sink(message)

        return self.generate_last_message(message)

    def generate_last_message(self, message: Dict = {}) -> Dict:
        for filter in self._filters:
            message = filter.last_process(message)
            if KO_FILTER_TAG in message:
                break
        self._process_sink(message)
        logger.debug(f'message : {message}')
        return message
