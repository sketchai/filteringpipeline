from typing import List, Dict
import logging

from .abstractpipeline import AbstractPipeline
from .. import END_SOURCE_PIPELINE, KO_FILTER_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()



class SubPipeline(AbstractPipeline):

    def __init__(self) -> None:
        super().__init__()
    

    def execute(self) -> Dict:
        while self._pipeline_status:
            message = self._source.process()
            logger.debug(f'message: {message}')
            if END_SOURCE_PIPELINE in message:  # No more data in the source
                self.stop_pipeline()
                del message[END_SOURCE_PIPELINE]
                break
            else:
                for filter in self._filters:
                    message = filter.process(message)
                    if KO_FILTER_TAG in message:
                        self.stop_pipeline()
                        return SubPipeline().construct_final_message(message)

                if self._pipeline_status :
                    self._process_sink(message)

        return self.generate_last_message(message)

    def generate_last_message(self, message: Dict = {}) -> Dict:
        for filter in self._filters:
            message = filter.last_process(message)
            if KO_FILTER_TAG in message:
                return SubPipeline().construct_final_message(message)
        self._process_sink(message)
        logger.debug(f'message : {message}')
        return message, {}

    @staticmethod
    def construct_final_message(message : Dict) -> [Dict, Dict]:
        message_status = {KO_FILTER_TAG : message.get(KO_FILTER_TAG)}
        del message[KO_FILTER_TAG]
        return {}, message_status