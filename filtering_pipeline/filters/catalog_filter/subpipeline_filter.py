from typing import Dict
import logging
import copy

from ...pipe import SUB_PIPELINE
from ..abstract_filter import AbstractFilter
from ...factory import pipeline_factory
from ...utils.to_dict import update_nested_dict
from ... import END_SOURCE_PIPELINE, GENERAL_CONF_PIPELINE


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SubPipelineFilter(AbstractFilter):
    """
        A filter that exectutes a pipeline each time self.process() is called.
    """

    def __init__(self, conf: Dict = {}):
        super().__init__()

        self.conf_filter = conf.get('conf_filter')
        self.catalog_filter = conf.get('catalog_filter')
        self.source_name = self.conf_filter.get(GENERAL_CONF_PIPELINE).get('source')

    def update_conf_pipeline(self, message) -> object:
        raise NotImplementedError

    def _update_conf_pipeline(self, message) -> object:
        new_conf = copy.deepcopy(self.conf_filter)
        # Modify the source input based on the message content
        message_conf = self.update_conf_pipeline(message)
        return update_nested_dict(new_conf, message_conf)

    def process(self, message: object) -> object:
        logger.debug(f'-- {self.__class__.__name__}: new message received (message = {message})')
        # Create a new pipeline and update the source

        curr_pipeline_conf: Dict = self._update_conf_pipeline(message)  # Update some configuration pipeline
        pipeline = pipeline_factory(conf=curr_pipeline_conf, catalog_filter=self.catalog_filter, pipeline_type = SUB_PIPELINE)

        # Launch the pipeline
        pipeline_message, stop_message = pipeline.execute()
        message.update(stop_message)
        message.update(pipeline_message)
        logger.debug(f'-- {self.__class__.__name__}: processed message = {message} {pipeline_message}')

        del curr_pipeline_conf
        del pipeline


        return message

        

