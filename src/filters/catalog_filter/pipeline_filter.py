from typing import Dict
import logging
import copy


from ..abstract_filter import AbstractFilter
from ..factory import pipeline_factory
from ...utils.to_dict import update_nested_dict

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class PipelineFilter(AbstractFilter):
    """
        A filter that exectutes a pipeline each time self.process() is called.
    """

    def __init__(self, conf: Dict = {}):
        super().__init__()

        self.conf_filter = conf.get('conf_filter')
        self.catalog_filter = conf.get('catalog_filter')

    def update_conf_pipeline(self, message) -> object:
        new_conf = copy.deepcopy(self.conf_filter)
        # Modify the source input based on the message content
        return update_nested_dict(new_conf, message)

    def process(self, message: object) -> object:
        logger.debug(f'-- PipelineFilter: new message received (message = {message}')
        # Create a new pipeline and update the source
        curr_pipeline_conf: Dict = self.update_conf_pipeline(message)  # Update some configuration pipeline
        pipeline = pipeline_factory(conf=curr_pipeline_conf, catalog_filter=self.catalog_filter)

        # Launch the pipeline
        message = pipeline.execute()
        logger.debug(f'-- PipelineFilter: processed message = {message}')

        del curr_pipeline_conf
        del pipeline
        return message
