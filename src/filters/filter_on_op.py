from typing import Dict
import logging
import copy


from filtering_pipeline.filters.catalog_filter.subpipeline_filter import SubPipelineFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class OpSubPipelineFilter(SubPipelineFilter):
    """
        A filter that exectutes a pipeline each time self.process() is called.
    """

    def update_conf_pipeline(self, message) -> object:
        return {self.source_name: {'parms': {'l_data': message.get('sequence')}}}

    def process(self, message: object) -> object:
        """Update the message to transmit the sequence"""
        sequence = message.get('sequence')
        message = super().process(message)
        message.update({'sequence': sequence})
        return message
