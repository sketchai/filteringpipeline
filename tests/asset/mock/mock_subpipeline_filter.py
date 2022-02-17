from typing import Dict
import logging
import copy


from src.filters.catalog_filter.subpipeline_filter import SubPipelineFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class MockSubPipelineFilter(SubPipelineFilter):
    """
        A filter that exectutes a pipeline each time self.process() is called.
    """

    def update_conf_pipeline(self, message) -> object:
        return {self.source_name: {'parms': {'l_data': message}}}
