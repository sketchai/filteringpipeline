from typing import Dict
import logging
import json

from filtering_pipeline.filters.abstract_filter import AbstractFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class SinkDict(AbstractFilter):
    """
        A sink filter that saves a dictionnary into a json as a last process
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.output_path: str = conf_filter.get('output_path')
        self.input_tag: str = conf_filter.get('input_tag')

    def process(self, message: Dict) -> Dict:
        return message

    def last_process(self, message: Dict) -> Dict:
        dictionnary = message.get(self.input_tag)
        with open(self.output_path, 'w') as output_file:
            json.dump(dictionnary, output_file, indent=4)
        return message
