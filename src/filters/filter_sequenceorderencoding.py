from typing import Dict
import logging
from filtering_pipeline.filters.abstract_filter import AbstractFilter
from src import OPS_ENCODING_TAG, SEQUENCE_ENCODING_TAG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterSequenceOrderEncoding(AbstractFilter):
    """
        A filter that takes a list of op-level encodings and converts it into a sequence-level encoding
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__()
        self.name = 'FilterSequenceOrderEncoding'

    def process(self, message: object) -> object:
        sequence_encoding = message.get(OPS_ENCODING_TAG)
        message[SEQUENCE_ENCODING_TAG] = str(sequence_encoding)
        return message
