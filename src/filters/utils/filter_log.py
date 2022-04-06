import logging
import os
from typing import Dict
from datetime import datetime
from filtering_pipeline.filters.abstract_filter import AbstractFilter

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger()


class FilterLog(AbstractFilter):
    """
        A filter that logs the number of processed messages into a log file

        parms:
            output_path: str
            frequency: int (default=10000)
            clean_up: bool (default=True)
    """

    def __init__(self, conf_filter: Dict = {}):
        super().__init__(conf_filter)
        self.name = 'FilterLog'
        output_path : str = conf_filter.get('output_path')
        self.frequency : int = conf_filter.get('frequency', 10000)
        self.clean_up : bool = conf_filter.get('clean_up', True)
        self.n_messages = 0
        self.log_file = f'{output_path}_{datetime. now(). strftime("%Y_%m_%d-%I:%M:%S_%p")}'

    def _log(self):
        with open(self.log_file,'a') as logfile:
            logfile.write(f'Processed {self.n_messages} messages\n')

    def process(self, message: object) -> object:
        self.n_messages += 1
        if self.n_messages % self.frequency == 0:
            self._log()
        return message

    def last_process(self, message: Dict) -> Dict:
        if self.clean_up and os.path.isfile(self.log_file):
            os.remove(self.log_file)
        return message