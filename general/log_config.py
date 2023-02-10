import sys
import pandas as pd
from typing import Optional

from loguru import logger


class ConfigureLoguru:

    def _basic_config(self, level: str, level_filter_only: Optional[str] = None) -> logger:
        pd.set_option('display.max_rows', 0)
        pd.set_option('display.max_columns', 0)
        pd.set_option('display.width', 0)

        self.level_filter_only = level_filter_only
        logger.remove()
        formatted_format = ("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                            "<level>{level: <9}</level>| "
                            "<level>{message: <45}</level> | "
                            "<blue>{function}</blue> | "
                            "<magenta>{file}:{line}</magenta>")
        if level_filter_only:
            logger.add(sink=sys.stderr, level=level, format=formatted_format,
                       filter=self._log_level_filter)
        else:
            logger.add(sink=sys.stderr, level=level, format=formatted_format)

        logger.add(sink="errors.log", level="ERROR", format=formatted_format)
        return logger

    def _log_level_filter(self, record):
        return record["level"].name == self.level_filter_only

    def info_level(self):
        return self._basic_config(level="INFO")

    def debug_level(self):
        return self._basic_config(level="DEBUG")

    def error_level(self):
        return self._basic_config(level="ERROR")

    def debug_only(self):
        return self._basic_config(level="DEBUG", level_filter_only="DEBUG")

    def info_only(self):
        return self._basic_config(level="INFO", level_filter_only="INFO")