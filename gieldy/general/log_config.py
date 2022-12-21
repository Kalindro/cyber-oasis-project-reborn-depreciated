import sys
from typing import Optional

from loguru import logger


class ConfigureLoguru:

    def _basic_config(self, level: str, level_filter_only: Optional[str] = None):
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


def configure_logging(level: str):
    import logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(level)
    formatter = logging.Formatter("\033[92m%(asctime)s\033[0m | "
                                  "\033[1m%(levelname)-9s\033[0m | "
                                  "\033[1m%(message)-45s\033[0m | "
                                  "\033[94m%(funcName)s\033[0m | "
                                  "\033[95m%(filename)s:%(lineno)d\033[0m")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
