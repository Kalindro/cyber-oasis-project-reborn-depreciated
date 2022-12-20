import sys


def configure_loguru(level: str):
    from loguru import logger
    logger.remove()
    formatted_format = ("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                        "<level>{level: <9}</level>| "
                        "<level>{message: <45}</level> | "
                        "<blue>{function}</blue> | "
                        "<magenta>{file}:{line}</magenta>")
    logger.add(sink=sys.stderr, level=level, format=formatted_format)
    logger.add(sink="errors.log", level="ERROR", format=formatted_format)


def configure_logging(level):
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
