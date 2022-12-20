import sys


def configure_logging():
    from loguru import logger
    logger.remove()
    formatted_format = ("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                        "<level>{level: <9}</level>| "
                        "<level>{message: <45}</level> | "
                        "<blue>{function}</blue> | "
                        "<magenta>{file}:{line}</magenta>")
    logger.add(sink=sys.stderr, format=formatted_format)
