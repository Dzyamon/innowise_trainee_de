import logging


def logging_task1(log_level, text):
    """
    Set logging basic configuration and selecting logging type message (info, error, etc.) with time
    Args:
        log_level: int, 10-debug
                        20-info
                        30-warning
                        40-error
                        50-critical
        text: str, logging message
    """
    logging.basicConfig(
        level=log_level,                                     # logging level
        format='%(asctime)s - %(levelname)s - %(message)s',  # log message format
        filename='process.log',                              # log file name
        filemode='w')                                        # overwrite existing logs
    logging.log(log_level, text)
