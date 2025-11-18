import logging
import os
import sys
import traceback
import copy
from typing import Optional, Callable, Dict

from colors import green, yellow, red, blue, magenta, italic

# Type alias for your color/format helper callables (e.g., green("text"))
ColorFn = Callable[[str], str]

# Globals
global_root_logger: Optional[logging.Logger] = None
global_default_log_level: int = logging.INFO
global_has_setup_handlers: bool = False
disable_ansi_colors: bool = False


def get_logger(component_name: str) -> logging.Logger:
    """
    Get a component-level logger (e.g. "flexaura.api").
    """
    global global_root_logger
    if not global_root_logger:
        create_global_logger()
    # mypy: getChild returns Logger
    sub_logger: logging.Logger = global_root_logger.getChild(component_name)  # type: ignore[union-attr]
    sub_logger.setLevel(global_default_log_level)
    return sub_logger


def create_global_logger(default_log_level: Optional[int] = None) -> logging.Logger:
    """
    Initialize the root logger for the application.
    """
    global global_root_logger, global_default_log_level

    default_log_level = determine_log_level(default_log_level)
    global_default_log_level = default_log_level
    setup_log_handlers()
    set_log_levels()
    # After set_log_levels, global_root_logger is guaranteed non-None
    assert global_root_logger is not None
    global_root_logger.info(
        f"Initialized logger with level {logging.getLevelName(global_default_log_level)}"
    )
    return global_root_logger


def determine_log_level(default_log_level: Optional[int]) -> int:
    """
    Determine log level from arg or DEFAULT_LOG_LEVEL env var.
    """
    if default_log_level is None:
        env_val = os.getenv("DEFAULT_LOG_LEVEL")
        if env_val is None:
            default_log_level = logging.INFO
        else:
            try:
                default_log_level = int(env_val)
            except ValueError:
                default_log_level = logging.INFO
    os.environ["DEFAULT_LOG_LEVEL"] = str(default_log_level)
    return default_log_level


def setup_log_handlers() -> None:
    """
    Set up local log handler once.
    """
    global global_has_setup_handlers

    if global_has_setup_handlers:
        return

    logging.getLogger().addHandler(LocalLogHandler())
    global_has_setup_handlers = True


def set_log_levels() -> None:
    """
    Set log levels globally.
    """
    global global_root_logger
    logging.getLogger().setLevel(global_default_log_level)

    global_root_logger = logging.getLogger("flexaura")
    global_root_logger.setLevel(global_default_log_level)

    logging.getLogger("werkzeug").setLevel(global_default_log_level)
    logging.getLogger("twilio").setLevel(logging.WARNING)


def get_local_log_message_format(level: Optional[int] = None) -> str:
    level_color: ColorFn = green
    if level is not None:
        if level >= logging.ERROR:
            level_color = red
        elif level >= logging.WARNING:
            level_color = yellow

    level_fmt = "%(levelname)s" if disable_ansi_colors else italic(level_color("%(levelname)s"))
    time_fmt = "%(asctime)s.%(msecs)03d" if disable_ansi_colors else italic(blue("%(asctime)s.%(msecs)03d"))
    line_fmt = "%(filename)s:%(lineno)d" if disable_ansi_colors else italic(magenta("%(filename)s:%(lineno)d"))
    msg_fmt = "%(message)s" if disable_ansi_colors else level_color("%(message)s")

    return f"{level_fmt} {time_fmt} {line_fmt}: {msg_fmt}"


def get_local_time_format() -> str:
    return "%H:%M:%S"


class LocalLogHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.formatters: Dict[int, logging.Formatter] = {
            logging.INFO: logging.Formatter(get_local_log_message_format(logging.INFO), get_local_time_format()),
            logging.WARNING: logging.Formatter(get_local_log_message_format(logging.WARNING), get_local_time_format()),
            logging.ERROR: logging.Formatter(get_local_log_message_format(logging.ERROR), get_local_time_format()),
        }

    def emit(self, record: logging.LogRecord) -> None:
        # Use a shallow copy so we can safely adjust fields
        record = copy.copy(record)
        record.filename = record.filename.rjust(30)

        if record.levelno >= logging.ERROR:
            self.setFormatter(self.formatters[logging.ERROR])
            sys.stderr.write(self.format(record) + "\n")
        elif record.levelno >= logging.WARNING:
            self.setFormatter(self.formatters[logging.WARNING])
            sys.stderr.write(self.format(record) + "\n")
            if record.exc_info:
                # record.exc_info is a tuple: (exc_type, exc_value, exc_tb)
                exc = record.exc_info[1]
                if exc is not None:
                    traceback.print_exception(exc, file=sys.stderr)  # type: ignore[arg-type]
        else:
            self.setFormatter(self.formatters[logging.INFO])
            sys.stdout.write(self.format(record) + "\n")