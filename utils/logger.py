"""
Structured logger — writes to both console (rich) and rotating log files.
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

try:
    from rich.logging import RichHandler
    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False

LOG_DIR = Path(__file__).parent.parent / "logs"
LOG_DIR.mkdir(exist_ok=True)


def get_logger(name: str = "fashion_scraper") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    # File handler (rotating, max 5 MB × 3 files)
    fh = RotatingFileHandler(
        LOG_DIR / f"{name}.log",
        maxBytes=5 * 1024 * 1024,
        backupCount=3,
        encoding="utf-8",
    )
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
    )
    logger.addHandler(fh)

    # Console handler
    if _HAS_RICH:
        ch = RichHandler(rich_tracebacks=True, show_path=False)
    else:
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
    ch.setLevel(logging.INFO)
    logger.addHandler(ch)

    return logger


log = get_logger()
