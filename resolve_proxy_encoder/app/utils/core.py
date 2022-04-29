import logging
import sys
import time

from notifypy import Notify
from rich.logging import RichHandler
from rich.prompt import Prompt


def setup_rich_logging():

    """Set logger to rich, allowing for console markup."""

    FORMAT = "%(message)s"
    logging.basicConfig(
        level="WARNING",
        format=FORMAT,
        datefmt="[%X]",
        handlers=[
            RichHandler(
                rich_tracebacks=True,
                tracebacks_extra_lines=1,
                markup=True,
            )
        ],
    )


setup_rich_logging()
logger = logging.getLogger(__name__)


def install_rich_tracebacks(show_locals=False):
    """Install rich tracebacks"""
    from rich.traceback import install

    install(show_locals=show_locals)


def app_exit(level: int = 0, timeout: int = -1, cleanup_funcs: list = []):

    """
    Exit function to allow time to
    read console output if necessary before exit.
    Specify negative timeout to prompt exit instead of timeout.
    Provide a list of functions to call on cleanup if necessary.
    """

    # Run any cleanup functions
    if cleanup_funcs:

        for x in cleanup_funcs:
            if x is not None:
                x()

    if timeout < 0:
        answer = Prompt.ask("Press [yellow]ENTER[/] to exit")
        sys.exit(level)

    else:

        for i in range(timeout, -1, -1):

            time.sleep(1)
            sys.stdout.write(f"\rExiting in " + str(i))

        # Erase last line
        sys.stdout.write("\x1b[1A")
        sys.stdout.write("\x1b[2K")

    sys.exit(level)


def notify(message: str, title: str = "Resolve Proxy Encoder"):
    """Cross platform system notification

    Args:
        message(str): Message to display
        title(str): Title of notification

    Returns:
        True/False(bool): Success/Failure

    Raises:
        none

    """

    try:

        notification = Notify()
        notification.title = title
        notification.message = message
        notification.send(block=False)

    except Exception as e:
        logger.exception(f"[red] :warning: Couldn't send notification.[/]\n{e}")
        return False

    return True
