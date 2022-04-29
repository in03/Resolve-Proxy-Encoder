import logging
import platform
import subprocess

from ..app.utils import core, pkg_info
from ..settings.manager import SettingsManager

core.install_rich_tracebacks()

config = SettingsManager()
logger = logging.getLogger(__name__)

logger = logging.getLogger()
logger.setLevel(config["worker"]["loglevel"])


def check_wsl() -> bool:
    """Return True if Python is running in WSL"""

    if platform.system() == "Linux":
        return "Microsoft" in platform.uname().release

    return False


def get_wsl_path(windows_path: str):
    """Convert windows host paths to WSL paths if running WSL"""

    try:

        wsl_path = subprocess.run(
            ["wslpath", windows_path], stdout=subprocess.PIPE
        ).stdout.decode("utf-8")
        if not wsl_path or wsl_path == None:
            return windows_path

    except:
        return windows_path

    return wsl_path


def get_queue():

    """Get Celery queue name (routing key) from package git commit short SHA

    Allows constraining tasks and workers to exact same version and prevent breaking changes.

    """

    if config["app"]["disable_version_constrain"]:

        logger.warning(
            "[yellow]Version constrain is disabled!\n"
            + "You [bold]must[/] ensure routing and version compatability yourself!"
        )

        return "celery"

    # TODO: `git_sha` slice returns as 5 characters, not standard 7
    # labels: bug

    # Use git standard 7 character short SHA
    return config["version_info"]["commit_short_sha"]
