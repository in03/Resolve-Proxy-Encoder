""" Helper functions for main module """
import json
import logging
import os
import subprocess
import sys
import time
from distutils.sysconfig import get_python_lib
from pathlib import Path
from typing import Union

import pkg_resources
import requests
from notifypy import Notify
from rich import print
from rich.logging import RichHandler
from rich.prompt import Prompt

from resolve_proxy_encoder import python_get_resolve
from resolve_proxy_encoder.settings.app_settings import Settings


settings = Settings()
config = settings.user_settings


class fragile(object):
    class Break(Exception):
        """Break out of a with statement"""

    def __init__(self, value):
        self.value = value

    def __enter__(self):
        return self.value.__enter__()

    def __exit__(self, etype, value, traceback):
        error = self.value.__exit__(etype, value, traceback)
        if etype == self.Break:
            return True
        return error


def get_rich_logger(loglevel: Union[int, str]):

    """Set logger to rich, allowing for console markup."""

    FORMAT = "%(message)s"
    logging.basicConfig(
        level=loglevel,
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
    logger = logging.getLogger("rich")
    return logger


logger = get_rich_logger("WARNING")


def install_rich_tracebacks(show_locals=False):
    """Install rich tracebacks"""
    from rich.traceback import install

    install(show_locals=show_locals)


def app_exit(level: int = 0, timeout: int = 0, cleanup_funcs: list = []):

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

    # Exit statement
    colour = "green" if level == 0 else "yellow"

    # Newline
    print()

    if timeout < 0:

        Prompt.ask(f"[{colour}]Press ENTER to exit[/]")
        sys.exit(level)

    if timeout == 0:
        sys.exit(level)

    for i in range(timeout, -1, -1):

        time.sleep(1)
        print(f"[{colour}]Exiting in {str(i)}[/]", end="\r")

    # Erase last line
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

    logger = get_rich_logger("WARNING")

    try:

        notification = Notify()
        notification.title = title
        notification.message = message
        notification.send(block=False)

    except Exception as e:
        logger.exception(f"[red] :warning: Couldn't send notification.[/]\n{e}")
        return False

    return True


def get_resolve_objects():
    """Return necessary Resolve objects with error handling"""

    logger = get_rich_logger("WARNING")

    try:

        resolve = python_get_resolve.GetResolve()
        if resolve is None:
            raise TypeError

    except:

        logger.warning(
            "[red] :warning: Couldn't access the Resolve Python API. Is DaVinci Resolve running?[/]",
            extra={"markup": True},
        )
        app_exit(1, -1)

    try:

        project = resolve.GetProjectManager().GetCurrentProject()
        if project is None:
            raise TypeError

    except:

        logger.warning(
            "[red] :warning: Couldn't get current project. Is a project open in Resolve?[/]",
            extra={"markup": True},
        )
        app_exit(1, -1)

    try:

        timeline = project.GetCurrentTimeline()
        if timeline is None:
            raise TypeError
    except:

        logger.warning(
            "[red] :warning: Couldn't get current timeline. Is a timeline open in Resolve?[/]",
            extra={"markup": True},
        )
        app_exit(1, -1)

    try:

        media_pool = project.GetMediaPool()
        if media_pool is None:
            raise TypeError

    except:

        logger.warning(
            "[red] :warning: Couldn't get Resolve's media pool.[/]",
            extra={"markup": True},
        )
        app_exit(1, -1)

    return dict(
        resolve=resolve,
        project=project,
        timeline=timeline,
        media_pool=media_pool,
    )


def resolve_network_path(local_path):
    """
    Resolve cross-platform network paths from local mapped mounts
    Args:
        - local_path (str): local path to resolve
        - must_exist (bool): if True, raise an error if the path doesn't exist
        - fail_returns_local_path (bool): return None for unresolvable path or re-return local_path

    """

    if os.path.exists(local_path):
        return local_path

    path_maps = config["paths"]["path_maps"]
    print(path_maps)
    return


def get_package_current_commit(package_name: str) -> Union[str, None]:
    """Attempt to find the current commit SHA from the locally installed package or the local GitHub repo.

    Args:
        - package_name (str): The name of the package to get last commit from.

    Returns:
        - latest_commit_id (str): The commit ID.
        - None: on fail

    Raises:
        - TypeError: Caught by try/except; used to jump between blocks, readability.
    """

    try:

        logger.info("Getting commit ID from package dist info.")

        dist = pkg_resources.get_distribution(package_name)
        vcs_metadata_file = dist.get_metadata("direct_url.json")
        vcs_metadata = json.loads(vcs_metadata_file)
        package_latest_commit = vcs_metadata["vcs_info"]["commit_id"]

        if package_latest_commit is None:
            raise TypeError("Couldn't get package last commit id")

        return package_latest_commit.strip()

    except:
        logger.info(
            "[yellow]Couldn't get package dist info, assuming git repo.[/]",
            extra={"markup": True},
        )

    try:
        logger.info("[cyan]Getting commit ID from git[/]")

        latest_commit_id = subprocess.check_output(
            'git --no-pager log -1 --format="%H"', stderr=subprocess.STDOUT, shell=True
        ).decode()

        if latest_commit_id is None:
            raise TypeError("Couldn't get git last commit id")

        return latest_commit_id.strip()

    except:

        logger.warning(
            f"[yellow]Couldn't get git info or package dist info!\n"
            + "Check properly cloned or installed[/]",
            extra={"markup": True},
        )
        return None


def get_remote_latest_commit(github_url: str) -> Union[str, None]:
    """Attempt to find the the origin GitHub repo's latest commit SHA for the main branch.

    This currently only works with GitHub!

    Args:
        - github_url (str): The URL of the origin repo.

    Returns:
        - remote_latest_commit (str): The latest commit's SHA.
        - None: on fail

    Raises:
        - TypeError: Caught by try/except; used to jump between blocks, readability.
    """

    if not "github.com" in github_url:
        logger.error(
            "[red]Currently only github URLs supported for update checking!"
            + "Please disable update checking in user settings.[/]"
        )
        app_exit(1, -1)

    try:

        url_list = github_url.split(".com")[1].split("/")
        api_endpoint = (
            f"https://api.github.com/repos/{url_list[1]}/{url_list[2]}/commits/main"
        )

    except IndexError:

        logger.error(f"[red]Invalid url passed! '{github_url}'[/]")
        return None

    try:

        r = requests.get(api_endpoint, timeout=8)
        if not str(r.status_code).startswith("2"):

            logger.warning(
                f"[red]Couldn't connect to GitHub API\n[/]"
                + f"[yellow]HTTP status code:[/] {r.status_code}\n\n"
            )
            return None

    except Exception as e:

        logger.error(f"[red]Couldn't connect to GitHub API:[/]\n{e}")
        return None

    results = r.json()
    remote_latest_commit = results["sha"]
    return remote_latest_commit


def get_script_from_package(script_name: str) -> Union[str, None]:
    """Get path to a named script in the current package

    Allows us to call scripts buried in a virtual env like pipx.
    Case insensitive.
    """

    package_dir = Path(get_python_lib()).resolve().parents[1]
    scripts_dir = os.path.join(package_dir, "Scripts")

    for x in os.listdir(scripts_dir):

        file_ = x.lower()
        if script_name.lower() in file_.lower():
            return os.path.abspath(os.path.join(scripts_dir, file_))

    return None
