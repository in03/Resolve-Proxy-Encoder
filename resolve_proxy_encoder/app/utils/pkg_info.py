import json
import logging
import os
import subprocess
from distutils.sysconfig import get_python_lib
from pathlib import Path
from typing import Union

import pkg_resources
import requests

from ...settings.manager import SettingsManager

settings = SettingsManager()
config = settings.user_settings

logger = logging.getLogger(__name__)


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

        logger.info("[cyan]Getting commit ID from package dist info.")

        dist = pkg_resources.get_distribution(package_name)
        vcs_metadata_file = dist.get_metadata("direct_url.json")
        vcs_metadata = json.loads(vcs_metadata_file)
        package_current_commit = vcs_metadata["vcs_info"]["commit_id"]

        if package_current_commit is None:
            raise TypeError("Couldn't get package last commit id")

        return package_current_commit.strip()

    except:

        logger.info("[yellow]Couldn't get package dist info, assuming git repo.")

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
        )
        return None


def get_remote_current_commit(github_url: str) -> Union[str, None]:
    """Attempt to find the the origin GitHub repo's latest commit SHA for the main branch.

    This currently only works with GitHub!

    Args:
        - github_url (str): The URL of the origin repo.

    Returns:
        - remote_current_commit (str): The latest commit's SHA.
        - None: on fail

    Raises:
        - TypeError: Caught by try/except; used to jump between blocks, readability.
    """

    url_list = github_url.split(".com")[1].split("/")
    api_endpoint = (
        f"https://api.github.com/repos/{url_list[1]}/{url_list[2]}/commits/main"
    )

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
    remote_current_commit = results["sha"]
    return remote_current_commit


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
