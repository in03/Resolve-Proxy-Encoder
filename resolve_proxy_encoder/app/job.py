import glob
import logging
import os
from dataclasses import dataclass
from typing import Any, Union
from pathlib import Path

from settings.manager import SettingsManager

settings = SettingsManager()
logger = logging.getLogger(__name__)
logger.setLevel(settings["app"]["loglevel"])


@dataclass(frozen=True)
class SourceMetadata:

    clip_name: str
    file_name: str
    file_path: str
    duration: str
    resolution: list
    frames: int
    fps: float
    h_flip: bool
    v_flip: bool
    proxy_dir: str
    start: int
    end: int
    start_tc: str
    proxy_status: str
    proxy_media_path: str
    end_tc: str


@dataclass(frozen=True)
class ProjectMetadata:
    project_name: str
    timeline_name: str


# TODO: Considering making this singleton like SettingsManager
# then we can access media pool items from other modules

# Create a list of MediaPoolItems that stay in the queuer.
# We look up our items for post-encode link using file-name.
class MediaPoolRefs:
    def __init__(self):
        self.media_pool_items = dict()

    def add_ref(self, filename: str, media_pool_item: Any):
        """filename should reference source media media_pool_item belongs to"""
        if filename not in self.media_pool_items:
            if media_pool_item not in self.media_pool_items:
                self.media_pool_items.update({filename: media_pool_item})
                return

            raise ValueError(f"{media_pool_item} already registered!")
        raise ValueError(f"{filename} already registered!")

    def get_ref(self, filename: str):
        mpi = self.media_pool_items.get(filename)
        if mpi != None:
            return mpi

        raise ValueError(f"{filename} not registered!")


class Job:
    def __init__(
        self,
        project_metadata: ProjectMetadata,
        source_metadata: SourceMetadata,
        settings: SettingsManager,
    ):

        # Get data
        self.source_metadata = source_metadata
        self.project_metadata = project_metadata
        self.settings = settings

        # Get dynamic vars
        self.output_dir = self._get_output_dir()
        self.online = self._check_is_online()
        self.orphan = self._check_is_orphan()

        self.all_linkable_proxies = []
        self.newest_linkable_proxy = None
        self.safe_output_name = self.get_safe_output_name()

    # Private (cheap to init)
    def _check_is_online(self):
        """Parses Resolve's clip property 'Proxy'

        'Proxy' contains the resolution of the proxy if online,
        'Offline' if the source media is linked to an inaccessible proxy,
        'None' if the proxy is not linked.

        Returns:
            - False (bool) if not linked
            - None (NoneType) if offline
            - resolution (str) if online
        """
        status = self.source_metadata.proxy_status
        switch = {
            "Offline": None,
            "None": False,
        }
        # Status is resolution (depends on source-res) when online
        self.online = switch.get(status, True)

    def _check_is_orphan(self):
        if not self.online:
            # check if orphaned blah...

            self.orphan = False

    def _get_output_dir(self):
        p = Path(self.source_metadata.file_path)

        self.output_dir = os.path.normpath(
            os.path.join(
                settings["paths"]["proxy_path_root"],
                os.path.dirname(p.relative_to(*p.parts[:1])),
            )
        )

    # Public
    def _get_all_linkable_proxies(self):

        # Check for any file variants, including multiple extensions and suffixes
        glob_match_criteria = os.path.join(
            self.source_metadata.proxy_dir,
            self.source_metadata.file_name,
        )

        # Fetch paths of all possible variants of source filename
        self.all_linkable_proxies = glob.glob(glob_match_criteria + "*.*")

        if not self.all_linkable_proxies:
            logger.debug(
                f"[yellow]No existing proxies found for '{self.source_metadata.file_name}'\n"
            )
            self.all_linkable_proxies = []
            return

        # Sort matching proxy files by last modified
        self.all_linkable_proxies = sorted(
            self.all_linkable_proxies,
            key=os.path.getmtime,
            reverse=True,
        )

    def get_newest_linkable(self):

        if not self.all_linkable_proxies:
            self._get_all_linkable_proxies()

        if not self.all_linkable_proxies:
            return None

        self.newest_linkable_proxy = self.all_linkable_proxies[0]

    def get_safe_output_name(self):
        """Increment output filenames if necessary to prevent file collisions"""

        def _increment_file_if_exist(input_path: str, increment_num: int = 1) -> str:
            """Increment the filename in a given filepath if it already exists

            Calls itself recursively in case any incremented files already exist.
            It should get the latest increment, i.e. 'filename_4.mp4'
            if 'filename_3.mp4', 'filename_2.mp4', 'filename_1.mp4' and 'filename.mp4'
            already exist.

            Args:
                input_path(str): full filepath to check.

            Returns:
                output_path(str): a modified output path, incremented.

            Raises:
                none
            """

            # Split filename, extension
            file_name, file_ext = os.path.splitext(input_path)

            # If file exists...
            if os.path.exists(input_path):

                # Check if already incremented
                if file_name.endswith(f"_{increment_num}"):

                    # Increment again
                    increment_num += 1
                    _increment_file_if_exist(input_path, increment_num)

                else:
                    # First increment
                    file_name = file_name + "_1"

            return str(file_name + file_ext)

        if not self.all_linkable_proxies:
            pass

        if self.all_linkable_proxies:
            self.safe_output_name = _increment_file_if_exist(
                self.all_linkable_proxies[0]
            )
