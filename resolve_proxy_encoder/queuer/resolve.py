import imp
import logging
import os
import pathlib
import sys

from rich import print

from ..app.job import Job, MediaPoolRefs, ProjectMetadata, SourceMetadata
from ..app.utils import core
from ..settings.manager import SettingsManager

settings = SettingsManager()

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)
logger.setLevel(settings["app"]["loglevel"])


class ResolveObjects:
    def __init__(self):
        self._populate_variables()

    def _get_resolve(self):

        ext = ".so"
        if sys.platform.startswith("darwin"):
            path = "/Applications/DaVinci Resolve/DaVinci Resolve.app/Contents/Libraries/Fusion/"
        elif sys.platform.startswith("win") or sys.platform.startswith("cygwin"):
            path = "C:\\Program Files\\Blackmagic Design\\DaVinci Resolve\\"
            ext = ".dll"
        elif sys.platform.startswith("linux"):
            path = "/opt/resolve/libs/Fusion/"
        else:
            raise Exception("Unsupported system! " + sys.platform)

        bmd = imp.load_dynamic("fusionscript", path + "fusionscript" + ext)
        resolve = bmd.scriptapp("Resolve")

        if not resolve:
            return None

        try:
            sys.modules[__name__] = resolve
        except ImportError:
            return None

        return resolve

    def _populate_variables(self):

        try:

            self.resolve = self._get_resolve()
            if self.resolve is None:
                raise TypeError

        except:

            logger.critical(
                "[red] :warning: Couldn't access the Resolve Python API. Is DaVinci Resolve running?[/]"
            )
            core.app_exit(1, -1)

        try:

            self.project = self.resolve.GetProjectManager().GetCurrentProject()
            if self.project is None:
                raise TypeError

        except:

            logger.critical(
                "[red] :warning: Couldn't get current project. Is a project open in Resolve?[/]"
            )
            core.app_exit(1, -1)

        try:

            self.timeline = self.project.GetCurrentTimeline()
            if self.timeline is None:
                raise TypeError
        except:

            logger.critical(
                "[red] :warning: Couldn't get current timeline. Is a timeline open in Resolve?[/]"
            )
            core.app_exit(1, -1)

        try:

            self.media_pool = self.project.GetMediaPool()
            if self.media_pool is None:
                raise TypeError

        except:

            logger.critical("[red] :warning: Couldn't get Resolve's media pool.[/]")
            core.app_exit(1, -1)


def get_video_track_items(timeline):
    """Get all video track items from the provided timeline"""

    all_track_items = []

    # Get count of tracks (index) in active timeline
    track_len = timeline.GetTrackCount("video")
    logger.info(f"[green]Video track count: {track_len}[/]")

    # For each track in timeline (using index)
    for i in range(1, track_len + 1):

        # Get items
        track_items = timeline.GetItemListInTrack("video", i)

        if track_items is None:
            logger.debug(f"[magenta]No items found in track {i}[/]")
            continue

        else:
            all_track_items.append(track_items)

    return all_track_items


def get_media_pool_items(track_items):
    """Return media pool items for all track items"""

    all_media_pool_items = []

    for track in track_items:
        for item in track:
            media_item = item.GetMediaPoolItem()
            all_media_pool_items.append(media_item)

    return all_media_pool_items


def get_resolve_timelines(project, active_timeline_first=True):
    """Return a list of all Resolve timeline objects in current project."""

    timelines = []

    timeline_len = project.GetTimelineCount()
    if timeline_len > 0:

        for i in range(1, timeline_len + 1):
            timeline = project.GetTimelineByIndex(i)
            timelines.append(timeline)

        if active_timeline_first:
            active = project.GetCurrentTimeline().GetName()  # Get active timeline
            timeline_names = [x.GetName() for x in timelines]
            active_i = timeline_names.index(
                active
            )  # It's already in the list, find it's index
            timelines.insert(
                0, timelines.pop(active_i)
            )  # Move it to the front, indexing should be the same as name list
    else:
        return False

    return timelines


def get_resolve_proxy_jobs(media_pool_items):
    """Return source metadata for each media pool item that passes configured criteria.

    each media pool item must meet the following criteria:
        - return valid clip properties (needed for encoding, internal track items don't have them)
        - whitelisted extension (e.g, BRAW performs fine without proxies)
        - whitelisted framerate (optional) FFmpeg should handle most

    Args:
        - media_pool_items: list of Resolve API media pool items

    Returns:
        - filtered_metadata: a list of dictionaries containing clip attributes for proxy-encodable Resolve media.

    Raises:
        - none

    """

    jobs = []
    seen = []
    mpi_refs = MediaPoolRefs()

    for media_pool_item in media_pool_items:

        # Check media pool item is valid, get UUID
        try:

            mpi_uuid = str(media_pool_item).split("UUID:")[1].split("]")[0]
            logger.debug(f"[magenta]Media Pool Item: {mpi_uuid}")

        except:

            logger.debug(
                f"[magenta]Media Pool Item: 'None'[/]\n"
                + f"[yellow]Invalid item: has no UUID[/]\n"
            )
            continue

        if str(media_pool_item) in seen:

            logger.debug(
                f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                + "[yellow]Already seen media pool item. Skipping...[/]\n"
            )
            continue

        else:

            # Add first encounter to list for comparison
            seen.append(str(media_pool_item))

        # Check media pool item has clip properties
        if not hasattr(media_pool_item, "GetClipProperty()"):

            logger.debug(
                f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                + "[yellow]Media pool item has no clip properties. Skipping...[/]\n"
            )
            continue

        # Get source metadata, path, extension
        clip_properties = media_pool_item.GetClipProperty()
        source_path = clip_properties["File Path"]
        source_ext = os.path.splitext(source_path)[1].lower()

        # Might still get media that has clip properties, but empty attributes
        # Should only be internally generated media that returns this way
        if source_path is "":

            logger.debug(
                f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                + f"clip properties did not return a valid filepath. Skipping...\n"
            )
            continue

        # Filter extension
        if settings["filters"]["extension_whitelist"]:

            if source_ext not in settings["filters"]["extension_whitelist"]:

                logger.warning(
                    f"[yellow]Ignoring file with extension not in whitelist: '{source_ext}'\n"
                    + f"from '{clip_properties['File Path']}'[/]\n"
                )
                continue

        # Filter framerate
        if settings["filters"]["framerate_whitelist"]:

            # Make int to avoid awkward extra zeros.
            if float(clip_properties["FPS"]).is_integer():
                clip_properties["FPS"] = int(float(clip_properties["FPS"]))

            if clip_properties["FPS"] not in settings["filters"]["framerate_whitelist"]:

                logger.warning(
                    f"[yellow]Ignoring file with framerate not in whitelist: '{clip_properties['FPS']}'\n"
                    + f"from '{clip_properties['File Path']}' [/]\n"
                )
                continue

        project_metadata = ProjectMetadata(r_.project, r_.timeline)

        # Some picking and parsing, need to get hands dirty
        cp = clip_properties
        source_metadata = SourceMetadata(
            clip_name=cp["Clip Name"],
            file_name=cp["File Name"],
            file_path=cp["File Path"],
            duration=cp["Duration"],
            resolution=list(cp["Resolution"]).split("x"),
            frames=int(cp["Frames"]),
            fps=float(cp["FPS"]),
            h_flip=True if cp["H-FLIP"] == "On" else False,
            v_flip=True if cp["H-FLIP"] == "On" else False,
            proxy_status=cp["Proxy"],
            proxy_media_path=cp["Proxy Media Path"],
            start=int(cp["Start"]),
            end=int(cp["End"]),
            start_tc=cp["Start TC"],
            end_tc=cp["End TC"],
        )

        # Define job
        job = Job(project_metadata, source_metadata, settings)

        # Add media pool item reference
        mpi_refs.add_ref(source_metadata.file_name, media_pool_item)

        logger.debug(f"[magenta]Clip properties: {job}\n")
        jobs.append(job)

    logger.info(f"[green]Total queuable clips on timeline: {len(jobs)}[/]")

    return jobs, mpi_refs
