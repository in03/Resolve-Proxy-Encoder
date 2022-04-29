# Link proxies

import logging
import os
import tkinter
import tkinter.messagebox
import traceback
from tkinter import filedialog
from typing import Tuple

from rich import print

from ..app.utils import core
from ..settings.manager import SettingsManager

config = SettingsManager()

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)


def get_proxy_path():

    proxy_path_root = config["paths"]["proxy_path_root"]

    f = input("Enter path to search for proxies: ")
    if f is None:
        print("User cancelled. Exiting.")
        exit(0)
    return f


def recurse_dir(root):
    """Recursively search given directory for files
    and return full filepaths
    """

    all_files = [
        os.path.join(root, f) for root, dirs, files in os.walk(root) for f in files
    ]
    print(f"Found {len(all_files)} files in folder {root}")
    return all_files


def filter_files(dir_, extension_whitelist):
    """Filter files by allowed filetype"""

    # print(f"{Fore.CYAN}{timeline.GetName()} - Video track count: {track_len}")
    allowed = [x for x in dir_ if os.path.splitext(x) in extension_whitelist]
    return allowed


def get_track_items(timeline, track_type="video"):
    """Retrieve all video track items from a given timeline object"""

    track_len = timeline.GetTrackCount("video")
    logger.debug(f"[cyan]{timeline.GetName()} - Video track count: {track_len}[/]")

    items = []

    for i in range(track_len):
        i += 1  # Track index starts at one...
        items.extend(timeline.GetItemListInTrack(track_type, i))

    return items


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


def get_timeline_data(timeline):
    """Return a dictionary containing timeline names,
    their tracks, clips media paths, etc.
    """

    clips = get_track_items(timeline, track_type="video")
    data = {
        "timeline": timeline,
        "name": timeline.GetName(),
        "track count": timeline.GetTrackCount(),
        "clips": clips,
    }

    return data


def _link_proxies(proxy_files, clips):
    """Actual linking function.
    Matches filenames between lists of paths.
    'clips' actually needs to be a Resolve timeline item object.
    """

    linked_proxies = []
    failed_proxies = []

    for proxy in proxy_files:
        for clip in clips:

            proxy_name = os.path.splitext(os.path.basename(proxy))[0]
            if proxy in failed_proxies:
                logger.warning(f"[yellow]Skipping {proxy_name}, already failed.")
                break

            try:
                media_pool_item = clip.GetMediaPoolItem()
                path = media_pool_item.GetClipProperty("File Path")
                filename = os.path.splitext(os.path.basename(path))[0]

                if proxy_name.lower() in filename.lower():

                    logger.info("[cyan]Found match:")
                    print(f"{proxy} &\n{path}")

                    if media_pool_item.LinkProxyMedia(proxy):

                        logger.info(f"[green]Linked\n")
                        linked_proxies.append(proxy)

                    else:
                        logger.error(f"[red]Failed link.\n")
                        failed_proxies.append(proxy)

            except AttributeError:
                logger.debug(
                    f"[magenta]{clip.GetName()} has no 'file path' attribute,"
                    + " probably Resolve internal media."
                )

    return linked_proxies, failed_proxies


def find_and_link_proxies(project, proxy_files) -> Tuple[list, list]:
    """Attempts to match source media in active Resolve project
    with a list of filepaths to proxy files."""

    linked = []
    failed = []

    timelines = get_resolve_timelines(project)

    if not timelines:

        logger.error("[red]No timelines exist in current project.[/]")
        return linked, failed

    # Get clips from all timelines.
    for timeline in timelines:

        timeline_data = get_timeline_data(timeline)
        clips = timeline_data["clips"]
        unlinked_source = [x for x in clips if x not in linked]

        if not unlinked_source:
            logger.info(f"[yellow]No more clips to link in {timeline_data['name']}")
            continue
        else:
            logger.info(f"[cyan]Searching timeline {timeline_data['name']}")

        unlinked_proxies = [x for x in proxy_files if x not in linked]
        logger.info(f"Unlinked source count: {len(unlinked_source)}")
        logger.info(f"Unlinked proxies count: {len(unlinked_proxies)}")

        if not unlinked_proxies:
            logger.info(f"[green]No more proxies to link[/]")
            break

        linked_, failed_ = _link_proxies(proxy_files, clips)

        # Update lists for each timeline
        linked.extend(linked_)
        failed.extend(failed_)

    if linked:

        logger.info(f"[green]Link success:[/] {len(linked)}")

    if failed:

        logger.error(f"[red]Link fail:[/]{len(failed)}")
        failed_paths = [(os.path.basename(x)) for x in failed]
        logger.error(
            f"[red]The following files matched, but couldn't be linked. Suggest rerendering them:[/]\n{failed_paths}"
        )

    return linked, failed


def link_proxies_with_mpi(media_list):
    """Iterate through media mutated during script call, attempt to link the source media"""

    print(f"[cyan]Linking {len(media_list)} proxies.[/]")

    link_success = []
    link_fail = []

    # Iterate through all available proxies
    for media in media_list:

        proxy = media.get("Unlinked Proxy", None)

        if not proxy:
            continue

        if not os.path.exists(proxy):
            logger.error(f"[red]Proxy media not found at '{proxy}'")

        else:
            # Set existing to none once linked
            media.update({"Unlinked Proxy": None})

        # TODO: Should probably use MediaInfo here instead of hardcode
        media.update({"Proxy": "1280x720"})

        # Actually link proxies
        if media["media_pool_item"].LinkProxyMedia(proxy):

            # TODO get this working!
            logger.info(f"[green]Linked [/]'{media['clip_name']}'")
            link_success.append(proxy)

        else:
            logger.error(f"[red]Failed to link [/]'{media['clip_name']}'")
            link_fail.append(proxy)

    if link_success:
        print(f"[green]Succeeeded linking: [/]{len(link_success)}")

    if link_fail:
        print(f"[red]Failed linking: [/]{len(link_fail)}")

    print()

    pre_len = len(media_list)
    media_list = [x for x in media_list if "Unlinked Proxy" not in x]
    post_len = len(media_list)

    print(f"[green]{pre_len - post_len} proxy(s) successfully linked.")
    return media_list


def main():

    from .resolve import ResolveObjects

    try:

        r_ = ResolveObjects()
        proxy_dir = get_proxy_path()

        print(f"Passed directory: '{proxy_dir}'\n")

        all_files = recurse_dir(proxy_dir)
        proxy_files = filter_files(all_files, config["filters"]["extension_whitelist"])
        linked = find_and_link_proxies(r_.project, proxy_files)

    except Exception as e:
        print("ERROR - " + str(e))


if __name__ == "__main__":
    main()
