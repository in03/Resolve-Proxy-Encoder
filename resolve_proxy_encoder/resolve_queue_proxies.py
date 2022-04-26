# Save proxy clip list

import glob
import os
import pathlib
import shutil
import sys
import tkinter
import tkinter.messagebox
from typing import Union

from celery import group
from rich import print

# from rich.traceback import install as install_rich_tracebacks

from resolve_proxy_encoder import helpers
from resolve_proxy_encoder.link_proxies import link_proxies
from resolve_proxy_encoder.settings.app_settings import Settings
from resolve_proxy_encoder.worker.celery import app
from resolve_proxy_encoder.worker.tasks.standard.tasks import encode_proxy


# install_rich_tracebacks(show_locals=True)
settings = Settings()
config = settings.user_settings

logger = helpers.get_rich_logger(config["app"]["loglevel"])

# Get global variables
resolve_obj = helpers.get_resolve_objects()
resolve = resolve_obj["resolve"]
project = resolve_obj["project"]
timeline = resolve_obj["timeline"]
media_pool = resolve_obj["media_pool"]

resolve_job_name = f"{project.GetName()} - {timeline.GetName()}"
proxy_path_root = os.path.normpath(config["paths"]["proxy_path_root"])

# Prevent TKinter root window showing
root = tkinter.Tk()
root.withdraw()

# Set global flags
SOME_ACTION_TAKEN = False


def confirm(title, message):
    """General tkinter confirmation prompt using ok/cancel.
    Keeps things tidy"""

    answer = tkinter.messagebox.askokcancel(
        title=title,
        message=message,
    )

    global SOME_ACTION_TAKEN
    SOME_ACTION_TAKEN = True
    return answer


def handle_workers():
    """Detect amount of online workers. Warn if no workers detected.

    Args:
        None

    Returns:
        None

    Raises:
        Unhandled exception if can't access Celery app.control
    """

    try:

        i = app.control.inspect().active_queues()

    except Exception as e:
        raise Exception("Unhandled exception: " + str(e))

    if i is not None:
        worker_count = len(i)

        if worker_count > 0:
            print(f"[cyan]{worker_count} workers online[/]")
            return

    else:
        print(f"[yellow]No workers online![/]")
        answer = tkinter.messagebox.askokcancel(
            title="No workers online",
            message=f"You haven't got any workers running!\n"
            + f"Don't forget to start one after queuing :) ",
        )

        if answer is True:
            return

        else:
            print("Exiting...")
            helpers.app_exit(0)


def get_resolve_timelines(active_timeline_first=True):
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


def add_expected_proxy_path(media_list: list) -> list:
    """Adds the current Expected Proxy Dir to each media item in the media list.

    Having `Expected Proxy Dir` allows us to check if unlinked proxies exist
    and prompt the user to link them.

    Args:
        media_list: list of dictionary media items to add `Expected Proxy Dir` to.

    Returns:
        media_list: modified list of dictionary media items with added `Expected Proxy Dir`.

    Raises:
        ValueError: If `expected_proxy_path` cannot be resolved.
    """

    media_list_ = media_list

    for media in media_list_:

        file_path = media["File Path"]
        p = pathlib.Path(file_path)

        # Tack the source media relative path onto the proxy media path
        expected_proxy_path = os.path.join(
            proxy_path_root, os.path.dirname(p.relative_to(*p.parts[:1]))
        )

        if expected_proxy_path:
            media.update({"Expected Proxy Dir": expected_proxy_path})
        else:
            raise ValueError(f"Could not find Expected Proxy Dir for '{file_path}'")

    return media_list_


def handle_file_collisions(media_list: list) -> list:
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

    media_list_ = []
    multiple_versions_count = 0

    for item in media_list:

        current_path = item.get("Expected Proxy Dir")

        # !ERROR: no expected path key
        if not current_path:
            logger.error(
                f"Expected proxy Dir was missing for an item: {item}. Skipping..."
            )
            continue

        final_path = _increment_file_if_exist(current_path)

        # Increment multiple version flag
        if final_path != current_path:
            multiple_versions_count += 1

        # !ERROR: Couldn't get final path
        if not final_path:
            logger.error(f"Couldn't get final path for an item: {item}. Skipping...")
            continue

    if multiple_versions_count:

        logger.warning(
            f"[yellow]{multiple_versions_count} files have outdated proxies! Recommend manually deleting when possible.",
            extra={"markup": True},
        )

    return media_list_


def handle_final_queuable(media_list: list):
    """Final prompt to confirm number queueable or warn if none.

    Args:
        media_list: list of dictionary media items to check length for.

    Returns:
        None: No need to chain anything here.

    Raises:
        TypeError: if media_list is not a list
    """

    if len(media_list) == 0:
        global SOME_ACTION_TAKEN
        if not SOME_ACTION_TAKEN:
            print(f"[red]No clips to queue.[/]")
            tkinter.messagebox.showwarning(
                "No new media to queue",
                "Looks like all your media is already linked. \n"
                + "If you want to re-rerender some proxies, unlink those existing proxies within Resolve and try again.",
            )
            sys.exit(1)
        else:
            print(f"[green]All clips linked now. No encoding necessary.[/]")
            helpers.app_exit(0)

    # Final Prompt confirm
    if not confirm(
        "Go time!", f"{len(media_list)} clip(s) are ready to queue!\n" + "Continue?"
    ):
        helpers.app_exit(0)
    return


def get_video_track_items(timeline):
    """Get all video track items from the provided timeline"""

    all_track_items = []

    # Get count of tracks (index) in active timeline
    track_len = timeline.GetTrackCount("video")
    print(f"[green]Video track count: {track_len}[/]")

    # For each track in timeline (using index)
    for i in range(1, track_len + 1):

        # Get items
        track_items = timeline.GetItemListInTrack("video", i)

        if track_items is None:
            print(f"[yellow]No items found in track {i}[/]")
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


def get_source_metadata(media_pool_items):
    """Return source metadata for each media pool item that passes configured criteria.

    each media pool item must meet the following criteria:
        - return valid clip properties (needed for encoding, internal track items don't have them)
        - whitelisted extension (e.g, BRAW performs fine without proxies)
        - whitelisted framerate (optional) FFmpeg should handle most

    Args:
        - media_pool_items: list of Resolve API media pool items

    Returns:
        - filtered_metadata: a list of dictionaries containing all valid media and its Resolve-given properties.

    Raises:
        - none

    """

    # Lists
    filtered_metadata = []

    # Prevent filtering same media item multiple times
    # since multiple clips can have same source media
    seen_item = []

    for media_pool_item in media_pool_items:

        # Check media pool item is valid, get UUID
        try:

            mpi_uuid = str(media_pool_item).split("UUID:")[1].split("]")[0]

        except:

            logger.debug(
                f"[magenta]Media Pool Item: 'None'[/]\n"
                + f"[yellow]Invalid item: has no UUID[/]\n",
                extra={"markup": True},
            )
            continue

        if str(media_pool_item) in seen_item:

            logger.debug(
                f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                + "[yellow]Already seen media pool item. Skipping...[/]\n",
                extra={"markup": True},
            )
            continue

        else:

            # Add first encounter to list for comparison
            seen_item.append(str(media_pool_item))

        # Check media pool item has clip properties
        if not hasattr(media_pool_item, "GetClipProperty()"):

            logger.debug(
                f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                + "[yellow]Media pool item has no clip properties. Skipping...[/]\n",
                extra={"markup": True},
            )
            continue

        # Get source metadata, path, extension
        source_metadata = media_pool_item.GetClipProperty()
        source_path = source_metadata["File Path"]
        source_ext = os.path.splitext(source_path)[1].lower()

        # Might still get media that has clip properties, but empty attributes
        # Should only be internally generated media that returns this way
        if source_path is "":

            logger.debug(
                f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                + f"clip properties did not return a valid filepath. Skipping...\n",
                extra={"markup": True},
            )
            continue

        # Filter extension
        if source_ext not in config["filters"]["extension_whitelist"]:

            logger.warning(
                f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                + f"[yellow]Ignoring non-whitelisted file extension: '{source_ext}'[/]\n"
                + f"from '{source_metadata['File Path']}'[/]\n",
                extra={"markup": True},
            )
            continue

        # Filter framerate
        source_framerate = source_metadata["FPS"]
        if config["filters"]["use_framerate_whitelist"] == True:
            if source_framerate not in config["filters"]["framerate_whitelist"]:

                logger.warning(
                    f"[magenta]Media Pool Item: {mpi_uuid}[/]\n"
                    + f"[yellow]Ignoring non-whitelisted framerate: '{source_framerate}'[/] "
                    + f"from '{source_metadata['File Path']}' [/]\n",
                    extra={"markup": True},
                )
                continue

        # TODO: Add the Resolve API media pool item object so we can call it directly to link
        # source_metadata.update({'media_pool_item_object':media_pool_item})
        filtered_metadata.append(source_metadata)

    print(f"[green]Total queuable clips on timeline: {len(filtered_metadata)}[/]")

    return filtered_metadata


def remove_duplicate_elements(elements):
    """Ensure each element is unique so we don't process it multiple times."""

    unique_sets = set(frozenset(d.items()) for d in elements)
    unique_dict_list = [dict(s) for s in unique_sets]

    print(f"[green]Unique queuable: {len(unique_dict_list)}[/]")

    return unique_dict_list


def get_newest_proxy_file(expected_path: str) -> Union[str, None]:
    """Get the last modified proxy file if multiple variants of same filename exist.

    Args:
        proxy_dir(str): The path all proxies for comparison are located.

    Returns:
        final_proxy_path(str): The file path to the matching proxy file that was last modified.

    """

    expected_filename = os.path.basename(expected_path)

    # Fetch paths of all possible variants of source filename
    matching_proxy_files = glob.glob(expected_path + "*.*")

    if not len(matching_proxy_files):
        logger.info(f"[yellow]No existing proxies found for '{expected_filename}'[/]")
        return

    if len(matching_proxy_files) == 1:
        return matching_proxy_files[0]

    # Sort matching proxy files by last modified in descending order
    matching_proxy_files.sort(key=os.path.getmtime, reverse=True)

    logger.info(
        f"[yellow]Found {len(matching_proxy_files)} existing matches for '{expected_filename}'[/]",
        extra={"Markup": True},
    )

    # Assume we want newest matching file
    final_proxy_path = matching_proxy_files[0]
    final_proxy_filename = os.path.basename(final_proxy_path)

    logger.info(
        f"[yellow]Using newest: '{os.path.basename(final_proxy_filename)}'[/]",
        extra={"Markup": True},
    )

    return final_proxy_path


def handle_orphaned_proxies(media_list: list) -> list:
    """Prompts user to tidy orphaned proxies into the current proxy path structure.

    Orphans can become separated from a project if source media file-path structure changes.
    Finding them saves unncessary re-rendering time and lost disk space.

    Args:
        media_list: list of dictionary media items to check orphaned proxies for.

    Returns:
        media_list: unmodified `media_list`, returns for ease of chaining.
    """

    print(f"[cyan]Checking for orphaned proxies.")
    orphaned_proxies = []

    for clip in media_list:
        if clip["Proxy"] != "None" or clip["Proxy"] == "Offline":
            linked_proxy_path = os.path.splitext(clip["Proxy Media Path"])
            linked_proxy_path[1].lower()

            file_path = clip["File Path"]
            p = pathlib.Path(file_path)

            # Append the source media relative path onto the proxy media path
            output_dir = os.path.join(
                proxy_path_root, os.path.dirname(p.relative_to(*p.parts[:1]))
            )
            new_output_path = os.path.join(output_dir, os.path.basename(file_path))
            new_output_path = os.path.splitext(new_output_path)
            new_output_path[1].lower()

            if linked_proxy_path[0] != new_output_path[0]:

                # Rejoin extensions
                linked_proxy_path = "".join(linked_proxy_path)
                new_output_path = "".join(new_output_path)
                orphaned_proxies.append(
                    {
                        "Old Path": linked_proxy_path,
                        "New Path": new_output_path,
                    }
                )

    if len(orphaned_proxies) > 0:

        print(f"[yellow]Orphaned proxies: {len(orphaned_proxies)}[/]")

        answer = tkinter.messagebox.askyesnocancel(
            title="Orphaned proxies",
            message=f"{len(orphaned_proxies)} clip(s) have orphaned proxy media. "
            + "Would you like to attempt to automatically move these proxies to the up-to-date proxy folder?\n\n"
            + "For help, check 'Managing Proxies' in our YouTour documentation portal.",
        )
        global SOME_ACTION_TAKEN
        SOME_ACTION_TAKEN = True

        if answer == True:
            print(f"[yellow]Moving orphaned proxies.[/]")
            for proxy in orphaned_proxies:

                output_folder = os.path.dirname(proxy["New Path"])
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)

                if os.path.exists(proxy["Old Path"]):
                    shutil.move(proxy["Old Path"], proxy["New Path"])
                else:
                    print(
                        f"{proxy['Old Path']} doesn't exist. Most likely a parent directory rename created this orphan."
                    )
            print("\n")

        elif answer == None:
            helpers.app_exit()

    return media_list


def handle_already_linked(
    media_list: list, offline_types: list = ["Offline", "None"]
) -> list:
    """Remove items from media-list that are already linked to a proxy.
    
    Since re-rendering linked clips is rarely desired behaviour, we remove them without prompting.
    If we do want to re-render proxies, we can unlink them from Resolve and we'll be prompted with the 
    'unlinked media' warning. By default if a proxy item is marked as 'Offline' it won't be removed
    since we most likely need to re-render it.

    Args: 
        media_list: list of dictionary media items to check for linked proxies.\
        retain_types: list of strings of `Proxy` types to keep in returned `media_list` even if linked.

    Returns:
        media_list: refined list of dictionary media items that are not linked to a proxy.
    """

    print(f"[cyan]Checking for source media with linked proxies.[/]")
    already_linked = [x for x in media_list if str(x["Proxy"]) not in offline_types]

    if len(already_linked) > 0:

        print(f"[yellow]Skipping {len(already_linked)} already linked.[/]")
        media_list = [x for x in media_list if x not in already_linked]
        print("\n")

    return media_list


def handle_offline_proxies(media_list: list) -> list:
    """Prompt to rerender proxies that are 'linked' but their media does not exist.

    Resolve refers to proxies that are linked but inaccessible as 'offline'.
    This prompt can warn users to find that media if it's missing, or rerender if intentionally unavailable.

    Args:
        media_list: list of dictionary media items to check for `Proxy` value.

    Returns:
        media_list: Modified list of dictionary media items with `Proxy` set to `None` if re-rendering.
    """

    print(f"[cyan]Checking for offline proxies[/]")
    offline_proxies = [x for x in media_list if x["Proxy"] == "Offline"]

    if len(offline_proxies) > 0:

        print(f"[cyan]Offline proxies: {len(offline_proxies)}[/]")
        answer = tkinter.messagebox.askyesnocancel(
            title="Offline proxies",
            message=f"{len(offline_proxies)} clip(s) have offline proxies.\n"
            + "Would you like to rerender them?",
        )
        global SOME_ACTION_TAKEN
        SOME_ACTION_TAKEN = True

        if answer == True:
            print(f"[yellow]Rerendering offline: {len(offline_proxies)}[/]")
            # Set all offline clips to None, so they'll rerender
            for media in media_list:
                if media["Proxy"] == "Offline":
                    media["Proxy"] = "None"
            print("\n")

        if answer == None:
            helpers.app_exit(0)

    return media_list


def handle_existing_unlinked(media_list: list) -> list:

    """Prompts user to either link or re-render unlinked proxy media that exists in the expected location.

    This handler will run if proxies are either unlinked at some point or were never linked after proxies finished rendering.

    Args:
        media_list: list of dictionaries with media items to check `Expected Proxy Dir` variable on.

    Returns:
        media_list: refined list of dictionaries with media items that do not have linked proxies.
    """

    print(f"[cyan]Checking for existing, unlinked media.[/]")

    existing_unlinked = []

    add_expected_proxy_path(media_list)

    # Iterate media list
    for media in media_list:
        if media["Proxy"] == "None":

            expected_proxy_dir = media["Expected Proxy Dir"]
            source_media_basename = os.path.splitext(
                os.path.basename(media["File Name"])
            )[0]
            expected_proxy_file = os.path.join(
                expected_proxy_dir, source_media_basename
            )
            expected_proxy_file = os.path.splitext(expected_proxy_file)[0]

            existing_proxy_file = get_newest_proxy_file(expected_proxy_file)

            if existing_proxy_file:

                media.update({"Unlinked Proxy": existing_proxy_file})
                existing_unlinked.append(existing_proxy_file)

    # If any unlinked, prompt for linking
    if len(existing_unlinked) > 0:

        print(f"\n[yellow]Found {len(existing_unlinked)} unlinked[/]")

        answer = tkinter.messagebox.askyesnocancel(
            title="Found unlinked proxy media",
            message=f"{len(existing_unlinked)} clip(s) have existing but unlinked proxy media. "
            + "Would you like to link them? If you select 'No' they will be re-rendered.",
        )

        global SOME_ACTION_TAKEN
        SOME_ACTION_TAKEN = True

        if answer == True:
            media_list = legacy_link(media_list)

        elif answer == False:
            print(f"[yellow]Existing proxies will be OVERWRITTEN![/]")
            print("\n")

        else:
            helpers.app_exit(0)

    return media_list


def create_tasks(clips, **kwargs):
    """Create metadata dictionaries to send as Celery tasks'"""

    # Append project details to each clip
    tasks = [dict(item, **kwargs) for item in clips]
    return tasks


def queue_job(tasks):
    """Send tasks as a celery job 'group'"""

    # Wrap job object in task function
    callable_tasks = [encode_proxy.s(x) for x in tasks]
    logger.debug(f"callable_tasks: {callable_tasks}")

    # Create job group to retrieve job results as batch
    job = group(callable_tasks)

    # Queue job
    queued_job = job.apply_async()
    logger.info(f"[cyan]Queued job {queued_job}[/]")

    return queued_job


def wait_encode(job):
    """Block until all queued jobs finish, notify results."""

    helpers.notify(f"Started encoding job '{resolve_job_name}'")
    print(f"[yellow]Waiting for job to finish. Feel free to minimize.[/]")

    result = job.join()

    # Notify failed
    if job.failed():
        fail_message = (
            "Some videos failed to encode!"
            + f"Check flower dashboard at address: {config['celery_settings']['flower_url']}."
        )
        print("[red]fail_message[/]")
        helpers.notify(fail_message)

    # Notify complete
    complete_message = f"Completed encoding {job.completed_count()} proxies."
    print(f"[green]{complete_message}[/]")
    print("\n")

    helpers.notify(complete_message)

    return result


def postencode_link(media_list):
    """Iterate through media mutated during script call, attempt to link the source media"""

    print(f"[cyan]Linking {len(media_list)} proxies.[/]")

    link_success = []
    link_fail = []

    # Iterate through all available proxies
    for media in media_list:
        proxy = media.get("Unlinked Proxy", None)
        if proxy == None:
            continue

        # Check they exist
        if not os.path.exists(proxy):
            tkinter.messagebox.showerror(
                title="Error linking proxy",
                message=f"Proxy media not found at '{proxy}'",
            )
            print(f"[red]Error linking proxy: Proxy media not found at '{proxy}'")
            continue

        else:
            media.update({"Unlinked Proxy": None})  # Set existing to none once linked

        media.update({"Proxy": "1280x720"})

        # Actually link proxies
        media_pool_item_obj = media["media_pool_item_obj"]
        if media_pool_item_obj.LinkProxyMedia(proxy):

            # TODO get this working!
            print(f"[green]Linked {media['File Name']}[/]")
            link_success.append(proxy)

        else:
            print(f"[red]Failed link.\n[/]")
            link_fail.append(proxy)

    if link_success:
        print(f"[green]Succeeeded linking: {len(link_success)}[/]")

    if link_fail:
        print(f"[red]Failed linking: {len(link_success)}[/]")

    # link_proxies(existing_proxies)

    print("\n")

    pre_len = len(media_list)
    media_list = [x for x in media_list if "Unlinked Proxy" not in x]
    post_len = len(media_list)
    print(f"{pre_len - post_len} proxy(s) linked, will not be queued.")
    print(f"[magenta]Queueing {post_len}[/]")
    print("\n")

    return media_list


# TODO: Need to get new search and link working
# Legacy link is the original code and it's soooo bad.
# I haven't looked at it in ages. But then I bit off more than I could
# chew working on this. Find a middle ground!
# labels: bug


def search_and_link():
    """Search through all existing media in active project and
    attempt to find linkable proxies in expected directory
    """

    print("Not yet implemented!")
    print("Run the manual link program in 'Misc Tools' on the Streamdeck")
    sys.exit(1)

    linked = []
    # failed = []

    timelines = get_resolve_timelines()
    if not timelines:
        raise Exception("No timelines exist in current project.")

    # Get clips from all timelines.
    for timeline in timelines:

        track_items = get_video_track_items(timeline)
        media_pool_items = get_media_pool_items(track_items)
        source_metadata = get_source_metadata(media_pool_items)
        source_metadata = remove_duplicate_elements(source_metadata)

    #     clips = timeline_data['clips']
    #     unlinked_source = [x for x in clips if x not in linked]

    #     if len(unlinked_source) == 0:
    #         if config['loglevel'] == "DEBUG": print(f"[yellow]No more clips to link in {timeline_data['name']}[/]")
    #         continue
    #     else:
    #         print(f"[cyan]Searching timeline {timeline_data['name']}[/]")

    #     unlinked_proxies = [x for x in proxy_files if x not in linked]
    #     print(f"Unlinked source count: {len(unlinked_source)}")
    #     print(f"Unlinked proxies count: {len(unlinked_proxies)}")

    #     if len(unlinked_proxies) == 0:
    #         print(f"[yellow]No more proxies to link in {timeline_data['name']}[/]")
    #         return

    #     linked_, failed_ = (__link_proxies(proxy_files, clips))

    #     linked.extend(linked_)
    #     failed.extend(failed_)

    #     if config['loglevel'] == "DEBUG": print(f"Linked: {linked}, Failed: {failed}")

    # if len(failed) > 0:
    #     print(f"[red]The following files matched, but couldn't be linked. Suggest rerendering them:[/]")
    #     [print(os.path.basename(x)) for x in failed]
    #     print("\n")

    return linked


def legacy_link(media_list):
    """This is sooooo dank. But it's the only way that works atm."""

    print(f"[cyan]Linking {len(media_list)} proxies.[/]")
    existing_proxies = []

    for media in media_list:
        proxy = media.get("Unlinked Proxy", None)
        if proxy == None:
            continue

        existing_proxies.append(proxy)

        if not os.path.exists(proxy):
            tkinter.messagebox.showerror(
                title="Error linking proxy",
                message=f"Proxy media not found at '{proxy}'",
            )
            print(f"[red]Error linking proxy: Proxy media not found at '{proxy}'[/]")
            continue

        else:
            media.update({"Unlinked Proxy": None})  # Set existing to none once linked

        media.update({"Proxy": "1280x720"})

    link_proxies(existing_proxies)

    print("\n")

    pre_len = len(media_list)
    media_list = [x for x in media_list if "Unlinked Proxy" not in x]
    post_len = len(media_list)
    print(f"{pre_len - post_len} proxy(s) linked, will not be queued.")
    print(f"[magenta]Queueing {post_len}[/]")
    print("\n")

    return media_list


def main():
    """Main function"""

    print(f"[cyan]Working on: {resolve_job_name}[/]")
    handle_workers()
    print("\n")

    # Lets make it happen!
    track_items = get_video_track_items(timeline)
    media_pool_items = get_media_pool_items(track_items)
    source_metadata = get_source_metadata(media_pool_items)

    print("\n")

    clips = source_metadata

    # Prompt user for intervention if necessary
    clips = handle_already_linked(clips, ["Offline", "None"])
    clips = handle_offline_proxies(clips)
    clips = handle_existing_unlinked(clips)

    handle_final_queuable(clips)

    tasks = create_tasks(
        clips,
        project=project.GetName(),
        timeline=timeline.GetName(),
    )

    job = queue_job(tasks)
    wait_encode(job)

    # ATTEMPT POST ENCODE LINK
    try:

        clips = legacy_link(clips)

    except:

        print("[red]Couldn't link clips. Link manually...[/]")
        helpers.app_exit(1, -1)

    helpers.app_exit(0)


if __name__ == "__main__":
    main()
