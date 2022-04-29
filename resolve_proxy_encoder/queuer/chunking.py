from __future__ import absolute_import

import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path
from typing import Union

from icecream import ic
from pymediainfo import MediaInfo
from yaspin import yaspin

from ..app.utils import core
from ..settings.manager import SettingsManager
from ..worker.celery import app
from ..worker.ffmpeg.ffmpeg_process import FfmpegProcess
from ..worker.ffmpeg.utils import frac_to_tc

config = SettingsManager()

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)

# TODO: Refactor chunking module
# Other modules can import functions here as needed,
# since execution will be jump between queuer and worker


def chunk_jobs(jobs):
    """
    Split jobs into smaller jobs.

    Adds chunking metadata to each job with in/out ranges to encode long videos across multiple computers.
    Allows greater resource utilisation
    User-specified chunking across
    """

    chunked_jobs = []
    continuous_jobs = []

    if config["chunking"]["enable"]:

        for job in jobs:

            # Get job duration
            logger.debug(
                f"[magenta]Media duration: {job['frames']}\n"
                + f"Chunking threshold: {config['chunking']['chunk_threshold']}"
            )

            if job["frames"] / (job["fps"]) < config["chunking"]["chunk_threshold"]:

                logger.info(
                    f"[cyan]Leaving short job as continuous encode:[/]\n'{job['clip_name']}'\n"
                )
                continuous_jobs.append(job)
                continue

            logger.info(
                f"[cyan]Processing long job as chunked encode:[/]\n'{job['clip_name']}'\n"
            )
            chunks = calculate_chunks(job)

            # Add multiple jobs for each chunk
            for chunk in chunks:

                job_copy = deepcopy(job)
                job_copy.update({"chunk": chunk})
                chunked_jobs.append(job_copy)

    return chunked_jobs + continuous_jobs


def calculate_chunks(clip):
    """
    Calculate frame ranges for ffmpeg to split file
    at specified interval. Return a dictionary with a list of
    dictionaries and their chunking data.
    """

    fps = clip["fps"]
    nb_frames = clip["frames"]

    chunk_duration = config["chunking"]["chunk_duration"]
    chunk_frames = chunk_duration * fps

    logger.debug(
        f"[green][bold]{clip['clip_name']}[/green][/bold]:\n"
        + f"FPS {fps}, Total Secs {int(nb_frames/fps)}\n"
    )

    whole_chunks = int(nb_frames // chunk_frames)

    partial_chunk_length = float(str(nb_frames / chunk_frames)) % 1
    # TODO: This f-string isn't formatted correctly
    # labels: bug

    # Get total chunks
    total_chunks = whole_chunks
    if partial_chunk_length > 0:
        total_chunks = whole_chunks + 1

    logger.debug(
        f"[green][bold]Chunks:[/green][/bold]\n"
        + f"Whole: {whole_chunks}\n"
        + f"Partial: {partial_chunk_length:1.2f}\n"
        + f"Total: {total_chunks}\n"
    )

    print()

    # Init chunk vars
    in_point = 0
    out_point = 0
    chunk_num = 0
    chunks = []

    # Get whole chunks
    # Start at 1 to match total when iterating
    logger.debug(f"[magenta]Individual chunks:")
    for i in range(1, whole_chunks + 1):

        in_point = out_point
        out_point = in_point + chunk_frames
        chunk_num = i

        in_point_tc = frac_to_tc(in_point, fps)
        out_point_tc = frac_to_tc(out_point, fps)

        logger.debug(
            f"[magenta]Whole: {chunk_num}\n"
            + f"in point: {in_point_tc}\n"
            + f"out_point: {out_point_tc}[/]"
        )

        # Append whole chunk
        chunks.append(
            {
                "number": chunk_num,
                "in_point": in_point_tc,
                "out_point": out_point_tc,
            }
        )

    # If partial chunk, get
    if partial_chunk_length > 0:

        in_point = out_point
        out_point = in_point + (chunk_frames * partial_chunk_length)
        chunk_num = chunk_num + 1

        in_point_tc = frac_to_tc(in_point, fps)
        out_point_tc = frac_to_tc(out_point, fps)

        logger.debug(
            f"Partial {chunk_num}, in point: {in_point_tc}, out_point: {out_point_tc}"
        )

        # Append partial
        chunks.append(
            {
                "number": chunk_num,
                "in_point": in_point_tc,
                "out_point": out_point_tc,
            }
        )

    return chunks


def local_encode_chunks(chunk_data, temp_working_dir):
    """Function to encode chunk ranges sequentially.
    Use for testing locally outside of Celery."""

    print()
    logger.info("[green]Encoding Info:[/]")

    # Example syntax:
    # ffmpeg -i input.mp4 -ss 00:00:00 -to 00:10:00 -c copy output1.mp4
    # ffmpeg -i input.mp4 -ss 00:10:00 -to 00:20:00 -c copy output2.mp4

    input_file = chunk_data["file"]
    chunks = chunk_data["chunks"]

    exported_chunks = []

    # Feed chunks
    chunk_len = len(chunks)
    logger.debug(f"Received chunks", chunk_len)
    logger.info(f"[yellow]Encoding chunks :hourglass: ... [/]")

    for chunk in chunks:

        in_point = chunk["in_point"]
        out_point = chunk["out_point"]
        chunk_num = chunk["chunk_number"]

        logger.debug(f"chunk data: {chunk}")

        output_filename = os.path.splitext(os.path.basename(input_file))[0]
        output_filename = output_filename + f"-{chunk_num}" + ".mxf"
        output_file = os.path.join(temp_working_dir, output_filename)

        # Seeking cannot be accurate with '-c copy', since FFMpeg is forced to cut on I-frames!
        cmd = (
            f'ffmpeg -loglevel error -nostats -ss {in_point} -to {out_point} -i "{input_file}" '
            + f'-c:v dnxhd -profile:v dnxhr_sq -vf "scale=1280:720,fps=50,format=yuv422p" -c:a pcm_s16le -ar 48000 "{output_file}"'
        )

        logger.debug("CMD ->\n" + cmd)
        subprocess.call(cmd, shell=True)
        logger.info(f"Finished chunk {chunk_num} :heavy_check_mark:")

        exported_chunks.append(output_file)

    logger.debug(f"Exported chunks: {exported_chunks}")
    return exported_chunks


def stitch(job):
    """Stitch provided chunks using FFMpeg"""

    logger.info(f"[yellow]Stitching chunks[/] :hourglass: ...")
    sorted_chunks = parse_chunks(exported_files)
    temp_concat_file = make_concat_file(sorted_chunks, temp_working_dir)
    stitched_file = ffmpeg_concat(temp_concat_file, sorted_chunks)

    try:

        # TODO: Add ffmpeg concat call here
        logger.info(f"[green]Finished stitching chunks :heavy_check_mark:[/]")
        return True

    except:

        logger.error(f"[red]Failed stitching chunks![/]")
        return False


def parse_chunks(chunks):
    """Check passed files are concatanatable, sort them in order"""

    # Check extensions are the same
    exts = [os.path.splitext(x)[1] for x in chunks]
    if len(Counter(exts)) > 1:

        logger.error(
            "[red]Multiple formats passed!\n"
            "codec, format and extension must be identical![/]"
        )
        core.app_exit(1)

    sorted_chunks = sorted(chunks)
    logger.debug(f"[magenta]Sorted chunks:[/]\n{sorted_chunks}")

    for chunk in sorted_chunks:

        incrementals = []
        # Check filenames are incremented by 1, none missing
        match = re.search(config["chunking"]["increment_regex"], chunk)

        if not match:
            logger.error(
                "File missing incremental! Ensure suffix matches regex set in user settings.\n"
                + "e.g. '-1.mp4', '-2.mp4', etc."
            )
            core.app_exit(1)

        # incrementals.append(match.group(1))

        # if not list(range(int(incrementals[0]), int(incrementals[-1]) + 1)):
        #     print("Filenames do not increment by 1! Are there clips missing?")
        #     sys.exit(1)

        return sorted_chunks


def make_concat_file(job):
    """Create temporary concatentation file for FFmpeg"""

    filepath = os.path.join(job["Expected Proxy Path"], subfolder)

    # Ensure .txt extension
    if not os.path.splitext(filepath)[1]:
        filepath = filepath + ".txt"

    parsed_chunks = [f"file '{os.path.normpath(x)}'\n" for x in chunks]

    with open(filepath, "a") as concat_file:
        concat_file.writelines(parsed_chunks)

    return filepath


def ffmpeg_concat(filename, chunks):
    """Losslessly stitch file chunks with FFmpeg.
    Output to same path as source chunks"""

    # Get source clip's name and extension for output
    source_name, source_ext = os.path.splitext(chunks[0])

    # Remove increment pattern for output
    output_name = re.sub(
        config["chunking"]["increment_regex"],
        "",
        source_name,
    )

    stitched_file = output_name + config["chunking"]["output_suffix"] + source_ext

    # TODO: Use custom ffmpeg class instead of this subprocess call
    # Concat with FFmpeg
    cmd = f'ffmpeg -y -loglevel error -nostats -f concat -safe 0 -i "{filename}" -c copy "{stitched_file}"'
    logger.debug("CMD -> " + cmd)

    subprocess.call(shlex.split(cmd))

    return stitched_file


def move(self):

    """Move the final stitched file to the output destination folder"""

    filename = os.path.basename(self.stitched_file)

    destination_path = os.path.normpath(os.path.join(self.dest, filename))
    logger.info(
        f"[yellow]Moving stitched file: '{os.path.basename(self.stitched_file)} -> '{destination_path}' :hourglass:",
    )

    try:
        shutil.move(self.stitched_file, destination_path)
    except:
        logger.error(
            f"[red]Couldn't move stitched file to destination path[/]{destination_path}"
        )
    else:
        logger.info(f"[green]Successfully moved :heavy_check_mark:[/]")
