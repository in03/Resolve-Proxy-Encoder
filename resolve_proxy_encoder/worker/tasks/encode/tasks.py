import logging

from ....app.utils import core
from ....settings.manager import SettingsManager
from ....worker.celery import app
from ....worker.ffmpeg.ffmpeg_process import FfmpegProcess
from ....worker import utils

from rich import print
from rich.console import Console

# Worker and Celery settings pulled from local user_settings file
# All other settings are passed from queuer
settings = SettingsManager()
console = Console()

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)
logger.setLevel(settings["worker"]["loglevel"])


@app.task(
    bind=True,
    acks_late=True,
    track_started=True,
    prefetch_limit=1,
    soft_time_limit=60,
    reject_on_worker_lost=True,
    queue=utils.get_queue(),
)
def encode_proxy(self, job):

    """
    Celery task to encode proxy media using parameters in job argument
    and user-defined settings
    """

    # Print new job header #####################################################################

    print("\n")
    console.rule(f"[green]Received proxy encode job :clapper:[/]", align="left")
    print("\n")

    logger.info(
        f"[magenta bold]Job: [/]{self.request.id}\n" f"Input File: '{job['file_path']}'"
    )

    ############################################################################################

    # Get proxy output path
    output_path = utils.ensure_output_paths(job)

    # Log job details
    logger.info(f"Output File: '{output_path}'\n")
    logger.info(
        f"Source Resolution: {int(job['resolution'][0])} x {int(job['resolution'][1])}"
    )
    logger.info(f"Horizontal Flip: {job['h_flip']}\n" f"Vertical Flip: {job['h_flip']}")
    logger.info(f"Starting Timecode: {job['start_tc']}")

    # Get FFmpeg Command
    ps = job["proxy_settings"]

    ffmpeg_command = [
        "ffmpeg",
        "-y",  # Never prompt!
        *ps["misc_args"],  # User global settings
        "-i",
        job["file_path"],
        "-c:v",
        ps["codec"],
        "-profile:v",
        ps["profile"],
        "-vsync",
        "-1",  # Necessary to match VFR
        "-vf",
        f"scale=-2:{int(job['proxy_settings']['vertical_res'])},"
        f"{utils.get_flip(job)}"
        f"format={ps['pix_fmt']}",
        "-c:a",
        ps["audio_codec"],
        "-ar",
        ps["audio_samplerate"],
        "-timecode",
        job["start_tc"],
        output_path,
    ]

    print()  # Newline
    logger.debug(f"[magenta]Running! FFmpeg command:[/]\n{' '.join(ffmpeg_command)}\n")

    process = FfmpegProcess(
        command=[*ffmpeg_command], ffmpeg_loglevel=ps["ffmpeg_loglevel"]
    )

    # Get logfile path
    logfile_path = utils.ensure_logs_output_path(job, output_path)
    logger.debug(f"[magenta]Encoder logfile path: {logfile_path}[/]")

    # Run encode job
    logger.info("[yellow]Encoding...[/]")

    try:
        process.run(
            task_id=self.request.id,
            worker_name="",
            logfile=logfile_path,
        )

    except Exception as e:
        logger.exception(f"[red] :warning: Couldn't encode proxy.[/]\n{e}")

    return f"{job['file_name']} encoded successfully"
