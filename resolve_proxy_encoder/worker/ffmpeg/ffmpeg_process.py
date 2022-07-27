import logging
import os
import subprocess
import json

from rich.console import Console
from rich.progress import (
    BarColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeRemainingColumn,
)
from rich.prompt import Confirm

from ffmpeg import probe

from ...app.utils import core
from ...settings.manager import SettingsManager
from ...app.broker import PubSub

settings = SettingsManager()

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)
logger.setLevel(settings["worker"]["loglevel"])


class FfmpegProcess:
    def __init__(self, command, ffmpeg_loglevel="verbose"):
        """
        Creates the list of FFmpeg arguments.
        Accepts an optional ffmpeg_loglevel parameter to set the value of FFmpeg's -loglevel argument.
        """
        index_of_filepath = command.index("-i") + 1
        self._filepath = command[index_of_filepath]
        self._output_filepath = command[-1]

        self.pubsub = PubSub(settings)

        dirname = os.path.dirname(self._output_filepath)

        if dirname != "":
            self._dir_files = [file for file in os.listdir(dirname)]
        else:
            self._dir_files = [file for file in os.listdir()]

        self._can_get_duration = True

        try:
            self._duration_seconds = float(probe(self._filepath)["format"]["duration"])
        except Exception:
            self._can_get_duration = False

        self._ffmpeg_args = command + ["-loglevel", ffmpeg_loglevel]

        if self._can_get_duration:
            # pipe:1 sends the progress to stdout. See https://stackoverflow.com/a/54386052/13231825
            self._ffmpeg_args += ["-progress", "pipe:1", "-nostats"]

    def update_progress(self, **kwargs):

        channel = f"task-progress:{kwargs['task_id']}"
        self.pubsub.publish(
            channel,
            **kwargs,
        )

    def run(self, task_id=None, worker_name=None, logfile=None):

        # Get progress bar
        console = Console(record=True)
        progress_bar = Progress(
            TextColumn("                   "),  # Spacer to match logging
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            # TextColumn("[progress.completed]{task.completed:>3.0f} of"),
            # TextColumn("[progress.total]{task.total:>3.0f} secs"),
            TextColumn("[yellow]ETA:[/]"),
            TimeRemainingColumn(),
            # TimeElapsedColumn(),
            console=console,
            transient=True,
        )

        # Open the logfile for logging if enabled
        if logfile:
            with open(logfile, "w") as f:
                pass

        # Catch the ffmpeg overwrite prompt
        if "-y" not in self._ffmpeg_args and self._output_filepath in self._dir_files:
            if not Confirm.ask(
                f"[yellow]'{self._output_filepath}' already exists. Overwrite?[/]"
            ):
                core.app_exit(0)

        self._ffmpeg_args += ["-y"]

        # If we sucessfully got file duration, start the process
        if self._can_get_duration:

            process = None

            if logfile:
                with open(logfile, "a") as f:
                    process = subprocess.Popen(
                        self._ffmpeg_args, stdout=subprocess.PIPE, stderr=f
                    )
        else:
            logger.critical(f"[red]Couldn't get duration of '{self._filepath}'[/]")
            core.app_exit(1, -1)

        previous_seconds_processed = 0
        seconds_processed = 0

        encoding_task = progress_bar.add_task(
            description="[yellow]Encode[/]",
            total=self._duration_seconds,
        )

        # Don't continue if we don't have the process
        if not process:
            logger.critical(f"Couldn't start ffmpeg process!")
            core.app_exit(1, -1)

        try:

            progress_bar.start()

            while process.poll() is None:

                while not progress_bar.finished:

                    if self._can_get_duration:

                        ffmpeg_output = process.stdout.readline().decode()

                        if "out_time_ms" in ffmpeg_output:

                            seconds_processed = (
                                int(ffmpeg_output.strip()[12:]) / 1_000_000
                            )
                            seconds_increase = (
                                seconds_processed - previous_seconds_processed
                            )

                            # Update worker stdout progress bar
                            progress_bar.update(
                                task_id=encoding_task,
                                advance=seconds_increase,
                            )

                            # Publish task progress
                            if task_id:

                                self.update_progress(
                                    task_id=task_id,
                                    worker_name=worker_name,
                                    output_filename=os.path.basename(
                                        self._output_filepath
                                    ),
                                    advance=seconds_increase,
                                    completed=seconds_processed,
                                    total=self._duration_seconds,
                                )

                            previous_seconds_processed = seconds_processed

                        else:

                            break

            progress_bar.stop()
            logger.info("[green]Finished encoding[/]")

        except KeyboardInterrupt:
            progress_bar.stop()
            process.kill()
            print("[yellow][KeyboardInterrupt] FFmpeg process killed. Exiting...[/]")
            core.app_exit(0)

        except Exception as e:
            progress_bar.stop()
            process.kill()
            logger.critical(f"[red][Error] {e}\nExiting...[/]")
            core.app_exit(1, -1)
