#!/usr/bin/env python3.6

import operator
import os
import re
import shutil
import sys
import time
import webbrowser
from functools import reduce
from pathlib import Path

from deepdiff import DeepDiff
from resolve_proxy_encoder.helpers import (
    app_exit,
    get_rich_logger,
    install_rich_tracebacks,
)
from rich import print
from rich.prompt import Confirm
from ruamel.yaml import YAML

from schema import SchemaError

from .schema import settings_schema

# # Hardcoded because we haven't loaded user settings yet
logger = get_rich_logger("WARNING")
install_rich_tracebacks()

DEFAULT_SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "default_settings.yml")
USER_SETTINGS_FILE = os.path.join(
    Path.home(), ".config", "resolve_proxy_encoder", "user_settings.yml"
)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Settings(metaclass=Singleton):
    def __init__(
        self,
        default_settings_file=DEFAULT_SETTINGS_FILE,
        user_settings_file=USER_SETTINGS_FILE,
    ):

        self.yaml = YAML()
        self.default_file = default_settings_file
        self.user_file = user_settings_file
        self._ensure_user_file()

        self.default_settings = self._get_default_settings()
        self.user_settings = self._get_user_settings()
        self._ensure_user_keys()

        self._validate_schema(self.default_settings)
        self._validate_schema(self.user_settings)

        print("[green]User settings are valid :white_check_mark:[/]")

    def _get_default_settings(self):
        """Load default settings from yaml"""

        logger.debug(f"Loading default settings from {self.default_file}")

        with open(os.path.join(self.default_file)) as file:
            return self.yaml.load(file)

    def _get_user_settings(self):
        """Load user settings from yaml"""

        logger.debug(f"Loading user settings from {self.user_file}")

        with open(self.user_file, "r") as file:
            return self.yaml.load(file)

    def _ensure_user_file(self):
        """Copy default settings to user settings if it doesn't exist

        Prompt the user to edit the file afterwards, then exit.
        """

        logger.debug(f"Ensuring settings file exists at {self.user_file}")

        if not os.path.exists(self.user_file):

            try:

                if not Confirm.ask(
                    f"[yellow]No user settings file found: [/]'{self.user_file}'\n"
                    + "[cyan]Create using default settings[/]?"
                ):
                    print("[green]Exiting...\n[/]")
                    app_exit(0)

                # Create dir, copy file, open
                try:
                    os.makedirs(os.path.dirname(self.user_file))
                    print(
                        "\n[yellow]Creating user settings folder[/] :white_check_mark:"
                    )
                except FileExistsError:
                    print("\n[green]User settings folder exists[/] :white_check_mark:")
                except OSError as e:
                    logger.error(
                        f"\n[red]Couldn't create user settings folder![/]\n{e}"
                    )
                    app_exit(1, -1)

                shutil.copy(self.default_file, self.user_file)
                print(f"[green]Copied default settings[/] :white_check_mark:")

                if Confirm.ask("[cyan]Customise user settings now?[/]"):

                    webbrowser.open(self.user_file)  # Technically unsupported method
                    print("\n[yellow]Run again to validate settings.[/]")
                    app_exit(0, -1)

                else:

                    print("[green]Exiting...\n[/]")
                    app_exit(0)

            except KeyboardInterrupt:
                print("[yellow]User aborted...\n[/]")
                app_exit(1, -1)

    def _ensure_user_keys(self):
        """Ensure user settings have all keys in default settings"""

        diffs = DeepDiff(self.default_settings, self.user_settings)

        # Check for unknown settings
        if diffs.get("dictionary_item_added"):
            [
                logger.warning(f"Unknown setting -> {x} will be ignored!")
                for x in diffs["dictionary_item_added"]
            ]

        # Check for missing settings
        if diffs.get("dictionary_item_removed"):
            [
                logger.error(f"Missing setting -> {x}")
                for x in diffs["dictionary_item_removed"]
            ]
            logger.critical(
                "Can't continue. Please define missing settings! Exiting...\n"
            )

            # # TODO: Figure out how to copy defaults...
            # Prompt user to add missing settings
            # if not typer.confirm("Can't continue without all settings defined! Copy defaults?"):
            #     app_exit(1)
            # # Copy defaults to user settings
            # with open(self.user_file, "r+") as file_:
            #     user_settings = self.yaml.load(file_)

            #     for diff in diffs["dictionary_item_removed"]:
            #         bracketed_strings = re.findall(r"[^[]*\[\'([^]]*)'\]", diff)

            #         if bracketed_strings: # Minus value
            #             dict_path = bracketed_strings[::-1]

            #             # Get key path
            #             def_val = reduce(operator.getitem, dict_path, self.default_settings)
            #             print(def_val)

            #     if key not in user_settings:

            #         logger.warning(
            #             f"Adding missing key '{key}' to user settings with value '{value}'"
            #         )
            #         user_settings[key] = value
            #         print(self.yaml.dump(user_settings))

            #         # file_.seek(0)
            #         # file_.truncate()
            #         # self.yaml.dump(user_settings, file_)

    def _validate_schema(self, settings):
        """Validate user settings against schema"""

        logger.debug(f"Validating user settings against schema")

        try:

            settings_schema.validate(settings)

        except SchemaError as e:

            logger.error(f"Error validating settings: {e}")
            app_exit(1)
