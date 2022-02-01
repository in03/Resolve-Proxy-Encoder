#!/usr/bin/env python3.6

import operator
import os
import re
import shutil
import webbrowser
from pathlib import Path

import typer
from deepdiff import DeepDiff
from resolve_proxy_encoder.helpers import (
    get_rich_logger,
    install_rich_tracebacks,
    app_exit,
)
from rich import print
from rich.console import Console
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

        self.console = Console()
        self.yaml = YAML()
        self.default_file = default_settings_file
        self.user_file = user_settings_file

        # TODO:
        # Originally had default settings validated against schema too
        # but realised testing a path exists is not a good idea for defaults.
        # Instead let's write a build time test for this.

        # Validate default settings
        self.default_settings = self._get_default_settings()

        # Validate user settings
        with self.console.status("[cyan]Checking settings...[/]\n"):

            self._ensure_user_file()
            self.user_settings = self._get_user_settings()
            self._ensure_user_keys()
            self._validate_schema(self.user_settings)

        print("\n[green]User settings are valid :white_check_mark:[/]\n")

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

            if typer.confirm(
                f"No user settings found at path {self.user_file}\n"
                + "Load defaults now for adjustment?"
            ):

                # Create dir, copy file, open
                try:
                    os.makedirs(os.path.dirname(self.user_file))
                except FileExistsError:
                    typer.echo("Directory exists, skipping...")
                except OSError:
                    typer.echo("Error creating directory!")
                    app_exit(1)

                shutil.copy(self.default_file, self.user_file)
                typer.echo(f"Copied default settings to {self.user_file}")
                typer.echo("Please customize as necessary.")
                webbrowser.open(self.user_file)  # Technically unsupported method

            app_exit(0)

    def _ensure_user_keys(self):
        """Ensure user settings have all keys in default settings"""

        # TODO: Can't seem to properly catch exceptions as SchemaWrongKeyError, etc.
        # Just generic SchemaError for now. If we can catch them, we don't need this func.
        # We can also use the default option in Schema to add default keys.
        # Then we can get rid of the default_settings.yml file.

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
                "Can't continue. Please define missing settings! Exiting..."
            )
            app_exit(1, -1)

    def _validate_schema(self, settings):
        """Validate user settings against schema"""

        logger.debug(f"Validating user settings against schema")

        try:

            settings_schema.validate(settings)

        except SchemaError as e:

            logger.error(
                f"[red]Couldn't validate application settings![/]\n{e}\n"
                + f"[red]Exiting...[/]\n"
            )
            app_exit(1, -1)
