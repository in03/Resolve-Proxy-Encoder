import logging
import operator
import os
import re
import shutil
import webbrowser
from functools import reduce
from operator import getitem
from pathlib import Path
from functools import reduce
from typing import Union, Any

from deepdiff import DeepDiff
from rich import print
from rich.prompt import Confirm, Prompt
from ruamel.yaml import YAML
from yaspin import yaspin

from schema import SchemaError

from ..app.utils import core
from .schema import settings_schema

core.install_rich_tracebacks()
logger = logging.getLogger(__name__)

DEFAULT_SETTINGS_FILE = os.path.join(
    os.path.dirname(__file__),
    "default_settings.yml",
)

USER_SETTINGS_FILE = os.path.join(
    Path.home(),
    ".config",
    "resolve_proxy_encoder",
    "user_settings.yml",
)


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class SettingsManager(metaclass=Singleton):
    def __init__(
        self,
        default_settings_file=DEFAULT_SETTINGS_FILE,
        user_settings_file=USER_SETTINGS_FILE,
    ):

        self.yaml = YAML()
        self.yaml.indent(mapping=2, sequence=4, offset=2)
        self.yaml.default_flow_style = False

        self.default_file = default_settings_file
        self.user_file = user_settings_file
        self.user_settings = dict()

        # Originally had default settings validated against schema too
        # but realised testing a path exists is not a good idea for defaults.
        # Instead let's write a build time test for this.

        self._load_default_file()

        # Validate user settings
        if logger.getEffectiveLevel() > 2:

            self.spinner = yaspin(
                text="Checking settings...",
                color="cyan",
            )
            self.spinner.start()

        self._ensure_user_file()
        self._load_user_file()
        self._ensure_user_keys()
        self._validate_schema()

        self.spinner.ok("✅ ")

    def __len__(self):

        return len(self.user_settings)

    def __getitem__(self, __items):

        if type(__items) == str:
            __items = __items.split(" ")

        return reduce(operator.getitem, __items, self.user_settings)

    def _load_default_file(self):
        """Load default settings from yaml"""

        logger.debug(f"Loading default settings from {self.default_file}")

        with open(os.path.join(self.default_file)) as file:
            self.default_settings = self.yaml.load(file)

    def _load_user_file(self):
        """Load user settings from yaml"""

        logger.debug(f"Loading user settings from {self.user_file}")

        with open(self.user_file, "r") as file:
            self.user_settings = self.yaml.load(file)

    def update_nested_setting(self, key_list, value):
        """
        Update nested user settings with key list.

        Fully loads and rewrites the user settings file.
        As such, invalid keys and values will be left out of the updated file.

        Args:
            - self (contains user settings file path)
            - key_list (list of keys to access nested key/val)
            - value (new value to update)

        Returns:
            Nothing

        Raises:
            Nothing

        """

        reduce(getitem, key_list[:-1], self.user_settings)[key_list[-1]] = value

        with open(self.user_file, "w") as file_:

            logger.debug(f"[magenta]Writing updated settings to '{self.user_file}'")
            self.yaml.dump(self.user_settings, file_)

        return

    def _ensure_user_file(self):
        """Copy default settings to user settings if it doesn't exist

        Prompt the user to edit the file afterwards, then exit.
        """

        logger.debug(f"Ensuring settings file exists at {self.user_file}")

        if not os.path.exists(self.user_file):

            self.spinner.fail("❌ ")
            if Confirm.ask(
                f"[yellow]No user settings found at path [/]'{self.user_file}'\n"
                + "[cyan]Load defaults now for adjustment?[/]"
            ):
                print()  # Newline
                self.spinner.text = "Copying default settings..."
                self.spinner.start()

                # Create dir, copy file, open
                try:

                    os.makedirs(os.path.dirname(self.user_file))

                except FileExistsError:

                    self.spinner.stop()
                    logger.info("[yellow]Directory exists, skipping...[/]")
                    self.spinner.start()

                except OSError:

                    self.spinner.fail("❌ ")
                    logger.error("[red]Error creating directory![/]")
                    core.app_exit(1, -1)

                try:

                    shutil.copy(self.default_file, self.user_file)
                    self.spinner.ok("✅ ")

                except:

                    self.spinner.fail("❌ ")
                    print(
                        f"[red]Couldn't copy default settings to {self.user_file}![/]"
                    )
                    core.app_exit(1, -1)

                self.spinner.stop()
                webbrowser.open(self.user_file)  # Technically unsupported method

            core.app_exit(0)

    def _ensure_user_keys(self):
        """Ensure user settings have all keys in default settings"""

        self.spinner.stop()

        diffs = DeepDiff(self.default_settings, self.user_settings)
        logger.debug("[magenta]Diffs:[/]\n")

        # Check for unknown settings
        if diffs.get("dictionary_item_added"):

            # listcomp log warning for each unknown setting
            [
                logger.warning(
                    f'[yellow]Unknown setting[/] [white]"{x}"[/]'
                    "[yellow] will be ignored![/]"
                )
                for x in diffs["dictionary_item_added"]
            ]
            print()  # Newline

        def _prompt_setting_substitution(self, setting_name, key_list):

            # Get default value
            if len(key_list) > 0:
                logger.debug("Getting default value from nested keys")
                default_value = reduce(dict.get, key_list, self.default_settings)

            else:
                logger.debug("Standard dict lookup")
                default_value = self.default_settings[key_list]

            print(f'[red bold]Missing setting [green]"{setting_name}"[/][/]')

            try:

                custom_value = Prompt.ask(
                    f"[cyan]Type a new value or leave blank to use default[/] ('{default_value}')"
                )

            except KeyboardInterrupt:

                print()
                self.spinner.fail("❌ ")
                # Log all missing settings so user doesn't have to know each missing.
                [
                    logger.error(f'[red bold]Missing setting "{x}"')
                    for x in diffs["dictionary_item_removed"]
                ]

                print()
                print("[red bold]Cannot continue.\nPlease define missing settings!")
                core.app_exit(1, -1)

            else:

                if not custom_value:

                    print(f"[green]Using default '{default_value}'[/]")
                    custom_value = default_value

                else:

                    print(f"[green]Using custom value '{custom_value}'[/]")

                # TODO: Implement type validation for custom value and prompt retry on fail
                self.update_nested_setting(key_list, custom_value)

            finally:

                print()

        def _get_missing_settings(self):

            """
            Prompt substitution for settings present in default-settings that are not present in user-settings.

            Calls `_confirm_setting_substitution` if settings require substitution.

            """

            if diffs.get("dictionary_item_removed"):

                self.spinner.stop()
                logger.error(
                    f"[bold red]Required user settings are missing!\n[/]"
                    f"[yellow]Follow prompts to substitute missing settings with default or custom values.\n"
                    f"Or edit settings manually here and re-run: '{self.user_file}'\n"
                )
                print()

                for x in diffs["dictionary_item_removed"]:

                    # Match all keys in diff string
                    key_list = re.findall(r"\['(\w*)'\]", x)
                    _prompt_setting_substitution(self, x, key_list)

            def _catch_empty_setting_section_with_inline_comment(self):

                """
                Catch setting sections that don't show as empty because of inline comments.

                Diff usually gives `dictionary_item_removed` when a section is empty, but
                will actually show `type_changes` with `ruamel.yaml.comments.CommentedMap` as `old_type`
                if the setting section has an inline comment after the setting section key.
                This leads to inline comments breaking setting-substitution if all child settings are missing.
                Bit of an edge case really.

                Calls `_confirm_setting_substitution` if settings require substitution.

                Args:
                    self
                Returns:
                    Nothing
                Raises:
                    Nothing
                """

                if diffs.get("type_changes"):

                    self.spinner.stop()

                    # Match root keys with entire missing sections except for an inline comment
                    empty_root_keys = re.findall(
                        r"root\['(\w*)'\]\": {'old_type': <class 'ruamel\.yaml\.comments\.CommentedMap'>",
                        str(diffs),
                    )

                    # If only value is a comment, section is empty
                    for key in empty_root_keys:

                        # TODO: Fix this. `_prompt_setting_substitution` expects a list
                        # this is not the way to do it
                        # labels: enhancement

                        single_root_key_list = []
                        single_root_key_list.append(key)
                        _prompt_setting_substitution(
                            self, "".join(key), single_root_key_list
                        )

            _catch_empty_setting_section_with_inline_comment(self)

        _get_missing_settings(self)

        print()

    def _validate_schema(self):
        """Validate user settings against schema"""

        logger.debug(f"Validating user settings against schema")

        try:

            settings_schema.validate(self.user_settings)

        except SchemaError as e:

            self.spinner.fail("❌ ")
            logger.error(
                f"[red]Couldn't validate application settings![/]\n{e}\n"
                + f"[red]Exiting...[/]\n"
            )
            core.app_exit(1, -1)

    def update(self, dict_: dict):
        logger.info(f"[yellow]Reconfigured settings:\n{dict_}")
        self.user_settings.update(dict_)
