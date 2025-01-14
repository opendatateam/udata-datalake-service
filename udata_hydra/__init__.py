import importlib.metadata
import logging
import os
import tomllib
from pathlib import Path

log = logging.getLogger("udata-hydra")


class Configurator:
    """Loads a dict of config from TOML file(s) and behaves like an object, ie config.VALUE"""

    configuration: dict = {}

    def __init__(self):
        if not self.configuration:
            self.configure()

    def configure(self) -> None:
        # load default settings
        with open(Path(__file__).parent / "config_default.toml", "rb") as f:
            configuration: dict = tomllib.load(f)

        # override with local settings
        local_settings = os.environ.get("HYDRA_SETTINGS", Path.cwd() / "config.toml")
        if Path(local_settings).exists():
            with open(Path(local_settings), "rb") as f:
                configuration.update(tomllib.load(f))

        self.configuration = configuration
        self.check()

        # add project metadata to config
        self.configuration["APP_NAME"] = "udata-hydra"
        self.configuration["APP_VERSION"] = importlib.metadata.version("udata-hydra")

    def override(self, **kwargs) -> None:
        self.configuration.update(kwargs)
        self.check()

    def check(self) -> None:
        """Sanity check on config"""
        assert self.MAX_POOL_SIZE >= self.BATCH_SIZE, "BATCH_SIZE cannot exceed MAX_POOL_SIZE"

    def __getattr__(self, __name):
        return self.configuration.get(__name)

    @property
    def __dict__(self):
        return self.configuration


config = Configurator()
