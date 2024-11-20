import os
import toml

class config:
    CONNECT_CONFIG = {}
    SYNC_CONFIG = {}
    def __init__(self) -> None:
        self.update()

    def update(self) -> None:
        self.CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))
        self.SYNC_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "sync_scheduler.toml"))

gc = config()