import os
import toml

CONNECT_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "connect.toml"))
SYNC_CONFIG = toml.load(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "source", "config", "sync_scheduler.toml"))