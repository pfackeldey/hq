from __future__ import annotations

import requests


class HQBaseConnection:
    __slots__ = ("host", "port")

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port

    def __enter__(self):
        if not self.ping():
            raise Exception(f"Failed to connect to HQ server at {self.url}")
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        del exc_type, exc_value, traceback

    def ping(self):
        return requests.get(f"{self.url}/status").ok

    @property
    def url(self) -> str:
        return f"{self.host}:{self.port}"
