from typing import Protocol, runtime_checkable


@runtime_checkable
class Language(Protocol):
    def analyze_pipes(self) -> dict | None: ...
