from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any

DAEDALUS_CACHE_FILE = "dataset.json"
JSONType = dict[str, Any]


@dataclass
class Dataset:
    name: str
    path: Path
    sources: list[Source] = field(default_factory=list)

    @classmethod
    def from_cache(cls, cache_dir: Path) -> Dataset:
        meta_path = cache_dir / DAEDALUS_CACHE_FILE
        if not meta_path.exists():
            raise FileNotFoundError(
                f"Dataset metadata file ({meta_path}) was not found."
            )

        with open(meta_path) as stream:
            return cls.from_json(json.load(stream))

    def cache(self) -> None:
        """Cache the dataset metadata."""

        meta_path = self.path / DAEDALUS_CACHE_FILE
        with open(meta_path, "w") as stream:
            json.dump(self.to_json(), stream, indent=4)

    def to_json(self) -> JSONType:
        return {
            "name": self.name,
            "path": str(self.path),
            "sources": [source.to_json() for source in self.sources],
        }

    @classmethod
    def from_json(cls, json_data: JSONType) -> Dataset:
        return cls(
            name=json_data["name"],
            path=Path(json_data["path"]),
            sources=[Source.from_json(source) for source in json_data["sources"]],
        )


class SourceStatus(Enum):
    AWAITING_DOWNLOAD = auto()
    DOWNLOADED = auto()
    SKIPPED = auto()


@dataclass
class Source:
    name: str
    path: Path | None = None
    status: SourceStatus = SourceStatus.AWAITING_DOWNLOAD

    @classmethod
    def from_json(cls, json_data: JSONType) -> Source:
        return cls(
            name=json_data["name"],
            path=Path(json_data["path"]) if json_data["path"] is not None else None,
            status=SourceStatus(json_data["status"]),
        )

    def to_json(self) -> JSONType:
        return {
            "name": self.name,
            "path": str(self.path) if self.path is not None else None,
            "status": self.status.name,
        }
