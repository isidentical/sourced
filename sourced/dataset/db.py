from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any

from platformdirs import user_cache_path, user_config_path

GLOBAL_CONFIG_VERSION = "0.0.1"
GLOBAL_DATA_VERSION = "0.0.1"

SOURCED_CONFIG_DIR = user_config_path(
    "sourced",
    "sourced",
    version=GLOBAL_CONFIG_VERSION,
)
SOURCED_DATA_DIR = user_cache_path(
    "sourced",
    "sourced",
    version=GLOBAL_DATA_VERSION,
)

SOURCED_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
SOURCED_DATA_DIR.mkdir(parents=True, exist_ok=True)


DEFAULT_DATASET_CACHE_FILE_NAME = "datasets.json"
JSONType = dict[str, Any]


@dataclass
class Dataset:
    name: str
    path: Path
    sources: list[Source] = field(default_factory=list)

    @classmethod
    def from_cache(cls, cache_dir: Path) -> Dataset:
        meta_path = cache_dir / DEFAULT_DATASET_CACHE_FILE_NAME
        if not meta_path.exists():
            raise FileNotFoundError(
                f"Dataset metadata file ({meta_path}) was not found."
            )

        with open(meta_path) as stream:
            return cls.from_json(json.load(stream))

    def cache(self) -> None:
        """Cache the dataset metadata."""

        meta_path = self.path / DEFAULT_DATASET_CACHE_FILE_NAME
        temp_fd, temp_file = tempfile.mkstemp(suffix=meta_path.suffix, dir=self.path)
        with open(temp_fd, "w") as stream:
            json.dump(self.to_json(), stream)
        os.replace(temp_file, meta_path)

    def to_json(self) -> JSONType:
        # When serializing, use paths relative to the path of
        # this dataset.

        return {
            "name": self.name,
            "path": str(self.path),
            "sources": [
                source.to_json(
                    relative_to=self.path,
                )
                for source in self.sources
            ],
        }

    @classmethod
    def from_json(cls, json_data: JSONType) -> Dataset:
        dataset_path = Path(json_data["path"])
        return cls(
            name=json_data["name"],
            path=dataset_path,
            sources=[
                Source.from_json(source, relative_to=dataset_path)
                for source in json_data["sources"]
            ],
        )


class SourceStatus(Enum):
    AWAITING_DOWNLOAD = "AWAITING_DOWNLOAD"
    DOWNLOADED = "DOWNLOADED"
    SKIPPED = "SKIPPED"


@dataclass
class Source:
    name: str
    path: Path | None = None
    status: SourceStatus = SourceStatus.AWAITING_DOWNLOAD

    @classmethod
    def from_json(cls, json_data: JSONType, relative_to: Path) -> Source:
        if json_data["path"] is not None:
            path = relative_to / json_data["path"]
        else:
            path = None

        return cls(
            name=json_data["name"],
            path=path,
            status=SourceStatus(json_data["status"]),
        )

    def to_json(self, relative_to: Path) -> JSONType:
        if self.path is not None:
            path = str(self.path.relative_to(relative_to))
        else:
            path = None

        return {
            "name": self.name,
            "path": path,
            "status": self.status.name,
        }


@dataclass
class GlobalStore:
    path: Path
    datasets: dict[str, Dataset] = field(default_factory=dict)

    @classmethod
    def from_file(
        cls,
        path: Path = SOURCED_CONFIG_DIR / "store.json",
    ) -> GlobalStore:
        if not path.exists():
            return cls(path)

        with open(path) as stream:
            return cls.from_json(path, json.load(stream))

    @classmethod
    def from_json(cls, path: Path, json_data: JSONType) -> GlobalStore:
        datasets = {}
        for dataset in json_data["datasets"]:
            name = dataset["name"]
            metadata_file = Path(dataset["path"])
            try:
                dataset = Dataset.from_cache(metadata_file)
            except FileNotFoundError:
                print(
                    f"WARNING: Dataset '{name}' does not appear to be exist on the disk"
                    f" (no {metadata_file}). Skipping."
                )
            else:
                datasets[name] = dataset

        return cls(path, datasets=datasets)

    def to_json(self) -> JSONType:
        # Only include references to the actual dataset metadata
        # files, not the full dataset metadata.
        return {
            "datasets": [
                {"name": dataset.name, "path": str(dataset.path)}
                for dataset in self.datasets.values()
            ],
        }

    def cache(self) -> None:
        """Cache the global store metadata."""

        temp_fd, temp_file = tempfile.mkstemp(
            suffix=self.path.suffix, dir=self.path.parent
        )
        with open(temp_fd, "w") as stream:
            json.dump(self.to_json(), stream)
        os.replace(temp_file, self.path)


load = Dataset.from_cache
