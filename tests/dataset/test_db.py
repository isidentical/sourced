from __future__ import annotations

from pathlib import Path

from daedalus.dataset.db import (
    DEFAULT_DATASET_CACHE_FILE_NAME,
    Dataset,
    GlobalStore,
    Source,
)


def test_cache(tmp_path):
    cache_dir = Path(tmp_path) / "cache"
    cache_dir.mkdir(parents=True)

    dataset = Dataset(
        name="test",
        path=cache_dir,
        sources=[
            Source(name="source1", path=cache_dir / "source1"),
            Source(name="source2", path=cache_dir / "source2"),
        ],
    )
    dataset.cache()

    assert (cache_dir / DEFAULT_DATASET_CACHE_FILE_NAME).exists()
    cached_dataset = Dataset.from_cache(cache_dir)

    assert dataset.name == cached_dataset.name
    assert dataset.path == cached_dataset.path
    assert dataset.sources == cached_dataset.sources

    dataset.sources.append(Source(name="source3", path=cache_dir / "source3"))
    dataset.cache()

    cached_dataset = Dataset.from_cache(cache_dir)
    assert dataset.name == cached_dataset.name
    assert dataset.path == cached_dataset.path
    assert len(cached_dataset.sources)


def test_global_store_non_existent(tmp_path):
    global_path = tmp_path / "global"
    global_path.mkdir(parents=True)

    global_store_path = global_path / "datasets.json"
    store = GlobalStore.from_file(global_store_path)
    assert store.datasets == {}

    assert not global_store_path.exists()

    store.cache()
    assert global_store_path.exists()

    store = GlobalStore.from_file(global_store_path)
    assert store.datasets == {}


def test_global_store(tmp_path):
    pypi_path = tmp_path / "pypi"
    pypi_path.mkdir(parents=True)

    dataset_pypi = Dataset(
        name="pypi",
        path=pypi_path,
        sources=[
            Source(name="pypi", path=pypi_path / "pypi"),
        ],
    )

    global_path = tmp_path / "global"
    global_path.mkdir(parents=True)

    global_store_path = global_path / "datasets.json"
    store = GlobalStore(path=global_store_path)
    store.cache()

    # This dataset does not exist yet
    store = GlobalStore.from_file(path=global_store_path)
    assert store.datasets == {}

    # Create the dataset
    store.datasets["pypi"] = dataset_pypi
    dataset_pypi.cache()
    store.cache()

    store = GlobalStore.from_file(global_store_path)
    assert store.datasets == {"pypi": dataset_pypi}

    # Add a new dataset
    conda_path = tmp_path / "conda"
    conda_path.mkdir(parents=True)

    dataset_conda = Dataset(
        name="conda",
        path=conda_path,
        sources=[
            Source(name="conda", path=conda_path / "conda"),
        ],
    )
    dataset_conda.cache()

    store = GlobalStore.from_file(global_store_path)
    assert store.datasets == {"pypi": dataset_pypi}

    store.datasets["conda"] = dataset_conda
    store.cache()

    store = GlobalStore.from_file(global_store_path)
    assert store.datasets == {"pypi": dataset_pypi, "conda": dataset_conda}

    # Remove a dataset
    del store.datasets["conda"]
    store.cache()

    store = GlobalStore.from_file(global_store_path)
    assert store.datasets == {"pypi": dataset_pypi}
