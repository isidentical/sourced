from __future__ import annotations

from pathlib import Path

from daedalus.dataset import db


def test_cache(tmp_path):
    cache_dir = Path(tmp_path) / "cache"
    cache_dir.mkdir(parents=True)

    dataset = db.Dataset(
        name="test",
        path=cache_dir,
        sources=[
            db.Source(name="source1", path=cache_dir / "source1"),
            db.Source(name="source2", path=cache_dir / "source2"),
        ],
    )
    dataset.cache()

    assert (cache_dir / db.DAEDALUS_CACHE_FILE).exists()
    cached_dataset = db.Dataset.from_cache(cache_dir)

    assert dataset.name == cached_dataset.name
    assert dataset.path == cached_dataset.path
    assert dataset.sources == cached_dataset.sources

    dataset.sources.append(db.Source(name="source3", path=cache_dir / "source3"))
    dataset.cache()

    cached_dataset = db.Dataset.from_cache(cache_dir)
    assert dataset.name == cached_dataset.name
    assert dataset.path == cached_dataset.path
    assert len(cached_dataset.sources)
