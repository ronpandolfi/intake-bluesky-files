import tifffile
import numpy as np
import pytest
import os
from intake_bluesky_files import FileHandlerPlugin


@pytest.fixture
def data_files(tmp_path):
    paths = []
    for i in range(3):
        for j in range(4):
            data = np.random.random((1000, 1000))
            path = os.path.join(tmp_path, f'sample_{i}_frame_{j}.tif')
            paths.append(path)
            tifffile.imsave(path, data)
    return paths


def test_ingest(data_files):
    docs = list(FileHandlerPlugin.ingestor.ingest(data_files))
    assert len(docs) == 39  # 1 start, 1 descriptor, 12 resources, 12 datums, 12 events, 1 stop
