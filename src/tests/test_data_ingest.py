import os
import tempfile
from pathlib import Path

import pytest

from src.data_ingest import crawl_directories, create_crawler_meta, persist_extracted_text


@pytest.fixture
def init_testing():
    pass


@pytest.fixture
def crawler_config():
    return {
        "directories": [os.getcwd() + os.sep + "test_data"],
        "extensions": [".pdf"],
        "destination_directory": os.curdir + os.sep + "crawler_output",
    }


@pytest.fixture
def paths():
    data = [r"C:\code\adr\label1\1.pdf",
            r"C:\code\adr\label1\2.pdf",
            r"C:\code\adr\label2\1.pdf",
            r"C:\code\adr\label2\2.pdf"]
    return [Path(d) for d in data]


@pytest.fixture
def crawler_meta(paths):
    return create_crawler_meta(paths)

def test_crawl_directories(crawler_config):
    res = crawl_directories(crawler_config)

    # test that the path is an absolute path
    temp = res.__next__()
    assert str(temp.absolute()) == str(temp), "Expected crawl_directories to return absolute paths of files."


def test_crawler_meta_shape(crawler_meta):
    assert crawler_meta.shape[0] > 1, "Expected dataframe with at least 1 returned path"


def test_crawler_meta_row_count(crawler_meta):
    assert crawler_meta.shape[0] == 4, "Expected dataframe to have 4 rows"


def test_crawler_meta_schema(crawler_meta):
    assert set(crawler_meta.columns) == {"absolute_path", "extension", "status", "error_messages",
                                         "last_update_datetime"}, "Failed dataframe schema validation"


def test_persist_extracted_text():
    with tempfile.TemporaryDirectory() as tempdir:
        persisted = persist_extracted_text(["I am a brown bear"], r"C:\temp\label1\1.pdf", tempdir)
        assert len(persisted) == 1, "validation failed for persisted metadata"

        # check if the file exists
        expected_filename = "1 -- Page 1 of 1.txt"
        assert Path(persisted[0]).exists(), "the file wasn't persisted"
        assert Path(persisted[0]).name == expected_filename, "mistmatch in expected filename"

