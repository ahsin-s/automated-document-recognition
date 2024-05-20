import os
import tempfile
from pathlib import Path
from typing import List

import pytest
import pandas as pd

from src.data_ingest import crawl_directories, create_crawler_meta, persist_extracted_text, update_persisted_meta, crawl


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


@pytest.fixture
def source_document_path(tmp_path):
    return str(tmp_path.joinpath("/label1/label1.pdf"))


@pytest.fixture
def pages():
    return ["here is a story", "quick brown fox", "jumps over the lazy dog"]


@pytest.fixture
def persisted_text(tmp_path, pages, source_document_path) -> List[str]:
    return persist_extracted_text(pages, source_document_path, str(tmp_path))


@pytest.fixture
def extracted_meta(tmp_path):
    return [str(tmp_path.joinpath(f"label1/label1 -- {pgnum} of 4.txt")) for pgnum in range(1, 5)]


@pytest.fixture
def persisted_metadata(tmp_path, extracted_meta, source_document_path):
    return update_persisted_meta(extracted_meta, source_document_path, pd.DataFrame([]))


@pytest.fixture
def post_processed_crawler_meta(crawler_config, crawler_meta) -> pd.DataFrame:
    persisted_meta, updated_crawler_meta = crawl(crawler_meta, crawler_config)
    return updated_crawler_meta


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


def test_persisted_text_len(persisted_text: List):
    assert len(persisted_text) == 3, "validation failed for persisted metadata"


def test_persisted_text_exists(persisted_text):
    assert Path(persisted_text[0]).exists(), "the file wasn't persisted"


def test_persisted_text_ends_with(persisted_text):
    expected_filename = " -- Page 1 of 3.txt"
    assert Path(persisted_text[0]).name.endswith(expected_filename), "mistmatch in expected filename"


def test_persisted_text_path_is_absolute(persisted_text):
    assert Path(persisted_text[0]).absolute() == Path(
        persisted_text[0]), "expected an absolute path for the persisted text metadata"


def test_update_persisted_meta(persisted_metadata):
    assert set(persisted_metadata.columns) == {"source_absolute_path", "persisted_path",
                                               "last_update_datetime"}, "schema of persisted metadata mistmatches"


def test_update_persisted_meta_rowcount(persisted_metadata):
    assert persisted_metadata.shape[0] == 4, "mistmatch row count of persisted metadata"


def test_rerunning_persisted_meta_rowcount(extracted_meta, source_document_path, persisted_metadata):
    original_rowcount = persisted_metadata.shape[0]
    iterations = 5
    expected_new_rowcount = len(extracted_meta) * iterations + original_rowcount
    for _ in range(iterations):
        persisted_metadata = update_persisted_meta(extracted_meta, source_document_path, persisted_metadata)
    assert persisted_metadata.shape[
               0] == expected_new_rowcount, "re-running update of persisted metadata rowcount doesn't match expectation"


def test_processed_crawler_meta(post_processed_crawler_meta):
    pass 
