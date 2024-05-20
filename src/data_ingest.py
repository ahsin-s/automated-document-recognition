import datetime
import os.path
import traceback
from pathlib import Path
from typing import List, Generator

import pandas as pd

from document_processors import extension_processing_config


def get_empty_crawler_df():
    return pd.DataFrame(columns=["absolute_path", "extension", "status", "error_messages", "last_update_datetime"])


def find_paths(root: str, extensions) -> List[Generator[Path, None, None]]:
    return [Path(root).glob(f"**/*.{extension.lstrip('.')}") for extension in extensions]


def crawl_directories(crawler_config: dict) -> Generator[Path, None, None]:
    directories: List[str] = crawler_config['directories']
    extensions: List[str] = crawler_config['extensions']

    all_paths = []
    for directory in directories:
        all_paths += find_paths(directory, extensions)

    return all_paths[0]


def create_crawler_meta(paths: Generator[Path, None, None]) -> pd.DataFrame:
    df = get_empty_crawler_df()

    for i, path in enumerate(paths):
        df.loc[i] = [str(path), path.suffix, "Not Started", "", datetime.datetime.now().isoformat()]
    return df


def persist_extracted_text(text: [str], source_absolute_path: str, destination_directory: str):
    path = Path(source_absolute_path)
    stem = path.stem
    destination_directory += os.path.sep + stem
    os.makedirs(destination_directory)

    page_count = len(text)
    persisted_destinations = []
    for pgnum, pagetext in enumerate(text, start=1):
        destination_filename = stem + f" -- Page {pgnum} of {page_count}.txt"
        destination_filename = destination_directory + os.path.sep + destination_filename
        with open(destination_filename, "w") as f:
            f.write(pagetext)
        persisted_destinations.append(destination_filename)
    return persisted_destinations


def update_persisted_meta(extracted_meta: List[str], source_document_path: str, persisted_meta: pd.DataFrame):
    for extracted_path in extracted_meta:
        row = pd.DataFrame({
            "source_absolute_path": [source_document_path],
            "persisted_path": [extracted_path],
            "last_update_datetime": [datetime.datetime.now().isoformat()]
        }
        )
        persisted_meta = pd.concat([persisted_meta, row], ignore_index=True)
    return persisted_meta


def crawl(crawler_meta: pd.DataFrame, crawler_config: dict) -> pd.DataFrame:
    persisted_meta = pd.DataFrame(columns=["source_absolute_path", "persisted_path", "last_update_datetime"])
    updated_crawler_config = get_empty_crawler_df()
    for rownum in range(crawler_meta.shape[0]):
        absolute_path, extension, status, error_messages, last_update_datetime = crawler_meta.iloc[rownum]
        if status == "Done":
            continue
        try:
            processed: List[str] = extension_processing_config[extension](absolute_path)
            extracted_meta = persist_extracted_text(processed, absolute_path, crawler_config["destination_directory"])
            persisted_meta = update_persisted_meta(extracted_meta, absolute_path, persisted_meta)
            status = "Done"
        except:
            error_messages = traceback.format_exc()
            status = "Failed"

        last_update_datetime = datetime.datetime.now().isoformat()
        updated_crawler_config.loc[rownum] = [absolute_path, extension, status, error_messages, last_update_datetime]
    return persisted_meta, updated_crawler_config
