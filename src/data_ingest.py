import datetime
import os.path
import traceback
from pathlib import Path
from typing import List, Generator

import pandas as pd

from document_processors import extension_processing_config


def find_paths(root: str, extensions) -> List[Generator[Path]]:
    return [Path(root).glob(f"**/*.{extension.lstrip('.')}") for extension in extensions]


def crawl_directories(crawler_config: dict) -> List[Generator[Path]]:
    directories: List[str] = crawler_config['directories']
    extensions: List[str] = crawler_config['extensions']

    all_paths = []
    for directory in directories:
        all_paths += find_paths(directory, extensions)

    return all_paths


def create_crawler_meta(paths: List[Generator: Path]) -> pd.DataFrame:
    df = pd.DataFrame(columns=["absolute_path", "extension", "status", "error_messages", "last_update_datetime"])

    for path in paths:
        row = pd.DataFrame(
            {"absolute_path": [str(path)],
             "extension": [path.suffix],
             "status": ["Not Started"],
             "error_messages": [""],
             "last_update_datetime": [datetime.datetime.now().isoformat()]}
        )
        df = pd.concat([df, row], ignore_index=True)
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
    for i in range(crawler_meta.shape[0]):
        status = crawler_meta.loc[i, 'status']
        if status == "Done":
            continue
        crawler_meta.loc[i, 'status'] = "In Progress"
        try:
            absolute_path = crawler_meta.loc[i, 'absolute_path']
            processed: List[str] = extension_processing_config[crawler_meta.loc[i, 'extension']](absolute_path)
            extracted_meta = persist_extracted_text(processed, absolute_path, crawler_config["destination_directory"])
            persisted_meta = update_persisted_meta(extracted_meta, absolute_path, persisted_meta)
            crawler_meta.loc[i, 'status'] = "Done"
        except:
            crawler_meta.loc[i, 'error_messages'] = traceback.format_exc()
            crawler_meta.loc[i, 'status'] = "Failed"

        crawler_meta.loc[i, 'last_update_datetime'] = datetime.datetime.now().isoformat()
    return persisted_meta
