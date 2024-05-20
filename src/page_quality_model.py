import os
import json
import random
import logging
from pathlib import Path

from PyPDF2 import PdfReader

from utils import download_s3_object_to_stream, upload_file_s3, get_document_bucket_and_key


def main():

    bucket, object_key = get_document_bucket_and_key
    # load the file into PyPDF2 reader
    pdf = PdfReader(download_s3_object_to_stream(bucket, object_key))
    logging.info("Loaded pdf file")
    logging.debug(f"Page count: {len(pdf.pages)}")

    # for each page, assign some random quality score
    quality_scores = {"document": object_key, "pages": {}}
    for pagenum, page in enumerate(pdf.pages, start=1):
        quality_scores["pages"][pagenum] = random.randint(75, 100)

    logging.info("Quality scores for each page created")

    # save the quality score into a json in s3
    with open("page_quality_scores.json", "w") as fp:
        json.dump(quality_scores, fp)
    quality_scores_object_key = str(Path(object_key).with_name("page_quality.json"))
    logging.info(f"Uploading quality score json to {quality_scores_object_key}")
    upload_file_s3("page_quality_scores.json", bucket, quality_scores_object_key)
    logging.info("Done with quality score analysis")


if __name__ == "__main__":
    main()
