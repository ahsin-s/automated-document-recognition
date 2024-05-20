import os
import pickle
import logging
import traceback
from io import BytesIO
from pathlib import Path

import boto3
import pdf2image
from PIL import Image
from PyPDF2 import PdfReader


def get_document_bucket_and_key():
    # s3 path to the object
    if not 'BUCKET' in os.environ:
        logging.error("Environment variable named BUCKET not found, specify BUCKET environment variable")
    bucket = os.environ['BUCKET']

    if not 'OBJECT_KEY' in os.environ:
        logging.error("Environment variable named OBJECT_KEY not found, specify OBJECT_KEY environment variable")
    object_key = os.environ['OBJECT_KEY']
    return bucket, object_key


def download_s3_object_to_stream(bucket_name, object_key):
    s3 = boto3.client('s3')
    stream = BytesIO()
    s3.download_fileobj(bucket_name, object_key, stream)
    stream.seek(0)
    return stream


def upload_stream_to_s3(stream: BytesIO, bucket_name, object_key):
    s3 = boto3.client('s3')
    stream.seek(0)
    s3.upload_fileobj(stream, bucket_name, object_key)
    return True


def upload_file_s3(filepath, bucket_name, object_key):
    s3 = boto3.client('s3')
    try:
        s3.upload_file(filepath, bucket_name, object_key)
    except Exception:
        logging.error(traceback.format_exc())
        return False
    return True


def load_page_classification_model():
    return pickle.load(open("model/model.pickle", "rb"))


def run_ocr(bucket, object_key):
    textract = boto3.client('textract')

    pdf_stream = download_s3_object_to_stream(bucket, object_key)
    page_images: [Image] = pdf2image.convert_from_bytes(pdf_stream.getvalue())
    # upload the page images to the same s3 bucket
    # run ocr on each page image
    ocr_pages = {}
    for pgnum, im in enumerate(page_images, start=1):
        image_object_key = str(Path(object_key).with_name(
            Path(object_key).stem + f" - page {pgnum} of {len(page_images)}.png")
        )
        with open("temp.png", "wb") as fp:
            im.save(fp, "png")
        upload_file_s3("temp.png", bucket, image_object_key)
        logging.info("page image uploaded to s3")

        # call the textract api
        response = textract.detect_document_text(
            Document={
                'S3Object': {
                    'Bucket': bucket,
                    'Name': image_object_key
                }
            }
        )
        blocks = response['Blocks']
        pagetext = ""
        for block in blocks:
            if block['BlockType'] != 'PAGE':
                pagetext += " " + block['Text']
        ocr_pages[pgnum] = pagetext

        # upload the page text
        text_object_key = str(Path(image_object_key).with_suffix(".txt"))
        with open("temp.txt", "w") as fp:
            fp.write(pagetext)
        upload_file_s3("temp.txt", bucket, text_object_key)
    return ocr_pages
