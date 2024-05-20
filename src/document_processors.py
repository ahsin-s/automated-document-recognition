import traceback
from typing import List

from PIL import Image
import pytesseract


def pdf_processor(absolute_path: str):
    try:
        import pdf2image
    except ImportError:
        raise traceback.format_exc()

    return [pytesseract.image_to_string(im) for im in pdf2image.convert_from_path(absolute_path)]


def powerpoint_processor(absolute_path: str):
    raise NotImplementedError


def plaintext_processor(absolute_path: str):
    with open(absolute_path, "r") as f:
        return f.read()


def word_document_processor(absolute_path: str):
    raise NotImplementedError


extension_processing_config = {
    "pdf": pdf_processor,
    "ppt": powerpoint_processor,
    "txt": plaintext_processor,
    "py": plaintext_processor,
    "docx": word_document_processor
}
