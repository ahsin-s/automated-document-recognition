import os
import pathlib

from PIL import Image
import pdf2image
from PyPDF2 import PdfReader
import pytesseract


def joinpath(path1, path2):
    return os.path.join(path1, path2)


def add_page_number_suffix(filepath, page_number):
    path = pathlib.Path(filepath)
    new_path = path.with_name(path.stem + f"-{page_number}").with_suffix(f"{path.suffix}")
    return str(new_path)


def change_extension(filepath, new_extension):
    p = pathlib.Path(filepath)
    new_extension = "." + new_extension.lstrip(".")
    return str(p.with_suffix(new_extension))


def get_base(filepath):
    p = pathlib.Path(filepath)
    return p.stem


def parse_directory_to_txt(directory_path):
    folders = [os.path.join(directory_path, m) for m in os.listdir(directory_path)]

    dataset = {}
    for folder in folders:
        files = [joinpath(folder, m) for m in os.listdir(folder) if m.endswith("pdf")]
        dataset[get_base(folder)] = {}
        for file in files:
            pdf_images = pdf2image.convert_from_path(file)
            page_pointer = {}
            file_pointer = {}
            for pagenum, page_image in enumerate(pdf_images, start=1):
                outputpath = change_extension(add_page_number_suffix(file, pagenum), "txt")

                # run ocr
                text = pytesseract.image_to_string(page_image)

                with open(outputpath, "w") as f:
                    f.write(text)
                print(f"Success: {outputpath}")
                page_pointer[pagenum] = outputpath
            file_pointer[get_base(file)] = page_pointer

            dataset.get(get_base(folder)).update(file_pointer)
    return dataset
