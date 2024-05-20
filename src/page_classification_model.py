import json
from pathlib import Path

from utils import get_document_bucket_and_key, run_ocr, upload_file_s3


def main():
    bucket, object_key = get_document_bucket_and_key()
    # model = load_page_classification_model()  # todo: load can be from MlFlow or from local file

    ocr_text = run_ocr(bucket, object_key)

    inference = {}
    for pgnum, text in ocr_text.items():
        # run the classifier
        # prediction = model.predict_proba([text])  # todo
        # store the category and associated confidence value
        inference[pgnum] = {"category": "abc", "confidence": 0.8}

    # upload the inference as json
    with open("temp.json", "w") as fp:
        json.dump(inference, fp)

    upload_file_s3("temp.json", bucket, str(Path(object_key).with_name("page_classifications.json")))


if __name__ == "__main__":
    main()
