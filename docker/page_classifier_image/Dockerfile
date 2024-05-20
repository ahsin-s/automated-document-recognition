FROM python:3.10-slim
RUN apt-get update -y && \
    apt-get install poppler-utils -y

COPY docker/page_classifier_image/requirements.txt .
RUN pip install --no-cache -r requirements.txt

COPY src/ .
ENTRYPOINT ["python", "page_classification_model.py"]