FROM python:3.10-slim

COPY src/ .
COPY docker/page_quality_scoring_image/requirements.txt .
RUN pip install --no-cache -r requirements.txt

ENTRYPOINT ["python", "page_quality_model.py"]