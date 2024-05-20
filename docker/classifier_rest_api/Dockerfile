FROM python:3.9

COPY docker/classifier_rest_api/requirements.txt .
RUN pip install --no-cache -r requirements.txt

COPY src/ .
EXPOSE 80
CMD ["fastapi", "run", "app.py", "--port", "80"]