./src/train.py --path_to_training_data ./sample_data
docker build -t adr-app .
docker run -d --name adr-container -p 8000:8000 adr-app