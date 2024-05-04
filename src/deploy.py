import fastapi
import pickle

# Create a FastAPI app
app = FastAPI()


# Load the machine learning model from the pickle file
with open('model.pkl', 'rb') as file:
    model = pickle.load(file)


@app.post("/predict")
def predict(item):
    data = item.data
    prediction = model.predict([data])
    return {"prediction": prediction[0]}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)