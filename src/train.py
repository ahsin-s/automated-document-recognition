import os
import sys
import pickle

from preprocess import parse_directory_to_txt
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression

sys.path.append("../")  # add parent directory to path


def train(path_to_dataset):
    dataset = parse_directory_to_txt("sample_data")
    """
    dataset has the structure 
    {
      "label1": {
        "filename1": {"pagenum": "path_to_txt", "pagenum2": "path_to_txt2"}, 
        "filename2": ("pagenum": "path_to_txt"}
      }
    }
    """

    X = []
    Y = []
    for label, files in dataset.items():
        for file, pages in files.items():
            for pagenum, path_to_txt in pages.items():
                s = open(path_to_txt).read()
                X.append(s)
                Y.append(label)

    # train a bag of words model
    vect = TfidfVectorizer(min_df=0.1, max_df=0.9, max_features=150, ngram_range=(2, 3), stop_words="english")
    vect.fit(X)
    tf_X = vect.transform(X)

    model = LogisticRegression()
    model.fit(tf_X, Y)

    with open("trained_model.pkl") as f:
        pickle.dump(model, f)
    print("Trained model saved")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--path_to_training_data",
        type=str
    )
    args = parser.parse_args()
    dataset_path = args.path_to_training_data
    print("Launching training")
    train(dataset_path)
