from cloudphysics.utils import df_dataset_path
from sklearn.cluster import KMeans
import pandas as pd


def build_model():
    dataset = pd.read_csv(df_dataset_path())
    print(dataset)
    del dataset["filename"]
    kmeans_model = KMeans(n_clusters=2).fit()


def run_model():
    build_model()
