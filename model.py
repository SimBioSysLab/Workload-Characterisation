import logging
import pprint

import numpy as np
import pandas as pd
from sklearn.cluster import AgglomerativeClustering

from loadconfig import config
from utils import get_all_cleaned_files


def building_model(dataset):

    del dataset["Hostname"]
    del dataset["Timestamp"]
    del dataset["Unnamed: 0"]
    del dataset["DiskNumber"]

    dataset = dataset.dropna()

    print("Starting building the model")
    clustering = AgglomerativeClustering(n_clusters=3, affinity="manhattan", linkage="complete")

    clustering.fit(dataset)
    print(clustering.labels_)
    np.savetxt(X=clustering.labels_, fname="results.txt", fmt="%d")
    pprint.pprint(clustering.labels_)
    logging.info(clustering.children_)
    logging.info(clustering.n_clusters_)


def run():
    files = get_all_cleaned_files()
    dataset = pd.read_csv(files[0])
    building_model(dataset)


if __name__ == '__main__':
    run()
