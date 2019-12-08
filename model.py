import logging
import pprint

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.cluster.hierarchy import dendrogram, linkage
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

    clustering.fit(dataset.head(36000))
    print(clustering.labels_)
    np.savetxt(X=clustering.labels_, fname="results.txt", fmt="%d")
    pprint.pprint(clustering.labels_)
    logging.info(clustering.children_)
    logging.info(clustering.n_clusters_)


def linkage_heirarchy(dataset):
    del dataset["Hostname"]
    del dataset["Timestamp"]
    del dataset["Unnamed: 0"]
    del dataset["DiskNumber"]

    reduced_dataset = dataset.head(24000).dropna()
    ip_dataset = linkage(reduced_dataset, "average")
    dn = dendrogram(Z=ip_dataset, orientation="right", p=3, truncate_mode="level")
    # print(dn)
    plt.savefig("results.png")
    leaves_list = dn["leaves"]
    print(leaves_list)
    # print(ip_dataset)


def run():
    files = get_all_cleaned_files()
    dataset = pd.read_csv(files[0])
    # building_model(dataset)
    linkage_heirarchy(dataset)


if __name__ == '__main__':
    run()
