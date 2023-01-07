import numpy as np
import pandas as pd
from torch.utils.data import DataLoader
from AnomalyDetection.dagmm2.Config import Config


class Loader(object):
    def __init__(self, data_path):
        df = pd.read_csv(data_path)
        # 特征不包含第一列(people)
        data = df[Config.train_col]
        # 归一化
        data = data.apply(lambda x: (x - np.min(x)) / (np.max(x) - np.min(x)))

        self.data = data.to_numpy()

    def __len__(self):
        return self.data.shape[0]

    def __getitem__(self, index):
        return np.float32(self.data[index])


def get_loader(data_path, batch_size):
    """Build and return data loader."""

    dataset = Loader(data_path)

    data_loader = DataLoader(dataset=dataset, batch_size=batch_size)
    return data_loader
