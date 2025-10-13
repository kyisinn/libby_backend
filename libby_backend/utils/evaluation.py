import numpy as np

def calculate_rmse(actual, predicted):
    return np.sqrt(np.mean((np.array(actual)-np.array(predicted))**2))