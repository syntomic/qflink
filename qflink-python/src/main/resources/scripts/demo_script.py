import joblib
import numpy as np

def user_open(path: str):
    """execute once"""
    return joblib.load(path)


def user_eval(model, data):
    """execute every time on data arrive"""
    return model.predict(np.array(data).reshape(1, -1)).tolist()[0]