import json
import numpy as np
from sklearn import cross_validation
from example.GridSearch.gridsearch import GridSearch


def create_input_data(self):
    np.random.seed(1)
    x = np.sort(np.random.uniform(-np.pi, np.pi, 100))
    y = np.sin(x) + 0.1*np.random.normal(size=len(x))
    x = x.reshape((len(x), 1))

    x_train, x_test, y_train, y_test = cross_validation.train_test_split(x, y, test_size=0.4)

    index = x_test.argsort(0).reshape(len(x_test))
    x_test = x_test[index]
    y_test = y_test[index]
    self.__save_input_data(x_train, x_test, y_train, y_test)


def __save_input_data(x_train, x_test, y_train, y_test):
    data_dictionary = {
        "x_train": x_train.tolist(),
        "y_train": y_train.tolist(),
        "x_test": x_test.tolist(),
        "y_test": y_test.tolist()
    }

    f = open("data.txt", "w")
    f.write(json.dumps(data_dictionary))
    return "data.txt"