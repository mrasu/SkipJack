#!/usr/bin/env python3

import numpy as np
import matplotlib.pyplot as plt
from sklearn import svm
from sklearn import cross_validation
from sklearn.grid_search import GridSearchCV


# sample from http://d.hatena.ne.jp/teramonagi/20130825/1377434479

def create_data():
    # generate data (y = sin(x) + noise)
    np.random.seed(1)
    x = np.sort(np.random.uniform(-np.pi, np.pi, 100))
    y = np.sin(x) + 0.1*np.random.normal(size=len(x))
    x = x.reshape((len(x), 1))
    
    # split to train/test data
    x_train, x_test, y_train, y_test = cross_validation.train_test_split(x, y, test_size=0.4)
    
    index = x_test.argsort(0).reshape(len(x_test))
    x_test = x_test[index]
    y_test = y_test[index]
    return x_train, x_test, y_train, y_test


def search_optimum_parameter(x_train, x_test, y_train, y_test):
    min_gamma = 0.001
    max_gamma = 1
    min_c = 200
    max_c = 1000
    
    gamma_list = [min_gamma + ((max_gamma - min_gamma) / 5 * a) for a in range(0, 5)]
    c_list = [(max_c - min_c) / 5 * a for a in range(1, 5)]
    
    search_result = do_grid_search(gamma_list, c_list, x_train, y_train)
    # I don't know better mean ignoring std means better score.
    # but this doesn't matter because this aims only provide code example.
    tops = sorted(search_result, key=lambda x: x.mean_validation_score, reverse=True)[:4]

    diff_gamma = max_gamma - min_gamma
    diff_c = max_c - min_c

    # too redundant!
    for _ in range(4):
        diff_gamma /= 5
        diff_c /= 4
        search_result = []
        for better in tops:
            min_gamma = better[0]["gamma"] - (diff_gamma / 2)
            max_gamma = better[0]["gamma"] + (diff_gamma / 2)
            min_c = better[0]["C"] - (diff_c / 2)
            max_c = better[0]["C"] + (diff_c / 2)
            gamma_list = [min_gamma + ((max_gamma - min_gamma) / 5 * a) for a in range(0, 5)]
            c_list = [(max_c - min_c) / 5 * a for a in range(1, 5)]
            search_result += do_grid_search(gamma_list, c_list, x_train, y_train)
        tops = sorted(search_result, key=lambda x: x.mean_validation_score, reverse=True)[:4]
        print(tops)
    best = tops[0]

    clf = svm.SVR(gamma=best[0]["gamma"], C=best[0]["C"])
    clf.fit(x_train, y_train)
    best_result = clf.predict(x_test)
    try:
        pass
        plt.plot(x_test, y_test, 'bo-', x_test, best_result, 'ro-')
        plt.show()
    except Exception:
        pass
    
    return best


def do_grid_search(gamma_list, c_list, x_train, y_train):
    tuned_parameters = [{'gamma': gamma_list, 'C': c_list}]
    
    gscv = GridSearchCV(svm.SVR(), tuned_parameters, scoring="mean_squared_error")
    gscv.fit(x_train, y_train)

    return gscv.grid_scores_

if __name__ == "__main__":
    x_train, x_test, y_train, y_test = create_data()
    print(search_optimum_parameter(x_train, x_test, y_train, y_test))