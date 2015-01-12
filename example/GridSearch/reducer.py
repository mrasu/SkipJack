#!/usr/bin/env python3
import json
from sklearn import svm
from sklearn.metrics import mean_squared_error
from util.stream_input import input_tab_key_value, input_tab_key_json
from util.stream_output import output_raw_json


f = open("cache/condition.txt")
conditions = json.loads((list(f))[0])


def estimate_value(condition, x_train, y_train, x_test, y_test):
    gamma = condition["gamma"]
    c = condition["C"]
    
    clf = svm.SVR(gamma=gamma, C=c)
    clf.fit(x_train, y_train)
    # no implementation of cross validation
    y_predict = clf.predict(x_test)
    return {"mean": mean_squared_error(y_test, y_predict), "gamma": gamma, "C": c}

previous_key = None

for key, train_test_data_dict in input_tab_key_json():
    x_train = train_test_data_dict["x_train"]
    y_train = train_test_data_dict["y_train"]
    x_test = train_test_data_dict["x_test"]
    y_test = train_test_data_dict["y_test"]
    result = estimate_value(conditions[int(key)], x_train, y_train, x_test, y_test)
    output_raw_json(result)
