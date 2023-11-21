from sklearn.model_selection import train_test_split
import pandas as pd
import random
import os
import numpy as np
from cnt.model import DesignEstimator, save_ner_model, load_ner_model,save_ner_model_v2, load_ner_model_v2
from cnt.evaluate import Metrics
import spacy
import json
from ast import literal_eval
from datetime import date
import warnings
warnings.filterwarnings("ignore")
import sys


sys.stdin=open("host/log/input.txt","r")

with open("host/Docker/config.txt") as file:
        data = file.read()

data = json.loads(data)
def train_ner():
	"""
	Run the training process of the model

	## TODO ##
	
	Add unit tests
	"""
	metrics = Metrics()

	
	annotated_designs = pd.read_csv("host/Docker/data/train_set.csv")
	annotated_designs["annotations"] = annotated_designs.apply(lambda row: literal_eval(row["annotations"]), axis=1)

	
	X_train, X_test, y_train, y_test = train_test_split(annotated_designs[[data["id_col"], data["design_col"]]],
	                                                    annotated_designs[[data["id_col"], "annotations"]],
	                                                    test_size=0.25, random_state = 12)

	y_test = y_test.rename(columns={"annotations": "y"})
	X_test.index = [i for i in range(X_test.shape[0])]
	y_test.index = [i for i in range(y_test.shape[0])]

	my_estimator = DesignEstimator(4, data["output_dir_ner"], data["model_name_ner"], data["id_col"], data["design_col"])
	my_estimator.set_labels("PERSON", "OBJECT", "ANIMAL", "PLANT")
	my_estimator.fit(X_train, y_train.annotations, "cnt")

	
	x_predict = my_estimator.predict(X_test,as_doc=False)

	precision, recall = metrics.score_precision_recall(y_test, x_predict)
	F1 = (2*precision*recall) / (precision + recall)

	return precision, recall, F1


def display(precision, recall, f1):
	"""
	Save F1 score as txt and complete report as html.
	"""
	f1_file = open("host/log/f1_score.txt","w")
	f1_file.write(str(int(f1*100)))
	f1_file.close()

	display_log = open("host/log/logger.html","r+")
	display_log.read()
	display_log.write("<pre>\n---------------------------------------------\n</pre>")
	display_log.write("<pre>\nTimestamp : " + str(date.today())+"\n</pre>")
	display_log.write("<pre>\nF1 achieved : " + str(round(f1*100,2))+"\n</pre>")
	display_log.write("<pre>\nPrecision achieved : " + str(round(precision*100,2))+"\n</pre>")
	display_log.write("<pre>\nRecall achieved : " + str(round(recall*100,2))+"\n</pre>")
	display_log.close()


if __name__ == "__main__":
	precision, recall, F1 = train_ner()
	display(precision, recall, F1)
	print(int(round(F1*100,0)))