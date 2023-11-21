from sklearn.model_selection import train_test_split
import pandas as pd
import random
import os
import numpy as np
from cnt.model import DesignEstimator, RelationExtractor, save_pipeline, load_pipeline
from cnt.annotate import (annotate, annotate_single_design, 
                          annotate_designs, 
                          extract_string_from_annotation, labeling_eng)
from cnt.extract_relation import (path, NERTransformer, FeatureExtractor)
from cnt.evaluate import Metrics
from cnt.vectorize import (Doc2Str, Path2Str, Verbs2Str, AveragedPath2Vec, 
                           AveragedRest2Vec)
import spacy
import json
from ast import literal_eval
from datetime import date
from sklearn.pipeline import Pipeline, make_pipeline, make_union
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import Normalizer
from sklearn.naive_bayes import MultinomialNB
from itertools import product
import warnings
warnings.filterwarnings('ignore')
import sys

sys.stdin=open("host/log/re_input.txt","r")

with open("host/Docker/config.txt") as file:
        data = file.read()

data = json.loads(data)
def train():
	"""
	Train the re model

	## TODO ##

	at the moment the best setting is implement (Logistic Regression + Path2Str + CountVecotrizer)
	--> Implement Gridsearch and/or Cross Validation and report the best result.
	"""
	metrics = Metrics()

	
	annotated_designs = pd.read_csv("host/Docker/data/re_train_set.csv")
	annotated_designs["y"] = annotated_designs.apply(lambda row: literal_eval(row["y"]), axis=1)

	
	X_train, X_test, y_train, y_test = train_test_split(annotated_designs[[data["id_col"], data["design_col"]]],
	                                                    annotated_designs[[data["id_col"], "y"]],
	                                                    test_size=0.25, random_state = 12)
	

	classifier = LogisticRegression(max_iter=1000)
	string_converter = Path2Str(pos=True) 
	vectorizer = CountVectorizer(ngram_range=(1,3))
	feature = make_pipeline(string_converter, vectorizer)


	inner_pipeline = make_pipeline(feature, classifier)
	pipeline = make_pipeline(NERTransformer(data["output_dir_ner"], data["model_name_ner"], data["id_col"], data["design_col"]),
	                         FeatureExtractor(data["output_dir_ner"], data["model_name_ner"], data["id_col"], data["design_col"]),
	                         RelationExtractor(data["output_dir_re"], data["model_name_re"], re_model_name, data["id_col"]))
	pipeline.fit(X_train, y_train)


	save_pipeline(pipeline, re_model_directory, re_model_name)
	model = load_pipeline(re_model_directory, re_model_name)

	y_pred = model.predict(X_test)
	metrics = Metrics()

	precision, recall = metrics.score_precision_recall(y_test, y_pred)
	F1 = (2*precision*recall) / (precision + recall)

	return precision, recall, F1


def display(precision, recall, f1):
	f1_file = open("host/log/re_f1_score.txt","w")
	f1_file.write(str(int(f1*100)))
	f1_file.close()

	display_log = open("host/log/re_logger.html","r+")
	display_log.read()
	display_log.write("<pre>\n---------------------------------------------\n</pre>")
	display_log.write("<pre>\nTimestamp : " + str(date.today())+"\n</pre>")
	display_log.write("<pre>\nF1 achieved : " + str(round(f1*100,2))+"\n</pre>")
	display_log.write("<pre>\nPrecision achieved : " + str(round(precision*100,2))+"\n</pre>")
	display_log.write("<pre>\nRecall achieved : " + str(round(recall*100,2))+"\n</pre>")
	display_log.close()


if __name__ == "__main__":
	precision, recall, F1 = train()
	display(precision, recall, F1)
	print(int(round(F1*100,0)))















