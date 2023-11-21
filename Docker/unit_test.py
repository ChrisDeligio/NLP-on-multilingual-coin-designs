import unittest
from cnt.model import DesignEstimator, RelationExtractor, save_pipeline, load_pipeline
from cnt.annotate import (annotate, annotate_single_design, 
                          annotate_designs, 
                          extract_string_from_annotation, labeling_eng)
from cnt.extract_relation import (path, NERTransformer, FeatureExtractor)
from cnt.evaluate import Metrics
from cnt.vectorize import (Doc2Str, Path2Str, Verbs2Str, AveragedPath2Vec, 
                           AveragedRest2Vec)
import pandas as pd


def make_frame(my_list):
    return pd.DataFrame(my_list, columns=["y"])


# The test based on unittest module
class TestMetrics(unittest.TestCase):
    def runTest(self):
        metrics = Metrics()
        self.assertEqual(metrics.score_precision_recall(make_frame(["1","2"]), make_frame(["1","2"])), (1.0,1.0), "Correct")
 
# run the test
print(unittest.main())