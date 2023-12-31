from sklearn.base import BaseEstimator, ClassifierMixin
import spacy
import random
from tqdm import tqdm
from pathlib import Path
import os
import pickle
from cnt.annotate import map_find_max_entity, find_max_entity, get_max_overlap
import pathlib
import pandas as pd

class DesignEstimator(BaseEstimator):
    def __init__(self, n_rep=3, output_dir="", model_name ="model", id_col = "DesignID", design_col = "Design", spacy_model = 'en_core_web_sm',save_optional=False, load_optional=False, optional_info="", learning_rate=0.001, batch_size=100, train=True):
        """
        modification:
        output_dir: instead of having one fix path, we need the path as a variable cause of different languages
        design_col: having different languages, the data is not stored in the same column
        """
        self.nlp = spacy.load(spacy_model)
        self.n_rep = n_rep
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.train = train
        self.output_dir = output_dir
        self.id_col = id_col
        self.design_col = design_col
        self.labels = []
        self.model_name = model_name
        self.save_optional = save_optional
        self.load_optional = load_optional
        if self.load_optional == True:
            self.optional_info = self.load_optional_info(self.output_dir)
        else:
            self.optional_info = optional_info

        print("Pretrained_model: ", spacy_model)
        print("Learning_rate: ", learning_rate)
        print("Batch_size: ", batch_size)
        print("Model_name: ", model_name)
    def set_labels(self, *args):
        """

        Parameters
        ----------
        args : NER Labels to be created ; example: create_labels("PERSON", "OBJECT") or ("PERSON", "OBJECT", "ANIMAL", "PLANT")

        Returns
        -------
        list containing given labels
        """
        for label in args:
            self.labels.append(label)

    def get_labels(self):
        """
        return labels set by user
        """
        return self.labels



    def create_batches(self, list_, batch_size):
        """
        Divides a list into several packages whose
        size is determined by the batch_size.

        Parameters
        ---------------

        list_ : list
            The input list

        batch_size: int
            The size of the batches
        """
        for i in range(0, len(list_), batch_size):
            yield list_[i:i + batch_size]



    def fit(self, X, y, dataBase):
        """
        Fits the model / the estimator

        Parameters
        ---------------

        X: list
            list of input sentences

        y: list
            the corresponding annotations to the sentences
            e.g. [[(0, 6, 'PERSON')], [(0, 9, 'PERSON')]]
        """
        if self.train == True:
            zipped = zip(X[self.design_col], y)
            train_data = list(zipped)

            output_dir =  self.output_dir

            other_pipes = [pipe for pipe in self.nlp.pipe_names if pipe != 'ner']
            # train an additional entity type
            if 'ner' not in self.nlp.pipe_names:
                ner = self.nlp.create_pipe('ner')
                self.nlp.add_pipe(ner)
            else:
                ner = self.nlp.get_pipe('ner')

            for label in self.labels:
                ner.add_label(label)

            with self.nlp.disable_pipes(*other_pipes):
                optimizer = self.nlp.begin_training()
                optimizer.alpha = self.learning_rate
                for iterations in range(self.n_rep):
                    random.shuffle(train_data)
                    for batch in list(self.create_batches(train_data, self.batch_size)):
                        raw_text, entity_offsets = zip(*batch)
                        entity_offsets =  map_find_max_entity(entity_offsets)
                        self.nlp.update(raw_text,
                                        [{'entities': ent} for ent in entity_offsets],
                                        sgd=optimizer)

            if output_dir is not None:
                output_dir = Path(output_dir)
                
                if not output_dir.exists():
                    output_dir.mkdir()
                
                #save_ner_model(self, str(output_dir)+"/", self.model_name)
                save_ner_model_v2(self, output_dir, self.model_name)
                #print("Saved model to", output_dir)


    def predict_single_sentence(self, string, as_doc=False):
        """
        Predicts the annotation of the estimator
        on a single sentence

        Parameters
        -----------

        string: str
            The sentence that is to be predicted

        as_doc = Boolean
            Indicates if a list is a doc object or not;
            required for rendering in SpaCy

        """
        doc = self.nlp(string)
        if as_doc:
            return doc
        l = []
        for ent in doc.ents:
            l.append((ent.start_char, ent.end_char, ent.label_))
        l = find_max_entity(l)

        return l

    def predict(self, X, as_doc=False):
        """
        Predicts the annotation of the estimator
        on a list of sentences

        Parameters
        -----------

        X: data frame
            data frame with designs and designIDs

        as_doc = Boolean
            Indicates if a list is a doc object or not;
            required for rendering in SpaCy
        """

        predictions = X[self.design_col].map(lambda x: self.predict_single_sentence(x, as_doc=as_doc))
        X_pred = pd.DataFrame({self.id_col: X[self.id_col], "y": predictions})
        return X_pred


    def predict_single_sentence_clear(self, string, as_doc=False):
        """
        Predicts the annotation of the estimator
        on a single sentence

        Parameters
        -----------

        string: str
            The sentence that is to be predicted

        as_doc = Boolean
            Indicates if a list is a doc object or not;
            required for rendering in SpaCy

        """
        doc = self.nlp(string)
        if as_doc:
            return doc
        l = []
        for ent in doc.ents:
            l.append((ent.text, ent.label_))
        return l

    def predict_single_sentence_clear_optional(self, string, as_doc=False):
        """
        Predicts the annotation of the estimator
        on a single sentence

        Parameters
        -----------

        string: str
            The sentence that is to be predicted

        as_doc = Boolean
            Indicates if a list is a doc object or not;
            required for rendering in SpaCy

        """
        doc = self.nlp(string)
        if as_doc:
            return doc
        l = []
        for ent in doc.ents:
            l.append((ent.text, ent.label_, self.get_optional(ent.text)))
        return l

    def get_optional(self, text):
        return self.optional_info.loc[self.optional_info["name"]==text]["link"].item()

    def predict_clear(self, X, as_doc=False):
        """
        Predicts the annotation of the estimator
        on a list of sentences

        Parameters
        -----------

        X: data frame
            data frame with designs and designIDs

        as_doc = Boolean
            Indicates if a list is a doc object or not;
            required for rendering in SpaCy
        """
        predictions = X[self.design_col].map(lambda x: self.predict_single_sentence_clear(x, as_doc=as_doc))
        X_pred = pd.DataFrame({self.id_col: X[self.id_col], "y": predictions})
        return X_pred

    def load_optional_info(self, directory):
        path = pathlib.PurePath(directory, "optional.csv")
        return pd.read_csv(path)

def save_ner_model(model, directory, model_name):
        path = pathlib.PurePath(directory, model_name)
        pickle.dump(model, open(path, "wb"))

def load_ner_model(directory, model_name):
    path = pathlib.PurePath(directory, model_name)
    file = open(path, "rb")
    return pickle.load(file)

def save_ner_model_v2(model, directory, model_name):
        model.nlp.to_disk(directory)
        if model.save_optional ==True:
            path = pathlib.PurePath(directory, "optional.csv")
            model.optional_info.to_csv(path, index=False)

def load_ner_model_v2(directory, model_name,id_col= "DesignID", design_col ="Design", load_optional=False):
    return DesignEstimator(0, directory, model_name, id_col, design_col, spacy_model=directory, load_optional=load_optional, train=False)


class RelationExtractor(BaseEstimator, ClassifierMixin):
    NONEXISTINGRELATION = "nonexisting_relation"
    KEY = "y"

    def __init__(self, pipeline, output_dir, model_name, id_col):
        self.pipeline = pipeline
        self.output_dir = output_dir
        self.model_name = model_name
        self.id_col = id_col

    def fit(self, X, y):
        """
        fits the model

        Parameters
        ----------

        X: list of lists of Feature objects
        y: list of lists of (subj, relation_class_label, obj)
        """
        X_features = []
        y_for_classification = []
        for list_of_features, list_of_annotations in zip(X["y"], y["y"]):
            dict_of_annotations = {(subj, obj): label for subj, _, label, obj, _ in list_of_annotations}
            for feature in list_of_features:
                label = dict_of_annotations.get((feature.subj.text, feature.obj.text), self.NONEXISTINGRELATION)
                y_for_classification.append(label)
                X_features.append(feature)

        self.pipeline.fit(X_features, y_for_classification)
        return self


    def predict(self, X):
        """
        predicts the models' output for a list of sentences

        Parameters
        ----------

        X: list of lists of Feature objects
        """
        trans = X[self.KEY].map(self.predict_single)
        return pd.DataFrame({self.id_col: X[self.id_col], "y": trans})

    def predict_single(self, x):
        """
        predicts the models' output for a single sentence

        Parameters
        ----------

        X: list of Feature objects
        """
        if len(x) == 0:
            return []

        list_of_predicted_relations = self.pipeline.predict(x)
        prediction = []
        for feature, rel in zip(x, list_of_predicted_relations):
            if rel != self.NONEXISTINGRELATION:
                prediction.append((feature.subj.text, feature.subj.label_, rel, feature.obj.text, feature.obj.label_))

        return prediction

def predict_re_single_sentence(model, sentence):
    import pandas as pd
    df = pd.DataFrame(columns=["id","design_en"])
    df = df.append({"id":0,"design_en":sentence}, ignore_index=True)
    y_pred = model.predict(df)
    y_pred = y_pred["y"].item()
    return y_pred
    
def save_pipeline(pipeline, model_dir, model_name):
    pickle.dump(pipeline, open(os.path.join(model_dir,model_name), "wb"), protocol=4)

def load_pipeline(model_dir, model_name):
    file = open(os.path.join(model_dir,model_name), "rb")
    return pickle.load(file)

