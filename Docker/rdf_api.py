### Import
import json
import os
import sys
import pathlib 
import base64
from fastapi import FastAPI, Response, Request
from fastapi.routing import APIRouter
from pydantic import BaseModel
from typing import Optional
import pandas as pd
from rdflib import Graph

src = "nlp/"
sys.path.append(src)

from cnt.model import load_ner_model_v2
from cnt.model import load_pipeline, predict_re_single_sentence
import cnt.extract_relation    
from cnt.create_rdf_graph import create_graph
from cnt.create_rdf_graph_restapi_single import create_graph_rest_api,  get_csv_data, ner_uri, re_uri, create_single_rdf, get_db_data
import mysql.connector

from load_models import Model_Selector



### Set API
app = FastAPI()
router = APIRouter()


# Set path to csv
design_id = 10 # default id to use, id issue not fixed yet - send static id back
csv_path = "nlp/data/"
none_value = "NULL" ## for '/N' value use "//N" 

# Save path for rdf files
save_path = 'tmp_storage/'

with open("config.txt") as file:
        data = file.read()
config = json.loads(data) # some database and preset parameter

use_csv = config["use_csv"]

data = \
{'design_id': [design_id], \
 'description': [""], \
 'NER_Entities': [""], \
 'RE_Objects' : [""]}

df = pd.DataFrame(data) 

# load models
model_selector = Model_Selector()
model_no_object_selector = Model_Selector(modes=["re"], single=True)

# load data for rdf graph
if use_csv:
    persons, persons_view, objects, objects_view, animals, animals_view, plants, plants_view, verbs, hierarchy = get_csv_data(csv_path, none_value)

else:
    params = config["database"]
    mydb = mysql.connector.connect(**params)
    cursor = mydb.cursor(buffered=True)

    persons, persons_view, objects, objects_view, animals, animals_view, plants, plants_view, verbs, hierarchy = get_db_data(mydb)

# prepare data for rdf information
tables = {"PERSON": persons, "OBJECT": objects, "ANIMAL": animals, "PLANT": plants, "VERB": verbs}
views = {"PERSON": persons_view, "OBJECT": objects_view, "ANIMAL": animals_view, "PLANT": plants_view}

def unify_single_re_and_re_model(prediction, prediction_single):
    """
    Unify the output of both models by eliminating duplicate meanings. 
    --> If Athena, holding, sword exits, eliminate Athena, holding, "no object"
    """
    assert isinstance(prediction, list), "Needs to be a list."
    assert isinstance(prediction_single, list), "Needs to be a list."

    result = []

    for pred_s in prediction_single:
        exists = True
        for pred in prediction:
            if  pred_s[0] == pred[0] and pred_s[2] == pred[2]:
                continue
            else:
                exists = False

        if exists == False:
            result.append(pred_s)

    return result

def apply_nlp(sentence, lang, mode):
    """
    Apply models based on language and mode selected.
    If re is selected unify the output of both models.
    """
    prediction = []
    id_col = "id"
    design_col = "design_en"
    if mode == "ner":
        prediction = model_selector.models[lang, mode].predict_single_sentence_clear(sentence)
        
    elif mode == "re":
        prediction = predict_re_single_sentence(model_selector.models[lang, mode], sentence)
        prediction_single = predict_re_single_sentence(model_no_object_selector.models[lang+"_no_object", mode], sentence)
        prediction += unify_single_re_and_re_model(prediction, prediction_single)
    if prediction == []:
        prediction = "Null"
   

    return prediction


class Design(BaseModel):
    """
    Setting request body
    """
    design: str
    designid: Optional[int] = 1
    lang: Optional[str] = "en"
    result_format: Optional[str] = "n3"
    

## TODO ##
# raising basic except not optimal, if time adapt to individual errors.

@router.get("/")
async def api():
    return "NLP-on-multilingual-coin-datasets-API"


@router.post("/re/")
async def predict_relation_extraction(design: Design):
    try:        
        if design.lang not in ["de", "en"]:
            return "lang not implemented. Type 'de' for German or 'en' for english"

        return json.dumps({"RE" : apply_nlp(design.design.replace("%20", " "), design.lang, "re")})
    except:
        return "Something went wrong..."


@router.post("/ner/")
async def predict_relation_extraction(design: Design):
    try:
        if design.lang not in ["de", "en"]:
            return "lang not implemented. Type 'de' for German or 'en' for english"

        return json.dumps({"NER" : apply_nlp(design.design.replace("%20", " "), design.lang, "ner")})
    except:
        return "Something went wrong..."

@router.post("/rdf/")
def rdf(design: Design):
    """
    Creates rdf structure and returns as xml, turtle or simple list.
    """

    df.at[0, 'design_id'] = design.designid
    df.at[0, 'description'] = design.design
    df.at[0, 'NER_Entities'] = apply_nlp(df.at[0, 'description'], design.lang, "ner")
    df.at[0, 'RE_Objects'] = apply_nlp(df.at[0, 'description'], design.lang, "re")

    
    if df.at[0, 'NER_Entities'] != []:
        try:
            rdf, _ = ner_uri(df, tables, views, "NER_Entities") 
        except:
            rdf = "No entity found."

        df.at[0, 'NER_Entities'] = rdf

    if df.at[0, 'RE_Objects'] != "No relation found.":
        try:
            re_uri(df, persons, persons_view, objects, objects_view, animals, animals_view, plants, plants_view,  verbs)
        except:
            df.at[0, 'RE_Objects'] = "No relation found."


   
    ner_rdf, re_rdf = df.at[0, 'NER_Entities'], df.at[0, 'RE_Objects']
    
  
    response = "Null"
    try:
        if design.result_format == "list":
            body = create_list_output(ner_rdf, re_rdf)
            response = Response(content=json.dumps(body), media_type="application/json")
        else:
            body = create_single_rdf(ner_rdf, re_rdf, design.result_format, hierarchy, design.designid, design.design)
            response = Response(content=body, media_type="application/"+design.result_format)

    except:
            response = "Design could not be processed."
    return response

def create_list_output(ner_rdf, re_rdf):
    body = {}

    ner = []
    re = []

    for item in ner_rdf:
        ner.append(item)
    for item in re_rdf:
        s = item[0][0]
        p = item[2][0]
        o = item[1][0]

        re.append((s,p,o))

    body["NER"] = ner
    body["RE"] = re

    return body



app.include_router(router, prefix="/api/nlp")
