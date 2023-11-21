import pandas as pd
import random
import os
import numpy as np
from cnt.annotate import (annotate, annotate_single_design, 
                          annotate_designs, 
                          extract_string_from_annotation, split_alternativenames)
from cnt.io import  Database_Connection


with open("config.txt") as file:
        data = file.read()



dc =  Database_Connection("mysql+mysqlconnector://"+data["user"]+":"+data["password"]+data["host"]+data["database"])
designs = dc.load_designs_from_db("nlp_training_designs", [data["id_col"], data["design_col"]])

add_columns = ["name"+data["language"], "alternativenames"+data["language"]]

entities = {
    "PERSON": dc.load_entities_from_db("nlp_list_person", ["name", "alternativenames"], ["alternativenames"], ",", True),
    "OBJECT": dc.load_entities_from_db("nlp_list_obj", add_columns, [add_columns[1]], ",", True),
    "ANIMAL": dc.load_entities_from_db("nlp_list_animal", add_columns, [add_columns[1]], ",", True),
    "PLANT": dc.load_entities_from_db("nlp_list_plant", add_columns, [add_columns[1]], ",", True)}

annotated_designs = annotate_designs(entities, designs, id_col, design_col)
annotated_designs = annotated_designs[
    annotated_designs.annotations.map(len) > 0]

annotated_designs.to_csv("design_dump.csv", index=False)
