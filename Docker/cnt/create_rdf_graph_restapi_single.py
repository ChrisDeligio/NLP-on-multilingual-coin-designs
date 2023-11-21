"""
This code contains the implementation discussed in the bachelor thesis 
"Optimizing a Natural Language Processing Pipeline for the automatic creation of RDF data".
by Nils Dambowy and modified by Sebastian Gampe.
Further modified by Chris Deligio
"""

## TODO ## 
# This script needs to be cleaned and structured further.

from rdflib import Graph, URIRef, Literal, BNode
from rdflib.namespace import RDF
from rdflib.namespace._XSD import XSD
from cnt.model import DesignEstimator
import pandas as pd
import os


#rdf prefixes
prefix_dict = { 
                "meta"    : "http://www4.wiwiss.fu-berlin.de/bizer/d2r-server/metadata#", 
                "map"     : "#",
                "db"      : "<>",
                "rdf"     : "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
                "rdfs"    : "http://www.w3.org/2000/01/rdf-schema#",
                "xsd"     : "http://www.w3.org/2001/XMLSchema#",
                "d2rq"    : "http://www.wiwiss.fu-berlin.de/suhl/bizer/D2RQ/0.1#",
                "d2r"     : "http://sites.wiwiss.fu-berlin.de/suhl/bizer/d2r-server/config.rdf#",
                "jdbc"    : "http://d2rq.org/terms/jdbc/",
                "skos"    : "http://www.w3.org/2004/02/skos/core#",
                "owl"     : "http://www.w3.org/2002/07/owl#",
                "foaf"    : "http://xmlns.com/foaf/0.1/",
                "un"      : "http://www.w3.org/2005/Incubator/urw3/XGR-urw3-20080331/Uncertainty.owl",
                "dcterms" : "http://purl.org/dc/terms/",
                "void"    : "http://rdfs.org/ns/void#/",
                "nm"      : "http://nomisma.org/id/",
                "nmo"     : "http://nomisma.org/ontology#",
                "cnt"     : "http://www.dbis.cs.uni-frankfurt.de/cnt/id/",
                "cn"      : "https://www.corpus-nummorum.eu/"
            }



# database methods

def get_db_data(mysql_connection):
    # Persons
    persons = pd.read_sql("select * from nlp_list_person",mysql_connection)
    persons_view = pd.read_sql("select * from nlp_list_person_view",mysql_connection)
    # Objects
    objects = pd.read_sql("select * from nlp_list_obj",mysql_connection)
    objects_view = pd.read_sql("select * from nlp_list_obj_view",mysql_connection)
    # Animals
    animals = pd.read_sql("select * from nlp_list_animal",mysql_connection)
    animals_view = pd.read_sql("select * from nlp_list_animal_view",mysql_connection)
    # Plants
    plants = pd.read_sql("select * from nlp_list_plant",mysql_connection)
    plants_view = pd.read_sql("select * from nlp_list_plant_view",mysql_connection)
    # Verbs
    verbs = pd.read_sql("select * from nlp_list_verb",mysql_connection)
    # Hierarchy
    hierarchy = pd.read_sql("select * from nlp_hierarchy",mysql_connection)
    return persons, persons_view, objects, objects_view, animals, animals_view, plants, plants_view, verbs, hierarchy



def add_categories(g, categories, res, cursor):
    """
    This functions add the categories of X to the graph.

    Args:
        g      : the rdf graph
        categories : categories
        res    : named entity
        cursor : mysql cursor
    """
    if len(categories) != 0:
                    for cat in categories[0]:
                        if cat is None:
                            # if category is empty
                            pass
                        else:
                            # search for class uri
                            try:                                
                                # search for category in nlp_hierarchy
                                # in order to retrieve the class uri
                                cursor.execute("select class_uri from nlp_hierarchy where Class like '{}';".format(str(cat)))
                                query_result = cursor.fetchall()
                                c_uri = check_for_none(query_result, "select class_uri from nlp_hierarchy where Class like '{}';.format(str(cat))")
                                g.add((URIRef(res[0]), URIRef(prefix_dict["rdf"]+"type"), URIRef(c_uri[0][0])))
                        
                            # search for superclass uri
                            except IndexError:
                                # search for category in nlp_hierarchy
                                # in order to retrieve the superclass uri
                                cursor.execute("select superclass_uri from nlp_hierarchy where superclass like '{}';".format(str(cat)))
                                query_result = cursor.fetchall()
                                c_uri = check_for_none(query_result, "select superclass_uri from nlp_hierarchy where superclass like '{}';.format(str(cat))")
                                g.add((URIRef(res[0]), URIRef(prefix_dict["rdf"]+"type"), URIRef(c_uri[0][0])))


def check_for_none(output, query):
    """
    Checks if the output for the used query 
    is None and if it is: it prints
    the usdq query for debugging.
    """
    if output is None:
        return "error with: {}".format(query)
    else:
        return output

# csv methods
def load_csv(csv_path, none_value, prefix, entity, postfix="", columns=[], datatype=".csv"):
    filepath = csv_path + (prefix+entity+datatype).lower()
    if os.path.isfile(filepath):
        
        return pd.read_csv(filepath, header=None, names=columns, \
                          na_values=[none_value], keep_default_na=False)
    

def get_csv_data(csv_path, none_value):
    # persons 
    columns_1 = ['id', 'name', 'name_german', 'description', 'alternativenames', 'typos', 'link', 'related', 'cnt_PersonID', \
          'Cat_I', 'Cat_II', 'Cat_III', 'Cat_IV', 'Cat_V']
    # objects, plants, animals
    columns_2 = ['id', 'name_en', 'name_ger', 'description', 'alternativenames_en', 'alternativenames_ger', 'typos_en', \
        'typos_ger', 'link', 'Cat_I', 'Cat_II', 'Cat_III']
    # verbs
    columns_3 = ['id', 'name_en', 'name_ger', 'alternativenames_en', 'alternativenames_ger', 'link']
    # views
    columns_4 = ['value', 'link']
    # hierarchy
    columns_5 = ['class', 'superclass', 'class_uri', 'superclass_uri']

    columns = {"PERSON": columns_1, "OBJECT": columns_2, "ANIMAL": columns_2, "PLANT": columns_2, "VERB":columns_3, \
                "views":columns_4}
    tables = {}
    views = {}
   
    #for item in items:
    #    tables[item] = load_csv(csv_path, none_value, prefix, item, "", columns[item])
    #    views[item] = load_csv(csv_path, none_value, prefix, item, postfix, columns["views"])
    #    hierarchy = pd.read_csv(csv_path + "nlp_hierarchy.csv", header=None, names=columns_5, \
    #                  na_values=[none_value], keep_default_na=False)
    #return tables, views, hierarchy
    # Load the csvs into dataframes
    # Persons
    persons = pd.read_csv(csv_path + "nlp_list_person.csv", header=None, names=columns_1, \
                      na_values=[none_value], keep_default_na=False)
    persons.fillna('None', inplace=True)
    persons_view = pd.read_csv(csv_path + "nlp_list_person_view.csv", header=None, names=columns_4, \
                      na_values=[none_value], keep_default_na=False)
    # Objects
    objects = pd.read_csv(csv_path + "nlp_list_object.csv", header=None, names=columns_2, \
                      na_values=[none_value], keep_default_na=False)
    objects.fillna('None', inplace=True)
    objects_view = pd.read_csv(csv_path + "nlp_list_object_view.csv", header=None, names=columns_4, \
                      na_values=[none_value], keep_default_na=False)
    # Animals
    animals = pd.read_csv(csv_path + "nlp_list_animal.csv", header=None, names=columns_2, \
                      na_values=[none_value], keep_default_na=False)
    animals.fillna('None', inplace=True)
    animals_view = pd.read_csv(csv_path + "nlp_list_animal_view.csv", header=None, names=columns_4, \
                      na_values=[none_value], keep_default_na=False)
    # Plants
    plants = pd.read_csv(csv_path + "nlp_list_plant.csv", header=None, names=columns_2, \
                      na_values=[none_value], keep_default_na=False)
    plants.fillna('None', inplace=True)
    plants_view = pd.read_csv(csv_path + "nlp_list_plant_view.csv", header=None, names=columns_4, \
                      na_values=[none_value], keep_default_na=False)
    # Verbs
    verbs = pd.read_csv(csv_path + "nlp_list_verb.csv", header=None, names=columns_3, \
                      na_values=[none_value], keep_default_na=False)
    verbs.fillna('None', inplace=True)
    #hierarchy
    hierarchy = pd.read_csv(csv_path + "nlp_hierarchy.csv", header=None, names=columns_5, \
                      na_values=[none_value], keep_default_na=False)
    hierarchy.fillna('None', inplace=True)
    return persons, persons_view, objects, objects_view, animals, animals_view, plants, plants_view, verbs, hierarchy


def create_cat_entity_list(entity_id, entity, df_categories, df_link):
    cat_list = []
    entity_list = []

    if entity[1] == "PERSON":
        cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], \
                            df_categories.iloc[0]["Cat_III"], df_categories.iloc[0]["Cat_IV"], df_categories.iloc[0]["Cat_V"]]
        entity_list = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1], cat_list[2], \
                          cat_list[3], cat_list[4],)
    elif entity[1] == "OBJECT":
        cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], df_categories.iloc[0]["Cat_III"]]
        entity_list = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1], cat_list[2])

    elif entity[1] in ["ANIMAL", "PLANT"]:
        cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"]]
        entity_list = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1])

    else:
        return cat_list, entity_list

    return cat_list, entity_list

def ner_uri(df, tables, views, column):
# match the found ner entities with the matching uris from the lists 
    entity_not_in_list = ()
    
    entity_list = []
    for entity in df.at[0, column]:
        cat_list = []
        
     
        entity_id = 0
        
        table = tables[entity[1]]
        view = views[entity[1]]

        #df_link = pd.DataFrame(view.loc[view['value'] == entity[0]]['link'])
        # case senistivity check muss drin sein, sonst funktioniert das nicht richtig
        df_link = pd.DataFrame(view.loc[view['value'].str.lower() == entity[0].lower()]['link'])
        if  df_link.empty == True:
            entity_not_in_list = (df.at[0, 'design_id'], entity[0], entity[1])
            continue

        df_categories = pd.DataFrame(table[table['link'] == df_link.iloc[0]["link"]])
        entity_id = df_categories.iloc[0]["id"]
        
        
        cat, ent = create_cat_entity_list(entity_id, entity, df_categories, df_link)
        entity_list.append(ent)
        
    return entity_list, entity_not_in_list

    

def re_uri(df, persons, persons_view, objects, objects_view, animals, animals_view, plants, plants_view, verbs):
# match the found re entities and verbs with the matching uris from the lists 
    for side in ['RE_Objects']:
        entity_list = []
        for entity in df.at[0, side]:
            objects_list = []
            entity_2 = entity
            entity_3 = entity
            cat_list = [] 
            df_categories = pd.DataFrame()
            entity_id = 0

            if len(entity) == 5:
                # Subject
                if entity[1] == 'PERSON':
                    df_link = pd.DataFrame(persons_view.loc[persons_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
    				# check if there is a problem with case sensitivity
                    # case senistivity check muss drin sein, sonst funktioniert das nicht richtig
                        df_link = pd.DataFrame(persons_view.loc[persons_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                        # create fake uri for entities which are not in the lists
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None', 'None', \
                                'None', 'None',)
                            objects_list.append(entity)
                            continue							
                    df_categories = pd.DataFrame(persons[persons['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], \
                            df_categories.iloc[0]["Cat_III"], df_categories.iloc[0]["Cat_IV"], df_categories.iloc[0]["Cat_V"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1], cat_list[2], \
                          cat_list[3], cat_list[4],)
                    objects_list.append(entity)

                if entity[1] == 'OBJECT':
                    df_link = pd.DataFrame(objects_view.loc[objects_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(objects_view.loc[objects_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None', 'None')
                            objects_list.append(entity)
                            continue
                    df_categories = pd.DataFrame(objects[objects['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], df_categories.iloc[0]["Cat_III"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1], cat_list[2])
                    objects_list.append(entity)

                if entity[1] == 'PLANT':
                    df_link = pd.DataFrame(plants_view.loc[plants_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(plants_view.loc[plants_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None')
                            objects_list.append(entity)
                            continue							
                    df_categories = pd.DataFrame(plants[plants['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1])
                    objects_list.append(entity)

                if entity[1] == 'ANIMAL':
                    df_link = pd.DataFrame(animals_view.loc[animals_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(animals_view.loc[animals_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None')
                            objects_list.append(entity)
                            continue							
                    df_categories = pd.DataFrame(animals[animals['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1])
                    objects_list.append(entity)    
            
                # Object 
                if entity_2[4] == 'PERSON':
                    df_link = pd.DataFrame(persons_view.loc[persons_view['value'] == entity_2[3]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(persons_view.loc[persons_view['value'].str.lower() == entity_2[3].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity_2[3]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity_2[3].replace(" ", "_") + '_' + entity_2[4].lower()	
                            entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], '0', 'None', 'None', 'None', \
                                'None', 'None',)
                            objects_list.append(entity_2)
                            continue			
                    df_categories = pd.DataFrame(persons[persons['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], \
                            df_categories.iloc[0]["Cat_III"], df_categories.iloc[0]["Cat_IV"], df_categories.iloc[0]["Cat_V"]]
                    entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], entity_id, cat_list[0], cat_list[1], \
                            cat_list[2], cat_list[3], cat_list[4],)
                    objects_list.append(entity_2)

                if entity_2[4] == 'OBJECT':
                    df_link = pd.DataFrame(objects_view.loc[objects_view['value'] == entity_2[3]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(objects_view.loc[objects_view['value'].str.lower() == entity_2[3].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity_2[3]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity_2[3].replace(" ", "_") + '_' + entity_2[4].lower()	
                            entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], '0', 'None', 'None', 'None')
                            objects_list.append(entity_2)
                            continue
                    df_categories = pd.DataFrame(objects[objects['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], df_categories.iloc[0]["Cat_III"]]
                    entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], entity_id, cat_list[0], cat_list[1], \
                            cat_list[2])
                    objects_list.append(entity_2)

                if entity_2[4] == 'PLANT':
                    df_link = pd.DataFrame(plants_view.loc[plants_view['value'] == entity_2[3]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(plants_view.loc[plants_view['value'].str.lower() == entity_2[3].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity_2[3]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity_2[3].replace(" ", "_") + '_' + entity_2[4].lower()	
                            entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], '0', 'None', 'None')
                            objects_list.append(entity_2)
                            continue		
                    df_categories = pd.DataFrame(plants[plants['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"]]
                    entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], entity_id, cat_list[0], cat_list[1])
                    objects_list.append(entity_2)

                if entity_2[4] == 'ANIMAL':
                    df_link = pd.DataFrame(animals_view.loc[animals_view['value'] == entity_2[3]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(animals_view.loc[animals_view['value'].str.lower() == entity_2[3].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity_2[3]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity_2[3].replace(" ", "_") + '_' + entity_2[4].lower()	
                            entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], '0', 'None', 'None')
                            objects_list.append(entity_2)
                            continue		
                    df_categories = pd.DataFrame(animals[animals['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"]]
                    entity_2 = (df_link.iloc[0]["link"], entity_2[3], entity_2[4], entity_id, cat_list[0], cat_list[1])
                    objects_list.append(entity_2)  

                # Relation 
                if entity_3[2] != 'None':
                    df_link = pd.DataFrame(verbs.loc[verbs['name_en'].str.lower() == entity_3[2].lower()]['link'])
                    if  df_link.empty == True:
                         df_link.at[0, 'value'] = entity_3[2]
                         df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity_3[2].replace(" ", "_")
                         entity_3 = (df_link.iloc[0]["link"], entity_3[2], '0')
                         objects_list.append(entity_3)
                         continue
                    df_categories = pd.DataFrame(verbs[verbs['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    entity_3  = (df_link.iloc[0]["link"], entity_3[2], entity_id)
                    objects_list.append(entity_3)
            
                else:
                    continue
                entity_list.append(objects_list)




            else:

                
                if entity[1] == 'PERSON':
                    df_link = pd.DataFrame(persons_view.loc[persons_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
                    # check if there is a problem with case sensitivity
                    # case senistivity check muss drin sein, sonst funktioniert das nicht richtig
                        df_link = pd.DataFrame(persons_view.loc[persons_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                        # create fake uri for entities which are not in the lists
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None', 'None', \
                                'None', 'None',)
                            objects_list.append(entity)
                            continue                            
                    df_categories = pd.DataFrame(persons[persons['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], \
                            df_categories.iloc[0]["Cat_III"], df_categories.iloc[0]["Cat_IV"], df_categories.iloc[0]["Cat_V"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1], cat_list[2], \
                          cat_list[3], cat_list[4],)
                    objects_list.append(entity)

                if entity[1] == 'OBJECT':
                    df_link = pd.DataFrame(objects_view.loc[objects_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(objects_view.loc[objects_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None', 'None')
                            objects_list.append(entity)
                            continue
                    df_categories = pd.DataFrame(objects[objects['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"], df_categories.iloc[0]["Cat_III"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1], cat_list[2])
                    objects_list.append(entity)

                if entity[1] == 'PLANT':
                    df_link = pd.DataFrame(plants_view.loc[plants_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(plants_view.loc[plants_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None')
                            objects_list.append(entity)
                            continue                            
                    df_categories = pd.DataFrame(plants[plants['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1])
                    objects_list.append(entity)

                if entity[1] == 'ANIMAL':
                    df_link = pd.DataFrame(animals_view.loc[animals_view['value'] == entity[0]]['link'])
                    if  df_link.empty == True:
                        df_link = pd.DataFrame(animals_view.loc[animals_view['value'].str.lower() == entity[0].lower()]['link'])
                        if  df_link.empty == True:
                            df_link.at[0, 'value'] = entity[0]
                            df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity[0].replace(" ", "_") + '_' + entity[1].lower()
                            entity = (df_link.iloc[0]["link"], entity[0], entity[1], '0', 'None', 'None')
                            objects_list.append(entity)
                            continue                            
                    df_categories = pd.DataFrame(animals[animals['link'] == df_link.iloc[0]["link"]])
                    entity_id = df_categories.iloc[0]["id"]
                    cat_list = [df_categories.iloc[0]["Cat_I"], df_categories.iloc[0]["Cat_II"]]
                    entity = (df_link.iloc[0]["link"], entity[0], entity[1], entity_id, cat_list[0], cat_list[1])
                    objects_list.append(entity)

                entity_2 = "no-value"
                objects_list.append(entity_2)
                
                if entity_3[2] != 'None':
                        df_link = pd.DataFrame(verbs.loc[verbs['name_en'].str.lower() == entity_3[2].lower()]['link'])
                        if  df_link.empty == True:
                             df_link.at[0, 'value'] = entity_3[2]
                             df_link.at[0, 'link'] = 'http://www.dbis.cs.uni-frankfurt.de/cnt/id/' + entity_3[2].replace(" ", "_")
                             entity_3 = (df_link.iloc[0]["link"], entity_3[2], '0')
                             objects_list.append(entity_3)
                             continue
                        df_categories = pd.DataFrame(verbs[verbs['link'] == df_link.iloc[0]["link"]])
                        entity_id = df_categories.iloc[0]["id"]
                        entity_3  = (df_link.iloc[0]["link"], entity_3[2], entity_id)
                        objects_list.append(entity_3)



                entity_list.append(objects_list)
               
        df.at[0, side] = entity_list
    return df	



            
def create_hierarchy(g, cursor, property_set, class_set):
    """
    Maps the content of nlp_hierarchy table
    Args:
        g            : the rdf graph
        cursor       : mysql cursor
        property_set : holds the different properties
        class_set    : holds the different classes
    """
    print("Using NLP tables from database")
    cursor.execute("Select class, superclass, class_uri, superclass_uri from nlp_hierarchy;") 
    for (c,sc,cu,scu) in cursor:
        g.add((URIRef(cu), URIRef(prefix_dict["skos"]+"prefLabel"), Literal(c, datatype=XSD.string)))
        g.add((URIRef(cu), URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdfs"]+"Class")))
        g.add((URIRef(cu), URIRef(prefix_dict["rdfs"]+"subClassOf"), URIRef(scu)))

    # add property and classes
    property_set.add(prefix_dict["rdfs"]+"subClassOf")
    class_set.add(prefix_dict["rdfs"]+"Class")

def create_hierarchy_csv(g, property_set, class_set, hierarchy):
    """
    Maps the content of nlp_hierarchy table
    Args:
        g            : the rdf graph
        cursor       : mysql cursor
        property_set : holds the different properties
        class_set    : holds the different classes
    """
    
    #print("Using csv files")
    for index, row in hierarchy.iterrows():
        row["class"], row["superclass"], row["class_uri"], row["superclass_uri"]
        g.add((URIRef(row["class_uri"]), URIRef(prefix_dict["skos"]+"prefLabel"), Literal(row["class"], datatype=XSD.string)))
        g.add((URIRef(row["class_uri"]), URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdfs"]+"Class")))
        g.add((URIRef(row["class_uri"]), URIRef(prefix_dict["rdfs"]+"subClassOf"), URIRef(row["superclass_uri"])))

    # add property and classes
    property_set.add(prefix_dict["rdfs"]+"subClassOf")
    class_set.add(prefix_dict["rdfs"]+"Class")

def create_prop_class(g, property_set, class_set):
    """
    Maps the content of the property and class set.

    Args:
        g            : the rdf graph
        property_set : holds the different properties
        class_set    : holds the different classes
    """
    for prop in property_set:
        g.add((URIRef(prop), URIRef( prefix_dict["rdf"]+"type" ), URIRef(prefix_dict["rdf"]+"Property")))

    for c in class_set:
        g.add((URIRef(c), URIRef( prefix_dict["rdf"]+"type" ), URIRef(prefix_dict["rdfs"]+"Class")))

def serialize_graph(g, save_path):
    """
    Creates the output file by serializing the g.

    Args:
        g : the rdf graph
    """
    print("Started serializing!")
    g.serialize(destination= save_path + "output_" + ".ttl", format="nt", encoding="utf-8")


def create_graph_rest_api(df,  hierarchy, use_csv, save_path, params): #save_path
    """
    This function executes the different mapping functions and
    establishes a connection to the database.
    
    Args:
        ids : array of coin ids
    """

    g = Graph()

    # fill out with your own data
    mysql_param = params
    mydb = mysql.connector.connect(**mysql_param)
    cursor = mydb.cursor(buffered=True)

    # used for executing sql statements when iterating over the first cursor
    cursor2 = mydb.cursor(buffered=True) 

    # holds the different properties
    property_set = set()

    # holds the different classes
    class_set = set()
    
    map_designs(g, df, property_set)
    map_nlp(g, df, property_set, class_set, cursor, hierarchy, use_csv)
    if use_csv == True: 
        create_hierarchy_csv(g, cursor, property_set, class_set, hierarchy)
    else:
        create_hierarchy(g, cursor, property_set, class_set)
        
    create_prop_class(g, property_set, class_set)
    serialize_graph(g, save_path)



def transform(item):
    return item.replace(" ", "")



def create_single_rdf(ner_rdf, re_rdf, graph_format, hierarchy, designid, design):

    ner_links = {"PERSON": "https://www.wikidata.org/wiki/Q215627",
                 "OBJECT": "https://www.wikidata.org/wiki/Q488383",
                 "ANIMAL": "https://www.wikidata.org/wiki/Q729",
                 "PLANT": "https://www.wikidata.org/wiki/Q756"}
    
    none_value = "NULL"
    #hierarchy = pd.read_csv(csv_path + "nlp_hierarchy.csv")
    add_data_ner = []
    rdf_type = RDF.type
    g = Graph()
    g = add_props(g)
    design_bnode_bag_r_appr = BNode()
    design_bnode_bag_r_icon = BNode()
    # zuvor wurde hier design übergeben - müsste ein Fehler sein.
    g.add((URIRef("https://data.corpus-nummorum.eu/api/designs/"+str(designid)), URIRef(prefix_dict["nmo"]+"hasAppearance"), design_bnode_bag_r_appr))

    # Designs --> nlp_bag (hasIconography)
    g.add((URIRef("https://data.corpus-nummorum.eu/api/designs/"+str(designid)), URIRef(prefix_dict["nmo"]+"hasIconography"), design_bnode_bag_r_icon))

    g.add((design_bnode_bag_r_icon, URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdf"]+"Bag")))
    g.add((design_bnode_bag_r_appr, URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdf"]+"Bag")))
    g.add((design_bnode_bag_r_icon, URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdf"]+"Bag")))
    g.add((design_bnode_bag_r_appr, URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdf"]+"Bag")))   

    #NER
    if ner_rdf != []:
        for item in ner_rdf: #item=entity
        
            s = item[0]
            o = item[2]

            g.add((design_bnode_bag_r_appr, URIRef(prefix_dict["rdf"]+"li"), URIRef(item[0])))
            g.add((
                URIRef(transform(s)),
                rdf_type,
                URIRef(transform(ner_links[o]))
                ))
            g.add((
                URIRef(transform(s)),
                rdf_type,
                URIRef(transform(ner_links[o]))
                ))

            prefix = str(item[2]).lower()
            
            if item[2] == "PERSON":
                prefix = "subject"
           
            g.add((
                    URIRef(transform(s)),
                    URIRef(prefix_dict["dcterms"]+"identifier"),
                    Literal(prefix+"_id="+str(item[3]))
                    ))
            g.add((
                URIRef(transform(s)), 
                URIRef(prefix_dict["skos"]+"prefLabel"), 
                Literal(item[1], datatype=XSD.string)
            ))

            
            for i in range(4, len(item)):
                    if item[i] != "None":
                        add_data_ner.append(item[i])
        add_data_ner = set(add_data_ner)
        for index, row in hierarchy.iterrows():
            
            if row["class"] in add_data_ner:
                g.add((URIRef(row["class_uri"]), URIRef(prefix_dict["skos"]+"prefLabel"), Literal(row["class"], datatype=XSD.string)))
                g.add((URIRef(row["class_uri"]), URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdfs"]+"Class")))
                g.add((URIRef(row["class_uri"]), URIRef(prefix_dict["rdfs"]+"subClassOf"), URIRef(row["superclass_uri"])))


        #RE
        if isinstance(re_rdf, list):
            predicates = []
            for item in re_rdf:
                
                s = item[0][0]
                p = item[2][0]
             
                
                curr_b_node = BNode()

                g.add((curr_b_node, URIRef(prefix_dict["rdf"]+"type"), URIRef(prefix_dict["rdf"]+"Statement")))
                #  nlp_bag --> entry
                g.add((design_bnode_bag_r_icon, URIRef(prefix_dict["rdf"]+"li"), curr_b_node))

                g.add((curr_b_node, URIRef(prefix_dict["rdf"]+"subject"), URIRef(transform(s))))
                if item[1] == "no-value":
                    g.add((curr_b_node, URIRef(prefix_dict["rdf"]+"object"), Literal(item[1], datatype=XSD.string)))
                else:
                    o = item[1][0]
                    g.add((curr_b_node, URIRef(prefix_dict["rdf"]+"object"), URIRef(transform(o))))
                g.add((curr_b_node, URIRef(prefix_dict["rdf"]+"predicate"), URIRef(transform(p))))
                predicates.append((item[2][0], item[2][1], item[2][2]))

            predicates = set(predicates)
            for predicate in predicates:
                g.add((URIRef(transform(predicate[0])), URIRef(prefix_dict["skos"]+"prefLabel"), Literal(predicate[1], datatype=XSD.string)))
                g.add((URIRef(transform(predicate[0])), URIRef(prefix_dict["skos"]+"prefLabel"), Literal("predicate_id="+str(predicate[2]))))

            
            
            g = add_design_information(g, design, designid)
        return g.serialize(format=graph_format)


def add_props(g):
    # add properties
        # holds the different properties
    property_set = set()

    # holds the different classes
    class_set = set()

    property_set.add(prefix_dict["dcterms"]+"identifier")
    property_set.add(prefix_dict["dcterms"]+"title")
    property_set.add(prefix_dict["dcterms"]+"publisher")
    property_set.add(prefix_dict["nmo"]+"hasIconography")
    property_set.add(prefix_dict["nmo"]+"hasAppearance")
    property_set.add(prefix_dict["rdf"]+"type")
    property_set.add(prefix_dict["skos"]+"prefLabel")
    property_set.add(prefix_dict["rdf"]+"type")
    property_set.add(prefix_dict["rdf"]+"li")
    property_set.add(prefix_dict["rdf"]+"subject")
    property_set.add(prefix_dict["rdf"]+"object")
    property_set.add(prefix_dict["rdf"]+"predicate")
    property_set.add(prefix_dict["rdfs"]+"subClassOf")

    class_set.add(prefix_dict["rdf"]+"Statement")
    class_set.add(prefix_dict["rdf"]+"Bag")
    class_set.add(prefix_dict["rdfs"]+"Class")

    for prop in property_set:
        
        g.add((URIRef(prop), URIRef( prefix_dict["rdf"]+"type" ), URIRef(prefix_dict["rdf"]+"Property")))

    for c in class_set:
        g.add((URIRef(c), URIRef( prefix_dict["rdf"]+"type" ), URIRef(prefix_dict["rdfs"]+"Class")))

    return g


def add_design_information(g, design, designid):

    g.add((URIRef("https://data.corpus-nummorum.eu/api/designs/"+str(designid)), URIRef(prefix_dict["dcterms"]+"identifier"), Literal("design_id="+str(designid))))
    #title
    g.add((URIRef("https://data.corpus-nummorum.eu/api/designs/"+str(designid)), URIRef(prefix_dict["dcterms"]+"title"), Literal("CNT Design"+str(designid))))
    #publisher
    g.add((URIRef("https://data.corpus-nummorum.eu/api/designs/"+str(designid)), URIRef(prefix_dict["dcterms"]+"publisher"), Literal("Corpus Nummorum Thracorum")))
    
    #Original: Z. 456-459 gleiches design bei beiden Sprachen?
    # Design -> dcterms:descriptions (de)
    #g.add((URIRef("https://data.corpus-nummorum.eu/api/designs/"+str(designid)), URIRef(prefix_dict["dcterms"]+"description"), Literal(str(design), lang="de"))) 
    # Design -> dcterms:descriptions (en)
    g.add((URIRef("https://data.corpus-nummorum.eu/api/designs/"+str(designid)), URIRef(prefix_dict["dcterms"]+"description"), Literal(str(design), lang="en")))

    return g