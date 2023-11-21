import os
import sys
import json
from cnt.model import load_ner_model_v2
from cnt.model import load_pipeline, predict_re_single_sentence

supported_languages = ["english", "german"] 

with open("config.txt") as file:
        data = file.read()
data = json.loads(data) # some database and preset parameter

class Model_Selector():
	"""
	This class loads and prepares all selected pretrained models (existing). 
	"""

	def __init__(self, langs=["english"], modes=["ner", "re"], single=False):
		"""
		    Parameter
		    --------
		    langs : list
		    	list of languages to load

		    modes : list
		    	list of modes to load

		    single : bool
		    	Set to load only Subject - Predicate model --> Object will be set to 'no_object'

		    Methods
		    --------
		    load_models(self, langs, mode)
		    	Iterates through the languges to load the specifc model by calling load_lang

		    load_lang(self, lang, method)
		    	Loads the model based on the parameter

		"""

		assert isinstance(langs, list), "langs must be a list like ['english', 'german'']"
		assert isinstance(modes, list), "mode must be a list of string and contain'ner' or 're'"

		for mode in modes:
			if (mode != "ner") and (mode != "re"):
				raise ValueError("Mode must be either 'ner' or 're'")
		for lang in langs:
			if lang not in supported_languages:
				raise ValueError("Language not supported or wrongly spelled.\n Language supported: english and german")
		
		self.langs = langs
		self.modes = modes
		self.models = {}
		self.single = ""
		
		if single == True: # Detecting relations without an object like Athena holding --> (Athena, holding, no_object)
			self.single = "_no_object"

		for mode in self.modes: 
			self.load_models(self.langs, mode)


	def load_models(self, langs, mode):
		for lang in self.langs:
			self.models[lang+self.single, mode] = self.load_lang(lang+self.single, mode)	


	def load_lang(self, lang, method):
		## TODO ##
		# Eliminate setting model_name by if and else -> not optimal
		
		if lang == "english"+self.single:
		  model_name = "english_cno"

		elif lang== "german"+self.single:
		  model_name = "german_cno"

		if method == "ner":
			try:
				model_directory = os.path.join(data["pre_trained_model_path"],"ner/",lang)
				return load_ner_model_v2(model_directory, model_name, id_col=data["id_col"], design_col=data["design_col"])
			except: raise BaseException("Loading the ner model failed. Please verify input.")
		elif method == "re":
			try:
				re_model_directory = os.path.join(data["pre_trained_model_path"],"re/",lang)
				return load_pipeline(re_model_directory, model_name)
			except: raise BaseException("Loading the re model failed. Please verify input.")