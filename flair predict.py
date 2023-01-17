# -*- coding: utf-8 -*-
"""
Created on Fri Jul 17 14:10:00 2020

@author: peter_goodridge
"""

from pymongo import MongoClient
import os
from flair.data import Sentence, build_spacy_tokenizer
from flair.models import SequenceTagger
from flair.embeddings import BertEmbeddings
import spacy
import json
import pandas as pd
from datetime import datetime
import re
from flair.tokenization import SpacyTokenizer

"""
connection = MongoClient('ds159204.mlab.com', 59204, retryWrites = False)
db = connection['oxford']
db.authenticate('oxford', 'datasci3nce')

listings_raw = []
docs = db.indeed_perm.find()
for item in docs:
    listings_raw.append(item)
"""

tagger = SequenceTagger.load(r'/content/drive/My Drive/best-prodigy-model.pt')

listings_raw = pd.read_csv('indeed_listings3.csv')
#from flair.models import SequenceTagger

#test_tagger = SequenceTagger.load('ner')

import copy

sent_nlp = spacy.blank("en") 
sent_nlp.add_pipe(sent_nlp.create_pipe('sentencizer')) # updated
generic_re = r'We are a.+\n'
tokenizer_nlp = spacy.blank("en")
tokenizer = SpacyTokenizer(tokenizer_nlp)
            

rows = []
all_sentences = []
all_meta = []
for index, row in listings_raw.iterrows():
  

  company = row['company']
  job_title = row['job_title']
  location = row['location']
  job_id = row['job_id']
  job_desc = row['job_desc']

  doc = sent_nlp(job_desc)

  
  
  for sent in doc.sents:
      sent_string = sent.string.strip()
      all_meta.append({'company': company,
                            'job_title': job_title, 'location': location, 'job_id': job_id}) 
      doc = Sentence(sent_string, use_tokenizer=SpacyTokenizer(sent_nlp))
      all_sentences.append(doc)


tagger.predict(all_sentences)    

all_data = []
for i in range(len(all_sentences)):
  item = all_sentences[i]           
  for entity in item.get_spans('ner'):
    meta = copy.copy(all_meta[i])
    meta['skill'] = entity.text   
    all_data.append(meta)  
    
df = pd.DataFrame(all_data)
all_data.to_csv('indeed_output3.csv')