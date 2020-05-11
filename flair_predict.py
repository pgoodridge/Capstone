# -*- coding: utf-8 -*-
"""
Created on Mon May 11 14:53:30 2020

@author: pgood
"""

from pymongo import MongoClient
from flair.data import Sentence, build_spacy_tokenizer
from flair.models import SequenceTagger
import spacy
import pandas as pd
import re


connection = MongoClient('ds145952.mlab.com', 45952)
db = connection['capstone']
db.authenticate('cuny', 'datasci3nce')

listings_raw = []
docs = db.pages_raw.find()
for item in docs:
    listings_raw.append(item)


tagger = SequenceTagger.load(r'/content/drive/My Drive/best-model.pt')

sent_nlp = spacy.blank("en") 
sent_nlp.add_pipe(sent_nlp.create_pipe('sentencizer')) # updated
generic_re = r'We are a.+\n'


rows = []
for index, job_listing in enumerate(listings_raw):
            if index//10 == 0:
              print(index)
            job_desc = job_listing['job_desc']
            
            company = job_listing.get('company')
            job_title = job_listing.get('job_title')
            location = job_listing.get('location')
            dice_id = job_listing.get('dice_id')
            if company.lower().find('vdart') >= 0:
                job_desc = re.sub(generic_re, '', job_desc)
            
            all_sentances = []
            doc = sent_nlp(job_desc)
            for sent in doc.sents:
                all_sentances.append(sent.string.strip())
            for sentance in all_sentances:
                if len(sentance) >= 5:

                    doc = Sentence(sentance, use_tokenizer=build_spacy_tokenizer(sent_nlp))
                    predictions = tagger.predict(doc)
                    labels_dict = predictions[0].to_dict(tag_type='ner')
                    
                    all_entities = [item['text'] for item in labels_dict['entities']]
                    
                
                    for item in all_entities:
                        row = (dice_id, company, job_title, location, item)
                        rows.append(row)
                    
skills_df = pd.DataFrame(rows, columns = ['dice_id', 'company', 'job_title', 'location', 'skill'])
skills_df.fillna('', inplace = True)
skills_df.to_csv('skills_df.csv')
