# -*- coding: utf-8 -*-
"""
Created on Mon May 11 14:31:28 2020

@author: pgood
"""

from pymongo import MongoClient
from flair.data import Sentence, build_spacy_tokenizer
from flair.models import SequenceTagger
from flair.embeddings import BertEmbeddings
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

embeddings = BertEmbeddings()
tagger = SequenceTagger.load(r'/content/drive/My Drive/best-model.pt')

sent_nlp = spacy.blank("en") 
sent_nlp.add_pipe(sent_nlp.create_pipe('sentencizer')) # updated
generic_re = r'We are a.+\n'


rows = []
skill_embeddings = []
for index, job_listing in enumerate(listings_raw):
            if index%10 == 0:
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
                if len(sentance) >= 5 and len(sentance) < 512:

                    doc = Sentence(sentance, use_tokenizer=build_spacy_tokenizer(sent_nlp))
                    predictions = tagger.predict(doc)
                    labels_dict = predictions[0].to_dict(tag_type='ner')
                    
                    all_entities = [item['text'] for item in labels_dict['entities']]
                    embeddings.embed(doc)
                    for token in doc:
                        if token.text in all_entities:
                            tensor = token.embedding.detach().cpu().numpy()
                            skill_embeddings.append((token.text, tensor))
              


    
from sklearn.cluster import KMeans
import numpy as np

embeddings_df = pd.DataFrame(skill_embeddings, columns = ['skill', 'embedding'])
embeddings_df['skill'] = embeddings_df['skill'].map(lambda x: x.lower())
skill_counts = embeddings_df.groupby('skill').size()
avg_embed = embeddings_df.groupby('skill')['embedding'].apply(np.mean)
full_df = pd.concat([skill_counts, avg_embed], axis = 1)
full_df.columns = ['count', 'embedding']
full_df = full_df.loc[full_df['count'] >=5]

skill_arrays = [x for x in full_df['embedding'].values]
skill_arrays = np.array(skill_arrays)


km = KMeans(25, max_iter=1000)
km.fit(skill_arrays)
avg_embed = pd.DataFrame(avg_embed)
full_df['cluster'] = km.labels_ 

top_skills = full_df.sort_values(by=['cluster', 'count'], ascending=[True,False]).groupby('cluster').head(5)
print(len(top_skills))
top_skills.to_csv('km_clusters.csv')