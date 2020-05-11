# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 14:40:43 2020

@author: pgood
"""

from pymongo import MongoClient
import json
import os
import random
import pandas as pd
from flair.data import Sentence, build_spacy_tokenizer
from spacy.matcher import Matcher
import spacy
import pandas as pd
from flair.data import Corpus
from flair.embeddings import TokenEmbeddings, WordEmbeddings, FlairEmbeddings, StackedEmbeddings,\
    BertEmbeddings, PooledFlairEmbeddings, ELMoEmbeddings
from typing import List
from hyperopt import hp
from flair.hyperparameter.param_selection import SearchSpace, Parameter
from torch.optim.adam import Adam
from flair.hyperparameter.param_selection import SequenceTaggerParamSelector, OptimizationValue


def mongo_extract():

    connection = MongoClient('ds145952.mlab.com', 45952)
    db = connection['capstone']
    db.authenticate('cuny', 'datasci3nce')
    
    listings_raw = []
    docs = db['pages_raw'].find()
    for item in docs:
        listings_raw.append(item)
        
    return listings_raw


def find_job_listings(listings_raw):
    
    #Find job listings with at least 5 skills in the skills section
    #and at least 200 characters
    #also discard if not all attributes are present as this
    #is a sign of a bad listing
    
    good_descs = []
    all_skills = []
    for job_listing in listings_raw:
      if len(job_listing['job_attrs']) == 4:
        skills_raw = job_listing['job_attrs'][0].split(',')
        all_skills.append(skills_raw)
        if len(skills_raw) > 4 and len(job_listing['job_desc']) > 200:
            good_descs.append(job_listing['job_desc'])
            
    return good_descs, all_skills


def find_unique(all_skills, bad_skills):
    #manually curate the list of skills
    
    unique_skills = []
    
    for skill_list in all_skills:
        if len(skill_list) >= 2:
            for skill in skill_list:
                skill = skill.strip()
                if len(skill.split()) <=4\
                and skill.lower().find('w2') == -1\
                and skill.lower().find('contract') == -1\
                and skill.lower().find('independent') == -1\
                and skill.lower().find('part time') == -1\
                and skill.lower().find('full time') == -1\
                and skill.lower().find('experience') == -1\
                and skill.lower().find('required') == -1\
                and skill.lower().find('english') == -1\
                and skill.lower().find('computer') == -1\
                and skill.lower().find('recruiter') == -1\
                and skill.lower().find('responsible') == -1\
                and skill.lower().find('education') == -1\
                and skill.lower().find('consult') == -1\
                and skill.lower() not in bad_skills:
                    unique_skills.append(skill)
    
    
    unique_skills = list(set(unique_skills))
    
    return unique_skills


def create_sentences(job_listings):
    
    #divide each job listing into sentences
    #Flair is meant to work on sentences, not documents
    sent_nlp = spacy.blank("en") 
    sent_nlp.add_pipe(sent_nlp.create_pipe('sentencizer'))
    
    all_sentances = []
    for item in job_listings:
        doc = sent_nlp(item)
        for sent in doc.sents:
            all_sentances.append(sent.string.strip())
            
    return all_sentances
    
def create_matchers(unique_skills):
    
    #Prepare the spacy matcher
    #We could randomize case to improve training in future 
    #iterations (avoid overfitting)
    match_lm = spacy.blank("en") 
    matcher = Matcher(match_lm.vocab)
    
    for index, skill in enumerate(unique_skills):
        skill_list = skill.split()
        pattern = [{'LOWER' : skill.lower()} for skill in skill_list]
        matcher.add(str(index), None, pattern)
        
    return matcher, match_lm

def match_max_span(matches):
    
    #We want to find the max span among matched skills
    #So if both AWS Lambda and AWS are matched, we only pick
    #AWS Lambda
    
    match_df = pd.DataFrame(matches)
    new_match_df = match_df.sort_values([1,2], ascending = False).groupby(1).head(1)
    newest_match_df = new_match_df.reset_index().sort_values([2,1]).groupby(2).head(1)
    almost_good = [tuple(tup)[2:4] for tup in newest_match_df[[0,1,2]].itertuples()]   

    rm_inds = []
    for a in range(len(almost_good)-1):
        for b in range(a+1, len(almost_good)):
            if almost_good[a][1] > almost_good[b][0]:
                rm_inds.append(b)
    rm_inds = list(set(rm_inds))            
    for index in sorted(rm_inds, reverse=True):
        almost_good.pop(index)
    
    return almost_good


def tag_it(skill_span, sentence):
    #Flair expects special tags on each entity to identify Single,
    #Begin, inside, and end
    
    for start, end in skill_span:
         if end - start == 1:
             sentence[start].add_tag('ner', 'S-SKILL')
         elif end - start == 2:
             sentence[start].add_tag('ner', 'B-SKILL')
             sentence[start+1].add_tag('ner', 'E-SKILL')
         else:
             token_span = end-start
             sentence[start].add_tag('ner', 'B-SKILL')
             sentence[start+token_span-1].add_tag('ner', 'E-SKILL')
             for x in range(1, end-start-2):
                 sentence[start+x].add_tag('ner', 'I-SKILL')
                 
    return sentence

def match_sentences(all_sentences, matcher, match_lm):
    
    #match and tag all the sentences
    
    spacy_nlp = spacy.blank('en')
    
    raw_descs = []
    all_annos = []
    for job_desc in all_sentences:
        doc = match_lm(job_desc)
        matches = matcher(doc)
        
        if len(matches) > 0:
           almost_good = match_max_span(matches)
           
        else:
            almost_good = []
        sentence = Sentence(job_desc, use_tokenizer=build_spacy_tokenizer(spacy_nlp))
        
        if len(almost_good) > 0:
            
            sentence = tag_it(almost_good, sentence)
 
                        
        raw_descs.append(job_desc)
        all_annos.append(sentence)
            
    return raw_descs, all_annos


bad_skills =  ['but','lead', 'project', 'system', 'systems', 'email', 'director', 'development',
               'design', 'technical', 'data', 'systems', 'requirements', 'system', 'support',
               'applications', '', 'software', 'analysis', 'can', 'code', 'management', 'it'
               'time', 'process', 'san', 'ba', 'manager', 'benefits', 'application', 'developer',
               'develop' ,'field', 'impact', 'production', 'web', 'test', 'report', 'release',
               'core', 'see job description', 'manager', 'engineer', 'engineering'
               ]

listings_raw = mongo_extract()
good_descs, all_skills = find_job_listings(listings_raw)
unique_skills = find_unique(all_skills, bad_skills)
all_sentences = create_sentences(good_descs)
matcher, matcher_lm = create_matchers(unique_skills)
raw_descs, all_annos = match_sentences(all_sentences, matcher, matcher_lm)

print(len(all_annos))

train_data = all_annos[:4500]
test_data = all_annos[4500:6200]
dev_data = all_annos[6200:]


search_space = SearchSpace()

#Create or embedding stacks
#Flair recommends adding GLoVe to their character-level embeddings

flair_normal = StackedEmbeddings([

    WordEmbeddings('glove'),
    FlairEmbeddings('mix-forward'),
    FlairEmbeddings('mix-backward')
    ])
 
bert = BertEmbeddings()
elmo = ELMoEmbeddings('original')
flair_pooled = StackedEmbeddings([

    WordEmbeddings('glove'),
    PooledFlairEmbeddings('mix-forward'),
    PooledFlairEmbeddings('mix-backward')
    ])

search_space.add(Parameter.EMBEDDINGS, hp.choice, options=[
  bert, elmo, flair_normal, flair_pooled
    
])
    
#other hyperparams are kept fixed for this excercise.
#Add to the lists to add to grid
#unfortunately for small grids, Flair picks random search instead of true
#grid search

search_space.add(Parameter.HIDDEN_SIZE, hp.choice, options=[384])
search_space.add(Parameter.RNN_LAYERS, hp.choice, options=[1])
search_space.add(Parameter.DROPOUT, hp.choice, options = [0.0])
search_space.add(Parameter.LEARNING_RATE, hp.choice, options=[.1])
search_space.add(Parameter.MINI_BATCH_SIZE, hp.choice, options=[16])
search_space.add(Parameter.USE_CRF, hp.choice, options=[True])

corpus = Corpus(train_data, test_data, dev_data)
tag_type = 'ner'
tag_dictionary = corpus.make_tag_dictionary(tag_type=tag_type)



param_selector = SequenceTaggerParamSelector (
    corpus, 
    tag_type='ner',
    base_path='tuning/results', 
    max_epochs=55,   
    training_runs=1,
    optimization_value=OptimizationValue.DEV_SCORE
)

#start the search
param_selector.optimize(search_space)