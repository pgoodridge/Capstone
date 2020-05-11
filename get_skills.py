# -*- coding: utf-8 -*-
"""
Created on Wed Apr  1 23:47:15 2020

@author: pgood
"""

from pymongo import MongoClient
import json
import os
import random
import pandas as pd
from flair.data import Sentence, build_spacy_tokenizer


connection = MongoClient('ds145952.mlab.com', 45952)
db = connection['capstone']
db.authenticate('cuny', 'datasci3nce')

listings_raw = []
docs = db['pages_raw'].find()
for item in docs:
    listings_raw.append(item)

good_descs = []
all_skills = []
for job_listing in listings_raw:
  if len(job_listing['job_attrs']) == 4:
    skills_raw = job_listing['job_attrs'][0].split(',')
    all_skills.append(skills_raw)
    if len(skills_raw) > 4 and len(job_listing['job_desc']) > 200:
        good_descs.append(job_listing['job_desc'])
print(len(listings_raw))
       
unique_skills = [skill.strip() for skill_list in all_skills for skill in skill_list\
                     if len(skill_list) >= 2
                     and skill.lower().find('w2') == -1
                     and skill.lower().find('contract') == -1
                     and skill.lower().find('independent') == -1
                     and skill.lower().find('part time') == -1
                     and skill.lower().find('full time') == -1
                     and skill.lower().find('experience') == -1
                     and skill.lower().find('required') == -1
                     and skill.lower().find('english') == -1
                     and skill.lower().find('computer') == -1
                     and skill.lower().find('recruiter') == -1
                     and skill.lower().find('responsible') == -1
                     and skill.lower().find('education') == -1
                     and skill.lower().find('consult') == -1
                     and skill.strip().lower() != 'but'
                     and skill.strip().lower() != 'lead'
                     and skill.strip().lower() != 'project'
                     and skill.strip().lower() != 'system'
                     and skill.strip().lower() != 'systems'
                     and skill.strip().lower() != 'email'
                     and skill.strip().lower() != 'director'
                     and skill.strip().lower() != 'development'
                     and skill.strip().lower() != 'design'
                     and skill.strip().lower() != 'technical'
                     and skill.strip().lower() != 'data'
                     and skill.strip().lower() != 'systems'
                     and skill.strip().lower() != 'requirements'
                     and skill.strip().lower() != 'system'
                     and skill.strip().lower() != 'support'
                     and skill.strip().lower() != 'applications'
                     and skill.strip().lower() != ''
                     and skill.strip().lower() != 'software'
                     and skill.strip().lower() != 'analysis'
                     and skill.strip().lower() != 'can'
                     and skill.strip().lower() != 'code'
                     and skill.strip().lower() != 'management'
                     and skill.strip().lower() != 'it'
                     and skill.strip().lower() != 'time'
                     and skill.strip().lower() != 'process'
                     and skill.strip().lower() != 'san'
                     and skill.strip().lower() != 'ba'
                     and skill.strip().lower() != 'manager'
                     and skill.strip().lower() != 'benefits'
                     and skill.strip().lower() != 'application'
                     and skill.strip().lower() != 'developer'
                     and skill.strip().lower() != 'develop'
                     and skill.strip().lower() != 'field'
                     and skill.strip().lower() != 'impact'
                     and skill.strip().lower() != 'production'
                     and skill.strip().lower() != 'web'
                     and skill.strip().lower() != 'test'
                     and skill.strip().lower() != 'report'
                     and skill.strip().lower() != 'release'
                     and skill.strip().lower() != 'core'
                     and skill.strip().lower() != 'see job description'
                     and skill.strip().lower() != 'manager'
                     and skill.strip().lower() != 'engineer'
                     and skill.strip().lower() != 'engineering'
                     and len(skill.split()) <=4
                     ]
unique_skills = list(set(unique_skills))


#unique_skills = pd.read_csv('skills.csv')
from spacy.matcher import Matcher
import spacy
import pandas as pd
    
    
sent_nlp = spacy.blank("en") 
sent_nlp.add_pipe(sent_nlp.create_pipe('sentencizer')) # updated

all_sentances = []
for item in good_descs:
    doc = sent_nlp(item)
    for sent in doc.sents:
        all_sentances.append(sent.string.strip())
    
match_lm = spacy.blank("en") 
matcher = Matcher(match_lm.vocab)

for index, skill in enumerate(unique_skills):
    skill_list = skill.split()
    pattern = [{'LOWER' : skill.lower()} for skill in skill_list]
    matcher.add(str(index), None, pattern)

raw_descs = []
all_annos = []
for job_desc in all_sentances:
    doc = match_lm(job_desc)
    matches = matcher(doc)
    if len(matches) > 0:
        match_df = pd.DataFrame(matches)
        new_match_df = match_df.sort_values([1,2], ascending = False).groupby(1).head(1)
        newest_match_df = new_match_df.reset_index().sort_values([2,1]).groupby(2).head(1)
        almost_good = [tuple(tup)[2:4] for tup in newest_match_df[[0,1,2]].itertuples()]   
        char_matches = []
        for start, end in almost_good:
            span = doc[start:end]
            char_matches.append((span.start_char, span.end_char, 'SKILL'))
    
        rm_inds = []
        for a in range(len(char_matches)-1):
            for b in range(a+1, len(char_matches)):
                if char_matches[a][1] > char_matches[b][0]:
                    rm_inds.append(b)
        rm_inds = list(set(rm_inds))            
        for index in sorted(rm_inds, reverse=True):
            char_matches.pop(index)
            
    else:
        char_matches = []

    raw_descs.append(job_desc)
    all_annos.append(char_matches)
            

all_data = []

for index, tup in enumerate(all_annos):
    
    obs = (raw_descs[index], {'entities' : tup})
    all_data.append(obs)
    
train_data = all_data[:4500]
test_data = all_data[4500:6200]
    
import plac
import random
from pathlib import Path
import spacy
from spacy.util import minibatch, compounding


nlp = spacy.blank("en") 
ner = nlp.create_pipe("ner")
nlp.add_pipe(ner)
ner.add_label('SKILL')


pipe_exceptions = ["ner", "trf_wordpiecer", "trf_tok2vec"]
other_pipes = [pipe for pipe in nlp.pipe_names if pipe not in pipe_exceptions]

n_iter = 50

optimizer = nlp.begin_training()

with nlp.disable_pipes(*other_pipes):  # only train NER
    with open('pretrained/model345.bin', "rb") as file_:
        ner.model.tok2vec.from_bytes(file_.read())

    for itn in range(n_iter):
        random.shuffle(train_data)
        losses = {}
        batches = minibatch(train_data, size=compounding(4.0, 32.0, 1.001))
        for batch in batches:
            texts, annotations = zip(*batch)
            nlp.update(
                texts,
                annotations,  
                drop=.4,  
                losses=losses,
                sgd=optimizer
            )
        print("Losses", losses)

        
        
from spacy.gold import GoldParse
from spacy.scorer import Scorer

examples = [(doc, examples['entities']) for doc, examples in test_data]
def evaluate(ner_model, examples):
    scorer = Scorer()
    for input_, annot in examples:
        doc_gold_text = ner_model.make_doc(input_)
        gold = GoldParse(doc_gold_text, entities=annot)
        pred_value = ner_model(input_)
        scorer.score(pred_value, gold)
    return scorer.scores

evaluate(nlp, examples)

"""
word_finds = []
for index, obs in enumerate(train_data):
    for find in obs[1]['entities']:
        start = find[0]
        end = find[1]
        word = obs[0][start:end]
        word_finds.append((index, word))
        
words_df = pd.DataFrame(word_finds, columns = ['doc', 'word'])
top_words = words_df.groupby('word').size()
"""