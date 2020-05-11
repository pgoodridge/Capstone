# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 23:25:00 2020

@author: pgood
"""
import pandas as pd
import numpy as np
from gensim import corpora, models
from sklearn import manifold
from scipy.stats import entropy
from scipy.spatial.distance import pdist, squareform
from pymongo import MongoClient


#Transform data output from NER model
skills_df_raw = pd.read_csv('https://raw.githubusercontent.com/pgoodridge/Capstone/master/skills_df2.csv').drop('Unnamed: 0', axis=1)
skills_df = skills_df_raw.copy()
skills_df['skill'] = skills_df_raw['skill'].map(lambda x: x.replace(' ', ''))

attrs_df = skills_df_raw.drop(['skill'], axis=1).drop_duplicates()
attrs_df['company'] = attrs_df['company'].fillna('Unknown')
attrs_df.index = list(range(len(attrs_df)))

grouped_skills = skills_df[['dice_id', 'skill']].groupby('dice_id').transform(lambda x: ' '.join(x)).drop_duplicates()


#Preprocess the text
num_topics = 25

def preprocess(desc):
    words = []
    try:
        for word in desc.split():
            word = word.lower()
            if word not in ['cloud', 'remote'] and word.find('month') < 0 and word.find('consult') < 0:
               words.append(word.lower())
        return words
    except(TypeError):
        return np.nan
    
grouped_skills['skill_words'] = grouped_skills['skill'].map(preprocess)
    


dictionary = corpora.Dictionary(grouped_skills['skill_words'])
dictionary.filter_extremes(no_below=2, no_above=0.4, keep_n=2000)
dictionary.compactify()
corpus = [dictionary.doc2bow(item) for item in grouped_skills['skill_words']]

#Run the LDA model
lda = models.LdaModel(corpus = corpus, num_topics=25, id2word=dictionary,
                      alpha=.0005, passes=200, eta=.001)

#get the output topic definitions
output = lda.print_topics(num_topics = 25)
topics = [lda.get_document_topics(element) for element in corpus]


connection = MongoClient('ds145952.mlab.com', 45952, retryWrites = False)
db = connection['capstone']
db.authenticate('cuny', 'datasci3nce')


#functions from pyldavis source code
def _jensen_shannon(_P, _Q):
   _M = 0.5 * (_P + _Q)
   return 0.5 * (entropy(_P, _M) + entropy(_Q, _M))


measure = []
for doc in topics:
    for tuple in doc:
        measure.append(tuple[0])
        
counts = []
for i in range(num_topics):
    count = measure.count(i)
    counts.append(count)


#Get the per doc topic probabilities
all_clus = []
vectors =[]
for i in range(len(topics)):
    cluster_vector = np.zeros(num_topics)
    percentage = 0
    for j in range(len(topics[i])):
        all_clus.append(
                {"dice_id": attrs_df.loc[i, 'dice_id'],
                 "top_num":topics[i][j][0], 
                 "percentage":topics[i][j][1]}
            )
        cluster_vector[topics[i][j][0]] = topics[i][j][1]
    vectors.append(cluster_vector)
    

all_clus = pd.DataFrame(all_clus)
all_clus = all_clus.merge(attrs_df, on='dice_id')
#all_clus['share'] = all_clus['percentage'] * all_clus['MarketCap']

#aggregate probability by topic  topic number
company_totals = pd.DataFrame(all_clus.groupby('top_num')['percentage'].sum())
company_totals.reset_index(inplace = True)



##based on pyldavis source code 
tokens = dictionary.token2id.values()
tokes = [value for value in tokens]
topic = lda.state.get_lambda()
topic = topic / topic.sum(axis=1)[:, None]
fnames_argsort = np.asarray(tokes, dtype = np.int_)
topic_term_dists = topic[:,fnames_argsort]


dist_matrix = squareform(pdist(topic_term_dists, metric=_jensen_shannon))
model = manifold.MDS(n_components=2, random_state=0, metric='precomputed')
#manifold like PCA, but is a generalization to find non-linear trends
coords = model.fit_transform(dist_matrix)

all_counts = [tup for item in corpus for tup in item]
corp_counts = np.zeros(len(fnames_argsort))       
for i in range(len(all_counts)):
   corp_counts[all_counts[i][0]] += all_counts[i][1]
corp_probs = corp_counts/np.sum(corp_counts)

#get salient words per topic
tws = []
top_totals = []
word_counts = []
for i in range(num_topics):
    tw = lda.show_topic(i,len(all_counts))
    top_total = 0
    for j in range(30):
        global_prob = corp_probs[dictionary.token2id[tw[j][0]]]
        score = tw[j][1]
        relevence = score*.5 + global_prob*.5
        top_dict = {"top_num": i, "word": tw[j][0], "score" : score, 
                    "global_prob": global_prob, 
                    'saliency': relevence 
                    }
        top_total += corp_counts[dictionary.token2id[tw[j][0]]] 
        tws.append(top_dict)
        word_counts.append(top_total)
    top_totals.append(top_dict)

top_words = pd.DataFrame(tws)
top_words.sort_values(by = ['top_num',  'saliency'], ascending = [True,False], inplace = True)
overall_tops = top_words.groupby('top_num').head(1)

#prepare various cuts needed for graphs

global_probs = top_words.drop_duplicates('word').sort_values(
        by = 'global_prob', ascending = False).head(30)

global_probs['top_num'] = 0

coords_dicts = []
for i in range(num_topics):
    tw = lda.show_topic(i,1)
    coords_dict = {'top_num': i, 
                   "pc1": coords[i][0], 
                   "pc2" : coords[i][1],
                   'topic': overall_tops.iloc[i, 1]
        }
    coords_dicts.append(coords_dict)

coords_df = pd.DataFrame(coords_dicts)
coords_df = coords_df.merge(company_totals, how = 'left', on = 'top_num')
coords_df.fillna(0, inplace = True)


coords_df['percentage'] = coords_df['percentage'].map(lambda x: max(x, 12))


all_clus = all_clus.merge(coords_df[['top_num', 'topic']], on = 'top_num')
company_pcts = all_clus.groupby(['top_num', 'topic', 'company'])['percentage'].sum().reset_index()

coords_df_cos = coords_df[['pc1', 'pc2', 'top_num']].merge(company_pcts, on = 'top_num')

new_top_words = top_words.copy()
new_top_words = new_top_words.sort_values(by = 
   ['top_num',  'saliency'], ascending = [True,False]).groupby('top_num').head(5)
newest_top_words = new_top_words[['top_num', 'word']]
newest_top_words['count'] = newest_top_words.groupby('top_num').cumcount()
newest_top_words['count'] = newest_top_words['count'].map(lambda x:  x+1)
pivot = newest_top_words.pivot(index = 'top_num', columns = 'count', values = 'word').reset_index()
pivot_words = pivot.merge(coords_df[['percentage', 'top_num']], on = 'top_num')
pivot_words.columns = ['Skill ' + str(s_index) for s_index in pivot_words.columns]


full_deets_df = all_clus[['top_num', 'topic', 'dice_id', 'percentage']].merge(
        skills_df_raw.drop_duplicates(), on = 'dice_id')


#load to mongo

db.coords.delete_many({})
db.company_pct.delete_many({})
db.top_words.delete_many({})
db.coords_cos.delete_many({})
db.pivot_words.delete_many({})
db.dice_id_pcts.delete_many({})
db.full_details.delete_many({})

db.coords.insert_many(coords_df.to_dict(orient = 'records'))
db.company_pct.insert_many(company_pcts.to_dict(orient = 'records'))
db.top_words.insert_many(top_words.to_dict(orient = 'records'))
db.coords_cos.insert_many(coords_df_cos.to_dict(orient = 'records'))
db.pivot_words.insert_many(pivot_words.to_dict(orient = 'records'))
db.dice_id_pcts.insert_many(all_clus.to_dict(orient = 'records'))
db.full_details.insert_many(full_deets_df.to_dict(orient = 'records'))