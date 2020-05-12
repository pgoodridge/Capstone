# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 11:38:40 2020

@author: pgood
"""

import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import dash_table
from dash.dependencies import Input, Output
import pandas as pd
from pymongo import MongoClient

#define the mongo connection
connection = MongoClient('ds145952.mlab.com', 45952, retryWrites = False)
db = connection['capstone']
db.authenticate('cuny', 'datasci3nce')

#Extract some of the larger DFs upfront so we don't need to compute these
#on every client request

raw_skill_data = []
iterator = db.full_details.find()
for row in iterator:
    raw_skill_data.append(row)

raw_skill_df = pd.DataFrame(raw_skill_data)

all_titles = [{'label' : row['job_title'], 'value' : row['dice_id']}
    for index, row in raw_skill_df[['job_title', 'dice_id']].drop_duplicates().iterrows()]

all_skills = [{'label' : row['skill'], 'value' : row['skill']}
    for index, row in raw_skill_df[['skill']].drop_duplicates().iterrows()]

cateogries_df = (raw_skill_df[['topic', 'top_num']]
    .groupby('topic')
    .size()
    .reset_index()
    .sort_values(by='topic')
)
    
all_categories = [{'label' : row['topic'] + ' ({})'.format(row[0]), 'value' : row['topic']}
    for index, row in cateogries_df.iterrows()]


def get_cos ():
    iterator = db.company_pct.find()
    all_cos = []
    for row in iterator:
        all_cos.append(row)
        
    return pd.DataFrame(all_cos)

def company_list():
    cos = get_cos()
    cos_list = cos['company'].drop_duplicates().sort_values().to_list()
    cos_list.insert(0, 'Select All')
    
    return cos_list

def raw_data():
    
    iterator = db.dice_id_pcts.find()
    all_docs = []
    for row in iterator:
        all_docs.append(row)
        
    return pd.DataFrame(all_docs)

def table_data_default():
    
    raw = []
    docs = db.pivot_words.find()
    for item in docs:
        raw.append(item)
        
    return pd.DataFrame(raw)

#define our vizes used in callbacks

def word_graph(word_probs, topic):
    figure={
            'data': [go.Bar(x = word_probs['word'], y = word_probs['saliency'],
                             marker = {'color':'rgba(55, 128, 191, 0.7)'}
                            )],
             
            'layout': {
                'title': 'Topic "{}" '.format(topic) + 'Word Relevance'
            }
        }
    return figure

def company_portions(co_tops, topic):
    figure={
            'data': [
                    go.Bar(
                        y = co_tops['company'], 
                        x = co_tops['percentage'],
                        orientation  = 'h',
                        text = co_tops['company'],
                        marker = {'color':'rgba(55, 128, 191, 0.7)'}
                    )],
             
            'layout': {
                'xaxis': {'tickformat': ',.0%',  'range': [0,10]},
                'yaxis' : {'automargin': True},
                'title': 'Topic "{}" '.format(topic) + 'Companies'
            }
        }
    return figure

def scatter_graph(graph_data):
    
    traces = []
    sizeref = 2.*max(graph_data['percentage'])/(40.**2)
    data = go.Scatter(
            x = graph_data['pc1'], 
            y = graph_data['pc2'],
            mode = 'markers',
            #name = topic,
            marker={
                    
                    'size': graph_data['percentage'],
                    'sizemode' : 'area',
                    'sizeref': sizeref,
                    'sizemin': 4,
            },
            text = graph_data['top_num'],
            customdata = graph_data['topic'],
            hovertemplate = "Category: %{customdata} <br>Category Number: %{text}"
        )
    traces.append(data)
    figure={
        'data': traces,
                 
        'layout': {
            'xaxis': {'showticklabels' :False},
            'yaxis': {'showticklabels' :False},
            'title': 'Topic Explorer'
        }
    }

    return figure

#########################Styling##############################################

side_style = {
    'height': '8%',
    'width' : '15%',
    'position': 'absolute',
    'z-index': 1,
    'top': 0,
    'left': 0,
    'padding-top': '10px',
}

graph_style = {
    'margin-left': '20%',
    'padding': '0px 10px'
}

graph_1_style = {'height': '35%', 'width': '70%',  'bottom': 0, 'padding-top': '10px', 
                 'padding-left': '10px', 'position': 'absolute', 'left': '400px'}

graph_2_style = {'height': '65%', 'width': '70%', 'top' : 0,'padding-top': '20px',
                 'position' : 'absolute'}

graph_3_style = {'height': '65%', 'width': '25%', 'bottom': 0, 'padding-top': '10px', 
                 'padding-left': '1px', 'left': '60px', 'position' : 'absolute'}

graph_4_style = {
    'bottom': 0,
    'margin-left':'60%',
    'position' : 'absolute'
}

raw_style = {
        'border': 'thin lightgrey solid',
        'overflowY': 'scroll',
  
    'height' : '80%', 'width': '40%', 'bottom' : '0px', 'left' : '0px' , 'padding-left': '10px',
    'position': 'absolute'}

dd_style = {'width' : '50%', 'right': '0px', 'top': '0px', 'position': 'absolute', 'padding-top': '5px'}

sector_title = {'width' : '30%', 'left': '0px', 'top': '75px', 'position': 'absolute', 'padding-top': '5px'}
sector_style = {'width' : '20%', 'left': '0px', 'top': '120px', 'position': 'absolute', 'padding-top': '5px'}


tabs_styles = {
    'height': '44px'
}
tab_style = {
    'borderBottom': '1px solid #d6d6d6',
    'padding': '6px',
    'fontWeight': 'bold'
}

tab_selected_style = {
    'borderTop': '1px solid #d6d6d6',
    'borderBottom': '1px solid #d6d6d6',
    'backgroundColor': '#119DFF',
    'color': 'white',
    'padding': '6px'
}

table_style = {
    'top': '120px',
    'right': '15px', 
    'position': 'absolute',
    'font-size': '80%'
}

st_style = {
    'top': '250px',
    'right': '600px', 
    'position': 'absolute',
    'font-size': '80%'
}

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css'    ]
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.config['suppress_callback_exceptions'] = True


################################App Layout#####################################

app.layout = html.Div(children = [
        html.Div(style = side_style, children = [
                html.H3('Skills Market'),
                dcc.Tabs(id="tabs_input", value='tab1', children=[
                dcc.Tab(label='Overview', value='tab1', style =  tab_style, selected_style = tab_selected_style),
                dcc.Tab(label='Search', value='tab2', style = tab_style, selected_style = tab_selected_style)
        ])
    ]),
    html.Div(id='tabs_content')
])

@app.callback(Output('tabs_content', 'children'),
              [Input('tabs_input', 'value')])

def render_tabs(tabname):
    
    raw = []
    docs = db.coords.find()
    for item in docs:
        raw.append(item)
    
    topics =  pd.DataFrame(raw)
    
    best_top = topics.sort_values('percentage').tail(1)
    best_num = best_top['top_num'].values[0]
    best_name = best_top['topic'].values[0]
    
    cos_raw = []
    docs = db.company_pct.find()
    for item in docs:
        cos_raw.append(item)
    
    cos =  pd.DataFrame(cos_raw).sort_values('percentage').tail(1)
    #best_co = cos['company'].values[0]
    cos = cos[['company']]
    #co_vals = cos.to_dict("rows")
    #cols = [{"name": i, "id": i} for i in cos.columns]
    
    cos_list = company_list()
    
    if tabname == 'tab1': 
        return html.Div(children=[
                html.Div(style = sector_style, children = [ 
                    html.H5('Choose Competitor:'),
                    dcc.Dropdown(
                            id = 'sector', 
                            options = [{'label' : company, 'value': company} 
                                            for company in  cos_list
                                      ],
                            value = 'Select All'
                    )]
                ),
               
                html.Div(style = graph_style, children=[
                        
                         dcc.Graph(
                            id='topic_explorer', style = graph_2_style,
                            clickData={'points': [{'text': best_num, 
                            'customdata': best_name}]},
                    ),
                    dcc.Graph(id='word_probs', style = graph_1_style ),
                    dcc.Graph(id = 'company_score', style = graph_3_style)
                ])
                     
            ]) 
    elif tabname == 'tab2':
       return  html.Div(children=[
                       html.Div(style=dd_style, children=[

                            dcc.Dropdown(options=all_categories, id='dd_cat'),
                            dcc.Dropdown(options=all_skills, id='dd_skill'),
                            dcc.Dropdown(id = 'dd_title', options=all_titles)                                   
                        ]),
                       dcc.Graph(id = 'company_break', style = graph_4_style,
                                 clickData={'points': [{ 'label': '7'}]}),
    
                       html.Div(style = raw_style, children = [ 
                           html.H2('Job Listing (skill words in bold)'),
                           html.P(id = 'raw_text')
                       ]),
                       html.Div(id = 'table', style = table_style),
                       html.Div(style = st_style, id = 'skill_table', children=[
                           dash_table.DataTable(id='skill_table',
                              columns=[{'name': 'skill', 'id': 'skill'}],
                              page_size=13
                            )
                       ])

                   ])


###################################Callbacks#################################
       
##############Market Tab##############
       
@app.callback(Output('topic_explorer', 'figure'),
              [Input('sector', 'value')]
    )

def topic_exlorer(company_choice):#topic explorer
    import pandas as pd
    
    if company_choice == 'Select All':
        
        raw = []
        docs = db.coords.find()
        for item in docs:
            raw.append(item)
    
    else:
        
        raw = []
        docs = db.coords_cos.find({'company': company_choice})
        for item in docs:
            raw.append(item)
        
    
    graph_data =  pd.DataFrame(raw)
    

        
    return scatter_graph(graph_data)
    
    


@app.callback(
        Output('word_probs', 'figure'),
        [Input('topic_explorer', 'clickData')]
        )


def update_word_probs(clickData):#Word Frequency Graph
    
    #topic_df = get_topics()
    topic = int(clickData['points'][0]['text'])
    name = clickData['points'][0]['customdata']
    
    
    raw = []
    docs = db.top_words.find({'top_num' : topic})
    for item in docs:
        raw.append(item)
    topic_words = pd.DataFrame(raw).sort_values(by='saliency', ascending=False)

   
    figure = word_graph(topic_words, name)
    return figure

@app.callback(
        Output('company_score', 'figure'),
        [Input('topic_explorer', 'clickData')]
    )


def update_cos(clickData): #Company breakdown graph
    #company_df = get_cos()
    topic = int(clickData['points'][0]['text'])
    name = clickData['points'][0]['customdata']  
    
    raw = []
    docs = db.company_pct.find({'top_num' : topic})
    for item in docs:
        raw.append(item)
        
    df = pd.DataFrame(raw)
    df.sort_values(by = 'percentage', inplace = True, ascending = False)

    #company_topics = company_df.loc[
    #       company_df.topic == topic, :].sort_values(by = 'percentage', ascending = False)
    
    rows = len(df.index)
    company_topics = df.head(min(15, rows))
    
    figure = company_portions(company_topics, name)
    return figure 

@app.callback(
        Output('dd_skill', 'options'),
        [Input('dd_cat', 'value')]
    )


############Search Tab############

def filter_skill(value): #Skill dropdown
    
    filtered_df = (raw_skill_df.loc[(raw_skill_df.topic == value) & (raw_skill_df.percentage > .3),
            ['skill', 'topic']]
        .groupby('skill')
        .size()
        .sort_values(ascending=False)
        .reset_index()
    )
    all_skills = [{'label' : row['skill'] + ' ({})'.format(row[0]), 'value' : row['skill']}
        for index, row in filtered_df.iterrows()] 
    
    return all_skills

@app.callback(
        Output('dd_title', 'options'),
        [Input('dd_skill', 'value'), Input('dd_cat', 'value')]
    )

def filter_title(skill, cat): #category dropdown.  Titles filtered in the
    #dd_title div directly
    
    filtered_df = (raw_skill_df.loc[(raw_skill_df['skill'] == skill) & (raw_skill_df['topic'] == cat),
            ['dice_id', 'job_title']]
        .drop_duplicates()
        .sort_values(by='job_title')
    )
    all_titles = [{'label' : row['job_title'], 'value' : row['dice_id']}
        for index, row in filtered_df.iterrows()] 
    
    return all_titles

@app.callback(
        Output('company_break', 'figure'),
        [Input('dd_title', 'value')]
    )      

def pie_graph(value): #Company Breakdown
    
    raw = []
    docs = db.dice_id_pcts.find({'dice_id' : value})
    for item in docs:
        raw.append(item)
        
    df = pd.DataFrame(raw)
    #print("MY Value::::::::::::::::" + value)
    #print(raw)
    #df = get_pcts()
    #df = df.loc[df['ticker'] == value, :]
    
    
    figure = {
            'data': [go.Pie(labels = df['top_num'], 
                           values = df['percentage'],
                           hoverinfo = 'text+percent',
                           text = df['topic']
                           
                    )],
            'layout': {
                'title': '{} Category Breakdown'.format(value),
                'showlegend': False
            }
        }
    return figure


@app.callback(
        Output('raw_text', 'children'),
        [Input('company_break', 'clickData'),
         Input('dd_title', 'value')]
    )

def highlight_text(clickData, value): #Highlight skill words in job desc
    
    
    topic = int(clickData['points'][0]['label'])
    page = db.pages_raw.find_one({'dice_id' : value})['job_desc']
    
    top_words = []
    docs = db.top_words.find({'top_num': topic})
    for item in docs:
        top_words.append(item['word'])
    print(top_words)
    #top_words = topics.loc[topics.top_num == topic, 'word'].values
    new_words = page.split()
    
    """
    children = []
    for word in new_words:
        if word in top_words:
            children.append(html.Span(word, style = {'color': 'red'}))
        else:
            children.append(html.P(word))
    """
    
    my_string = ''
    for word in new_words:
        if word.lower() in top_words:
            my_string += '**{}**'.format(word) + ' '
        else:
            my_string += word + ' '
    
    return dcc.Markdown(my_string)

@app.callback(
    Output('skill_table', 'data'),
    [Input('dd_title', 'value')]
    )

def skill_table(value): #All skills in job desc
    
    df = (raw_skill_df.loc[raw_skill_df.dice_id==value]['skill']
          .drop_duplicates()
          .sort_values()
    )
    df = pd.DataFrame(df)
    print(df)
    data=df.to_dict('records')
    
    return data


@app.callback(
        Output('table', 'children'),
        [Input('dd_title', 'value')]
    )


def attr_table(value): #Listing attributes
    raw = []
    docs = db.pages_raw.find({'dice_id' : value})
    for item in docs:
        desired_attrs = [item['job_title']]
        desired_attrs.append(item['company'])
        desired_attrs.append(item['job_attrs'][2])
        desired_attrs.append(item['job_attrs'][3])
        
        raw.append(desired_attrs)
    columns = ['Title', 'Company', 'Pay', 'WHF']
    df =  pd.DataFrame(raw, columns=columns)
    
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in df.columns])] +

        # Body
        [html.Tr([
            html.Td(df.iloc[i][col]) for col in df.columns
        ]) for i in range(len(df))]
    )

if __name__ == '__main__':
    app.run_server()