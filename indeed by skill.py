# -*- coding: utf-8 -*-
"""
Created on Wed May 27 09:08:16 2020

@author: peter_goodridge
"""




from selenium import webdriver
import time
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException, StaleElementReferenceException
import json
from datetime import datetime
import random
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import hashlib
import urllib.parse as urlparse
from urllib.parse import parse_qs
import re
from datetime import date, timedelta, datetime
#from google.cloud import bigquery
#from google.oauth2 import service_account
import pandas as pd
import os
import numpy as np

#from random_user_agent.user_agent import UserAgent
#from random_user_agent.params import SoftwareName, OperatingSystem


"""
key_path = "*********"
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
"""


"""
client = bigquery.Client(
    credentials=credentials,
    project=credentials.project_id,
)
"""

#software_names = [SoftwareName.CHROME.value]
#operating_systems = [OperatingSystem.WINDOWS.value]
#user_agent_rotator = UserAgent(software_names=software_names,
                               #operating_systems=operating_systems,

                               #limit=100)
#user_agent = user_agent_rotator.get_random_user_agent()


def save_job_info(job_id, job_info):


    fname2 = 'C:\\Indeed Temp\\' + job_id + '.json'
    #fname2 = 'c:\\Indeed temp\\' + dice_id + '.json'
    with open(fname2, 'w') as f:
        json.dump(job_info, f)        
    return


def load_bq(job_info, running_jobs):
    
    #unused but could be adapted to Azure
    
    row = pd.DataFrame(job_info)
    raw_len = len(row)
    
    row = row.dropna()
    
    diffs = raw_len - len(row)
    
    print('There were {} total jobs and we returned {} jobs'.format(running_jobs, raw_len))
    print("{} Jobs were droppped because of nulls".format(diffs))
    row.to_gbq('DW__test.raw_job_postings_tech', credentials = credentials, project_id = credentials.project_id, if_exists = 'append')
    
    table = client.get_table('DW__test.indeed_scraping_results')
    client.insert_rows(table, [(datetime.timestamp(datetime.now()), running_jobs, raw_len, diffs)])


def length_check(element):
    
    if len(element) >=1:
        
        return element[0].text

    else:
        print('element not found')
        return ''
    
    
def get_date(date_raw):
    

    try:
        
        if date_raw.lower().strip() in ['today', 'just posted']:
            lag = 0

        elif date_raw:   
            regex = re.compile('(\d+)')
            string_raw = re.match(regex, date_raw).group(1)
            lag = int(string_raw)
        else:
            print('no date no error')
            lag = 0
        
        job_date = date.today() - timedelta(lag)
        job_date = job_date.strftime('%m/%d/%Y')
        
    except Exception as e:
        print("Error: " + str(e))
        print('no date', date_raw)
        job_date = date.today().strftime('%m/%d/%Y')
    
    return job_date
    
def get_card_info(cards, card_id):
    
    
    
    company = length_check(cards[card_id].find_elements_by_class_name('companyName'))
    location = length_check(cards[card_id].find_elements_by_class_name('companyLocation'))
    #date = length_check(cards[card_id].find_elements_by_class_name('date'))
    title = length_check(cards[card_id].find_elements_by_xpath('//h2[contains(@class,"jobTitle ")]'))
    
    
                         
    card_info = {'company': company, 'date': get_date(date), 'location': location, 'job_title': title}
    
    return card_info



def get_job_info(driver_instance, job_data, state, job_desc):
    
        
        
        #WebDriverWait(driver, 120).until(EC.presence_of_element_located((By.ID, "vjs-tab-job")))
        #print('job_desc: ', driver2.find_elements_by_class_name('jobsearch-jobDescriptionText'))
        
        #if len(driver_instance.find_elements_by_id('vjs-tab-job')) > 0:
               
               #job_desc = driver_instance.find_elements_by_id('vjs-tab-job')
               
        #else:
            #WebDriverWait(driver, 180).until(EC.presence_of_element_located((By.ID, "vjs-tab-job")))
            #WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.ID, "jobsearch-JobComponent-embeddedBody")))
            #job_desc = driver_instance.find_elements_by_class_name('jobsearch-jobDescriptionText')
            
        
        url = driver_instance.current_url#after a redirect, we'll get the short url that should always have the job_id
        print('Cur url: ', url)
        unique_name = job_data['job_title']+job_data['company']+job_data['location']
        print(unique_name)
        if len(unique_name) > 1 and len(job_desc) > 0:#only save if we have a valid title and employer
            job_id  = hashlib.md5(unique_name.encode('utf-8')).hexdigest()
            parsed = urlparse.urlparse(url)
            try:
                true_job_id = parse_qs(parsed.query).get('vjk')[0]
            except Exception as e:
                print("Error: " + str(e))
                true_job_id = parse_qs(parsed.query).get('jk')[0]
            print('true Job ID:', true_job_id)
            
            if true_job_id:
                true_id = 1
                
            else:
                true_id = 0
                true_job_id = ''
                
            job_info = { 
                'job_desc' : job_desc,
                #'company': length_check(company),
                #'location': length_check(location),
                #'job_title' : length_check(job_title),
                'job_id': job_id,
                'url': url,
                #'job_date': get_date(date_raw),
                'scrape_time': datetime.timestamp(datetime.now()),
                'skill': 'technology', #eventually, we'll exapnd to HT, ENG, etc.
                'state': state,
                'true_job_id': true_job_id,
                'true_id': true_id
                
            }
            job_info.update(job_data)
            save_job_info(job_info['true_job_id'], job_info)
            return 
        

        
        print('Problem with a job!!!')
        


        

def initialize_driver():
    #user_agent = user_agent_rotator.get_random_user_agent()
    options = Options()
    #options.add_argument('--headless')
    #options.add_argument(f'user agent={user_agent}')
    
    fp = webdriver.FirefoxProfile()
    #fp.set_preference("general.useragent.override", user_agent)
    fp.set_preference("geo.enabled", False)#so Dice doesn't change the URL you pass it
    fp.set_preference('browser.cache.memory.enable', False)
    #fp.set_preference('network.cookie.cookieBehavior', 2)
    #fp.set_preference('headless', True)
    driver = webdriver.Firefox(options = options, firefox_profile = fp)
    driver.delete_all_cookies()
    driver.set_window_size(1600, 900)
    
    return driver

def time_filter(driver, query, state):
    
    try:
        driver.find_element_by_id('text-input-where').clear()
    except:
        driver.find_element_by_id('text-input-where').clear()
    finally:
        driver.find_element_by_id('text-input-where').clear()
    #where.send_keys('anywhere')
    
    search_bar = driver.find_elements_by_id('text-input-what')
    search_bar[0].send_keys(query)
    time.sleep(1)
    #search_bar[0].submit()
    #time.sleep(3)
    
    where_bar = driver.find_elements_by_id('text-input-where')
    where_bar[0].send_keys(state)
    time.sleep(1)
    where_bar[0].submit()
    time.sleep(3)
    
    
    date_posted_filter = driver.find_elements_by_id('filter-dateposted')
    if len(date_posted_filter) > 0: #if there are no jobs, there is no filter
    
        date_posted_filter[0].click()
        time.sleep(1)
        driver.find_element_by_partial_link_text('24 hours').click()
    
    

def get_accounts(path, file_name):
    return pd.read_csv(path + file_name, encoding='latin1')


def get_days_diff(client):
    
    #Not used.  I believe this was used to fill in dates when the date was null
    
    import datetime

    q1 = """
    select distinct date
    from DW__test.raw_job_postings 
    ;
    """
    
    dates = client.query(q1).result().to_dataframe()
    
    def convert_date(x):
        return datetime.datetime.strptime(x, '%m/%d/%Y').date()
    
    dates['date'] = dates['date'].map(lambda x: convert_date(x))
    last_date = dates.max()[0]
    
    diff = (datetime.date.today() - last_date).days
    
    return diff

def get_temp(directory):
    
    all_listings=[]
    for _, _, files in os.walk(directory):
        for file in files:
            file_name = os.path.join(directory, file)
            
            with open(file_name, 'r') as f:
                    data = json.load(f)

                    if not data.get('date'):
                        data['date'] = date.fromtimestamp(data['scrape_time']).strftime('%m/%d/%Y')
                            
                    all_listings.append(data)
                    
           
    return pd.DataFrame(all_listings)


def delete_temp(directory):
    
    for _, _, files in os.walk(directory):
        for file in files:
            file_name = os.path.join(directory, file)
            os.remove(file_name)
            

def create_search_entry(key_word='technology'):
    
    #Not used because not restricting salary range
    
    salary = random.choice(list(range(0,91)))
    return '{}'.format(key_word, salary)

def create_direct(state, keyword='banking'):

    #salary = random.choice(list(range(80,91)))
    base_string = 'https://www.indeed.com/jobs?q={}&l={}&fromage=30&filter=0' 
    return base_string.format(keyword, state)

def x_note(driver, html_type, identifier):
    if html_type == 'class_name':
        cookie_note = driver.find_elements_by_class_name(identifier)
    elif html_type == 'id':
        cookie_note = driver.find_elements_by_id(identifier)
    else:
        print('use a different html_type')

    time.sleep(1)
    if len(cookie_note) > 0:
        cookie_note[0].click()
    


def scrape_link(driver, links, link_ids):
        time.sleep(np.random.uniform(4,10))
        random.shuffle(link_ids)
        if len(link_ids) > 0:
            link_id = link_ids.pop()
        else:
            return 'break'
        
        try:
            card_info = get_card_info(links, link_id)
            if card_info['job_title'] == '':
                raise ElementClickInterceptedException
            print(card_info['company'], card_info['location'])
            #job_link = driver.find_element_by_xpath('//h2//a[@href]')
            #job_link.click()
            links[link_id].click()
            #iframes no longer used
            #driver.switch_to.frame('vjs-container-iframe')
            time.sleep(2)
            job_desc = driver.find_elements_by_id('jobDescriptionText')[0].text
        except ElementClickInterceptedException:
            
            time.sleep(4)
            driver.refresh()
            print('page reload')
            time.sleep(3)
            links = driver.find_elements_by_xpath('//ul[@class="{}"]//li/div[contains(@class,"cardOutline")]'.format('jobsearch-ResultsList css-0'))
            link_ids = list(range(len(links)))
            random.shuffle(link_ids)
            #link_id = link_ids.pop()
            
            try:
                card_info = get_card_info(links, link_id)
                if card_info['job_title'] == '':
                    raise ElementClickInterceptedException('Add still blocking')
                #job_link = driver.find_element_by_xpath('//h2//a[@href]')
                #job_link.click()
                #links[link_id].click()
                links[link_id].click()
                #I didn't see iframes used
                #driver.switch_to.frame('vjs-container-iframe')
                time.sleep(2)
                job_desc = driver.find_elements_by_id('jobDescriptionText')[0].text

            except Exception as e:
                
                print("Error: " + str(e))
                time.sleep(4)
                driver.refresh()
                print('page reload')
                time.sleep(3)
                links = driver.find_elements_by_xpath('//ul[@class="{}"]//li/div[contains(@class,"cardOutline")]'.format('jobsearch-ResultsList css-0'))
                link_ids = list(range(len(links)))
                random.shuffle(link_ids)
                #link_id = link_ids.pop()
                card_info = get_card_info(links, link_id)
                links[link_id].click()
                if len(driver.find_elements_by_id('vjs-container-iframe')) > 0:
                    driver.switch_to.frame('vjs-container-iframe')
                    time.sleep(2)
                    job_desc = driver.find_elements_by_id('jobDescriptionText')[0].text
                else:
                    bad_frames.append(card_info)
                    return 'continue'
                #job_link = driver.find_element_by_xpath('//h2//a[@href]')
                #links[link_id].click()
                #job_link.click()
        #company_jobs = get_job_info(driver2, card_info, duns_id, company_jobs)
        get_job_info(driver, card_info, state, job_desc)
        driver.switch_to.parent_frame()
        print('jobs done')
        return 'done'
        #time.sleep(random.uniform(0,1))
      
        
def change_page(driver):
    try:
        pagination = driver.find_elements_by_xpath('//span[@class="pagination-page-next"]')
        if len(pagination) == 2:
            pagination[1].click()
        elif len(pagination) == 1:
            pagination[0].click()
        else:
            return 'break'
        
    except ElementClickInterceptedException:
        #ads...
        #restart_driver(driver, p_num+starting_point)
        driver.refresh()
        print('page reload')
        pagination = driver.find_elements_by_xpath('//span[@class="pagination-page-next"]')
        if len(pagination) == 2:
            pagination[1].click()
        elif len(pagination) == 1:
            pagination[0].click()
        else:
            return 'break'
        
    except Exception as e:
        
        #ads...
        #restart_driver(driver, p_num+starting_point)
        print("Error: " + str(e))
        driver.refresh()
        print('page reload')
        pagination = driver.find_elements_by_xpath('//span[@class="pagination-page-next"]')
        if len(pagination) == 2:
            pagination[1].click()
            
        elif len(pagination) == 1:
            pagination[0].click()
        else:
            return 'break'

#search_string = {'q' : 'company:(COTTAGE HEALTH)'}
#url = urllib.parse.urlencode(search_string)


#regex = re.compile('(page \d+ of )(\d+,?\d*)')
regex = re.compile('(\d+)')

running_jobs=0
bad_frames=[]


driver = initialize_driver()

#Search more groups of states if needed
#Because "Banking" will return many jobs searching over multiple state groups
#should be split accross sessions.

state_list = [
    ["MA", "NH"]
    ]
    
states = state_list[0]


for state in states:
    print(state)
     
    #if random.choice(range(1,5)) == 1:
        #driver.quit()
        #driver = initialize_driver(user_agent_rotator)
        
    #if skill == 'Salesforce':
        #search_string = {'q' : '"{}" title:salesforce'.format(skill)}
    #else:
    #search_string = {'q' : 'company:({})'.format(account)}
    print(create_search_entry())
    #url = urllib.parse.urlencode(search_string)
    #company_jobs = [] #this will be loaded into GCP
    p_num = 1
    
    while True:
        
        time.sleep(np.random.uniform(1,5))
        page_abs = 0 + p_num
        job_num = page_abs*10
        
        if p_num >=75:
            break
        if p_num == 1:
            
            #driver.get('http://indeed.com')
            
            #time_filter(driver, create_search_entry(), state)
            driver.get(create_direct(state))
            x_note(driver, 'class_name', 'gnav-CookiePrivacyNoticeButton')
            #full_url = 'https://www.indeed.com/jobs?{}+${},000%2B&fromage={}'.format(url, random.choice([80,81,82,83,84,85]), job_age)
            #print(full_url)
            #driver.get(full_url)
            
            
            #total_jobs_raw = driver.find_elements_by_xpath('//*[@id="searchCountPages"]')
            
            total_jobs_class = 'jobsearch-JobCountAndSortPane-jobCount'
            total_jobs_raw = driver.find_elements_by_xpath('//div[@class="{}"]'.format(total_jobs_class))
            
            tos = driver.find_elements_by_css_selector('.tos-Button')
            print('tos:', tos)
            if len (tos) > 0:
                tos[0].click() 

            print(total_jobs_raw)
            if len(total_jobs_raw) > 0:
                total_jobs_string = total_jobs_raw[0].text
                string_raw = re.match(regex, total_jobs_string.lower()).group(1)
                total_jobs = int(string_raw.replace(',', ''))
                running_jobs += total_jobs
                total_pages = total_jobs//10 + 1
            else: 
                break
           
        
        """
        if p_num % 10 == 0:
            #to free up memeory
            time.sleep(5)
            driver = restart_driver(driver, page_abs, account)
        """
            
        time.sleep(3)
        #class_name = 'mosaic-jobcards'
        
        #links = driver.find_elements_by_xpath('//div[@class="{}"]//h2//a[@href]'.format(class_name))  
        links = driver.find_elements_by_xpath('//ul[@class="{}"]//li/div[contains(@class,"cardOutline")]'.format('jobsearch-ResultsList css-0'))
        
            
        
        #for link in links:
        #    hrefs.append(links[1].get_attribute('href'))
        link_ids = list(range(len(links)))
        print(len(links))
        for i in range(len(link_ids)):
            try:
                result = scrape_link(driver, links, link_ids)
                if result == 'continue':
                    continue
                elif result == 'break':
                    print('break from inner loop!!!')
                    break

            except StaleElementReferenceException:
                links = driver.find_elements_by_xpath('//ul[@class="{}"]//li/div[contains(@class,"cardOutline")]'.format('jobsearch-ResultsList css-0'))
                result = scrape_link(driver, links, link_ids)
                if result == 'continue':
                    continue
                elif result == 'break':
                    print('break from inner loop!!!')
                    print('yolo')
                    break            
            except Exception as e:
                print(e)
                driver.refresh()
                time.sleep(4)
                print('break from inner loop!!!')
                print('yolo')
                break

        try:
            page_result = change_page(driver)
            if page_result == 'break':
                print('break from outer loop!!!')
                time.sleep(5)
                break
            elif page_result == 'continue':
                print('break from outer loop!!!')
                time.sleep(5)
                continue
        except Exception as e:
            page_result = change_page(driver)
            print("Error: " + str(e))
            if page_result == 'break':
                print('break from outer loop!!!')
                time.sleep(5)
                break
            elif page_result == 'continue':
                continue

        p_num+=1
        print("Starting Page{}".format(p_num))
        dupe_text = driver.find_elements_by_class_name('dupetext')
        if p_num > total_pages or len(dupe_text) > 0:
            break  
    #load_bq(company_jobs)
    
    
#all_jobs = get_temp('/home/peter_goodridge/indeed-scraping/Indeed temp/')
#load_bq(all_jobs, running_jobs)
#delete_temp('/home/peter_goodridge/indeed-scraping/Indeed temp/') 