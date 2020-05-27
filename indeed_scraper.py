# -*- coding: utf-8 -*-
"""
Created on Sun Mar  1 18:25:09 2020

@author: pgood
"""

from selenium import webdriver
import time
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException
import json
from datetime import datetime
import random
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import hashlib

def save_job_info(dice_id, job_info):
    fname = 'C:\\Indeed Files perm\\' + dice_id + '.json'
    with open(fname, 'w') as f:
        json.dump(job_info, f)
        
    return


def length_check(element):
    
    if len(element) >=1:
        
        return element[0].text

    else:
        
        return ''

def get_job_info(driver):
    try:
        
        time.sleep(random.randint(1,2))
        
        job_desc = driver.find_elements_by_id('vjs-tab-job')
        job_title = driver.find_elements_by_id('vjs-jobtitle')
        company = driver.find_elements_by_id('vjs-cn')
        location = driver.find_elements_by_id('vjs-loc')
        
        unique_name = length_check(job_title)+length_check(company)+length_check(location)
        print(unique_name)
        if len(unique_name) > 1:#only save if we have a valid title and employer
            job_id  = hashlib.md5(unique_name.encode('utf-8')).hexdigest()
                
            job_info = { 
                'job_desc' : length_check(job_desc),
                'company': length_check(company),
                'location': length_check(location),
                'job_title' : length_check(job_title),
                'job_id': job_id
            }
            save_job_info(job_id, job_info)
        
        
        
    except:
        print('other problem')
    
    return
        

def initialize_driver():
    options = Options()
    #options.add_argument('--headless')
    
    fp = webdriver.FirefoxProfile()
    fp.set_preference("geo.enabled", False)#so Dice doesn't change the URL you pass it
    fp.set_preference('headless', True)
    driver = webdriver.Firefox(options = options, firefox_profile = fp)
    #driver.set_window_size(1600, 900)
    
    return driver

def restart_driver(driver, page_abs):

    plabel = page_abs * 10
    driver.quit()
    driver = initialize_driver()
    
    if page_abs >= 2:
        driver.get('https://www.indeed.com/jobs?q=software+developer&fromage=7&start={}'.format(plabel))
        
    else:
        driver.get('https://www.indeed.com/jobs?q=software+developer&fromage=7')
    
    return driver

starting_point = 43

for p_num in range(1, 450):
    
    page_abs = starting_point + p_num
    job_num = page_abs*10
    
    if p_num == 1:
        driver = initialize_driver()
        driver.get('https://www.indeed.com/jobs?q=software+developer&fromage=7&start={}'.format(job_num))
        
       
    if p_num % 5 == 0:
        #to free up memeory
        time.sleep(5)
        driver = restart_driver(driver, page_abs)
        
    time.sleep(3)
    class_name = 'jobsearch-SerpJobCard unifiedRow row result clickcard'
    links = driver.find_elements_by_xpath('//div[@class="{}"]//h2//a[@href]'.format(class_name))  
    
    #for link in links:
    #    hrefs.append(links[1].get_attribute('href'))
    link_ids = list(range(len(links)))
    print(len(links))
    for i in range(len(link_ids)):
        
        random.shuffle(link_ids)
        if len(link_ids) > 0:
            link_id = link_ids.pop()
        else:
            break
        
        try:
            links[link_id].click()
            
        except:
            
            driver.refresh()
            print('page reload')
            time.sleep(3)
            class_name = 'jobsearch-SerpJobCard unifiedRow row result clickcard'
            links = driver.find_elements_by_xpath('//div[@class="{}"]//h2//a[@href]'.format(class_name))
            
            link_ids = list(range(len(links)))
            random.shuffle(link_ids)
            link_id = link_ids.pop()
            links[link_id].click()
            
        get_job_info(driver)
        time.sleep(random.uniform(0,1))
        
    try:
        driver.find_element_by_xpath('//span[@class="np"]').click()
        print("Starting Page{}".format(p_num))
        
    except:
        #ads are a bitch
        #restart_driver(driver, p_num+starting_point)
        driver.refresh()
        print('page reload')
        driver.find_element_by_xpath('//span[@class="np"]').click()
        print("Starting Page{}".format(p_num))

driver.quit()
            
        
        
