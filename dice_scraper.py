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

def save_job_info(dice_id, job_info):
    fname = 'C:\Dice Files\\' + dice_id + '.json'
    with open(fname, 'w') as f:
        json.dump(job_info, f)
        
    return

def get_dice_id(driver):
    #This is impportant enough it deserves its own fuction
    #dice_id = driver.find_elements_by_xpath("//*[@id='bd']/div/div[2]/div[1]/div[2]/div")[0].text[10:]
    #try both xpaths for locating the element.  Second one is the direct way.
    dice_raw = driver.find_elements_by_xpath('//div[@class="company-header-info"]/div[@class="row"]/div[@class="col-md-12"]')
    if len(dice_raw) > 0:
        print(len(dice_raw))
        
        for item in dice_raw:
            text_raw = item.text
            if text_raw.lower().find('position id') >= 0:
                dice_id = text_raw[text_raw.find(':') + 2:]
                return dice_id
        
    else:
        text_raw = driver.find_element_by_xpath('/html/body/div[2]/div[9]/div/div[2]/div[1]/div[3]/div')
        dice_id = text_raw[text_raw.find(':') + 2:]
        
        return dice_id
    
    return None
    
    
    

def get_job_info(driver, url, timeout = 7):
    
    try:
        element_present = EC.presence_of_element_located((By.XPATH, '//div[@class="company-header-info"]/div[@class="row"]/div[@class="col-md-12"]'))
        WebDriverWait(driver, timeout).until(element_present)
        dice_id = get_dice_id(driver)
        
        if not dice_id:
            dice_id = get_dice_id(driver)
        #if dice_id: #if we can't get an ID, not worth saving
        job_desc = driver.find_element_by_id('jobdescSec').text
        job_title = driver.find_element_by_id('jt').text
        company = driver.find_element_by_id('hiringOrganizationName').text
        location_raw = driver.find_elements_by_xpath('/html/body/div[2]/div[5]/div[2]/div[2]/div/div[1]/ul/li[2]/span')
        
        if len(location_raw) > 0:#location is not required.  find_elements returns an empty list
            #if not found
            location = location_raw[0].text
        else:
            location = ''
        job_attrs = driver.find_elements_by_xpath("//div[@class='iconsiblings']")
    
        attrs_text = []
        for attr in job_attrs:
            attrs_text.append(attr.text)
            
        job_info = { 
            'job_desc' : job_desc,
            'company': company,
            'location': location,
            'job_title' : job_title,
            'job_attrs' : attrs_text,
            'dice_id': dice_id,
            'job_url' : url
        }
        save_job_info(dice_id, job_info)
        
        return
    
    except TimeoutException:
        print('Page timeout')
        
    except:
        print('other problem')
        

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

        driver.quit()
        driver = initialize_driver()
        driver.get('https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&page={}&pageSize=20&filters.employmentType=CONTRACTS&filters.employerType=Direct%20Hire&language=en'.format(page_abs))     
        return driver

def check_links(links, link_ids):
    
    while True:
        random.shuffle(link_ids)
        link_id = link_ids.pop()
        if link_id <= len(links):
            break
        
    return link_id

def get_link_info(driver, new_links=True, link_ids=None):
    
        links = driver.find_elements_by_xpath('//dhi-search-card//h5//a[@href]')
        link_ids = list(range(len(links)))
        link_id = check_links(links, link_ids)
        url_text = links[link_id].text
        
        return links, link_ids, link_id, url_text


starting_offset = 24

for p_num in range(1, 450):
    page_abs = starting_offset + p_num
    
    if p_num == 1:
        driver = initialize_driver()
        driver.get('https://www.dice.com/jobs?countryCode=US&radius=30&radiusUnit=mi&page={}&pageSize=20&filters.employmentType=CONTRACTS&filters.employerType=Direct%20Hire&language=en'.format(page_abs))
       
    if p_num % 5 == 0:
        #to free up memeory
        time.sleep(5)
        driver = restart_driver(driver, page_abs)
        
    time.sleep(3)
    
    links, link_ids, link_id, url_text = get_link_info(driver)

    print(len(links))
    
    for i in range(len(link_ids)):  
        
        try:
            links[link_id].click()
            
        except ElementClickInterceptedException:
            driver.refresh()
            print('page reload')
            time.sleep(3)
            links, link_ids, link_id, url_text = get_link_info(driver)
            
            try:
               links[link_id].click()
            except:
                print('bad link')
                driver = restart_driver(driver, page_abs)
                links, link_ids, link_id, url_text = get_link_info(driver)
                
            
        get_job_info(driver, url_text)
        time.sleep(random.randint(1,2))
        
        #Hit back link
        back_links = driver.find_elements_by_xpath('//*[@id="searchBackUrl"]')
        if len(back_links) == 1:
            back_links[0].click()
            
        else:
            driver.refresh()
            
        time.sleep(random.randint(1,3))
        #Reload the links
        links = driver.find_elements_by_xpath('//dhi-search-card//h5//a[@href]')
        link_id = check_links(links, link_ids)
        url_text = links[link_id].text
        
        
    try:
        driver.find_element_by_xpath('//*[@id="pagination_2"]/pagination/ul/li[7]/a').click()
        print("Starting Page{}".format(p_num))
        
    except ElementClickInterceptedException:
        #ads get in the way
        driver.refresh()
        print('page reload')
        driver.find_element_by_xpath('//*[@id="pagination_2"]/pagination/ul/li[7]/a').click()
        print("Starting Page{}".format(p_num))
    
    except:
        #ec
        restart_driver(driver, page_abs+1)
        print('error, restarting')
        print("Starting Page{}".format(p_num))
    #driver.quit()

driver.quit()
        
    #driver.execute_script("window.history.go(-1)")
    
        
        
