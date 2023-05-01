
import json
import os
import logging
import _pickle as cPickle
import traceback
from typing import Any
import requests
from bs4 import BeautifulSoup, PageElement
from multiprocess import Pool
import traceback

#Deleting previous files
try:
    os.remove("races_data_1_done.json")
    os.remove("races_data.pickle")
    os.remove("parcer_EROR.log")
except:
    pass

#Creating new output files
try:
    with open("races_data_1_done.json", "w") as file:
        json.dump([],file)
    with open(r"races_data.pickle", "wb") as file:
        cPickle.dump([], file)
except:
    pass

#Error logging configuration
logging.basicConfig(level=logging.ERROR, filename="parcer_EROR.log",format="%(asctime)s %(levelname)s %(message)s")

#Main link for parsing
url_main = "https://www.canalturf.com/sitemap/sitemap-res.xml"

soup = BeautifulSoup(requests.get(url_main).text, 'xml')

#Function to check for a number
def is_number(str):
    try:
        float(str)
        return True
    except ValueError:
        return False

#Function to get data about a horse from a table
def parce_horses(url_parce):
    rows = []
    link = BeautifulSoup(requests.get(url_parce).text, 'lxml').find(id="TablePartants")
    for row in link.contents[0].next_sibling:
        rows.append(row)
    return rows

#Race parsing function
def parce_race(url_race):
    try:
        data = {}
        link_parce = url_race.replace("resultats-PMU","pronostics-PMU").replace("\n","")
        race_info = url_race.split("/")
        print(link_parce)
        data["date"] = race_info[4]
        data["race_number"] = int(BeautifulSoup(requests.get(link_parce).text, 'lxml').find("a", class_="btn-primary").text)
        data["hippodrome"] = race_info[5]
        data["race_name"] = race_info[6].split('_')[1].split('.')[0]
        data["race_number_global"] = int(race_info[6].split('_')[0])
        data["url"] = link_parce

        horses_info = parce_horses(link_parce)
        horses = {}
        for horse in horses_info:
            horse_number = horse.next_element.next_element.text
            if is_number(horse_number):
                horses[horse_number] = {}
                horses[horse_number]["horse_name"] = horse.contents[1].next_element.next_element.next_element.text
                if horse.select_one('a[href ^="https://www.canalturf.com/courses_fiche_jockey"]'):
                    horses[horse_number]["jockey_name"] = horse.select_one('a[href ^="https://www.canalturf.com/courses_fiche_jockey"]').get_text()
                else:
                    horses[horse_number]["jockey_name"] = "NO NAME"
                if horse.select_one('a[href ^="https://ad.doubleclick.net/"]'):
                    if is_number(horse.select_one('a[href ^="https://ad.doubleclick.net/"]').text):
                        horses[horse_number]["coefficient"] = float(horse.select_one('a[href ^="https://ad.doubleclick.net/"]').text)
                    else: del horses[horse_number]
                else:
                    if is_number(horse.select_one('a[href ^="https://zeturf.page.link/"]').text):
                        horses[horse_number]["coefficient"] = float(horse.select_one('a[href ^="https://zeturf.page.link/"]').text)
                    else: del horses[horse_number]

        data["horses"] = horses
    except Exception as err:
        logging.error(link_parce)
        logging.exception(err)
        print(err)
        data.clear()
        data["url"] = link_parce
        data["ERROR"] = traceback.format_exc()
    return data

def save_pickle(data):
    with open(r"races_data.pickle", "rb") as file:
        file_upload = cPickle.load(file)
    file_upload += data
    with open(r"races_data.pickle", "wb") as file:
        cPickle.dump(file_upload, file)
    return

def save_json(data):
    with open("races_data_1_done.json", "r") as file:
        json_data = json.load(file)
    for race in data:
        json_data.append(race)
    with open("races_data_1_done.json", "w") as file:
        json.dump(json_data, file, indent=4)
    return

#The main function of passing
def parce():
    #Cycle by year
    for link_year in soup.find_all('loc'):
        #Parsing races by year
        url_year = BeautifulSoup(requests.get(link_year.get_text().replace("\n","")).text, 'xml')
        list_urls = [i.text for i in url_year.find_all('loc')]
        #Multiprocessing code Pool(n) where n is the number of parallel processes
        with Pool(8) as p:
            result = p.map(parce_race,list_urls)
            save_json(result)
            save_pickle(result)
            print("Data saved")
    return True

#Beginning of code
if __name__ == "__main__":
    result_program = parce()
    print(result_program)
