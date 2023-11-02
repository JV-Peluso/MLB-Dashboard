import pandas as pd
import os
import shutil
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
import time
import warnings
import pathVP as VP

def scrapData():

    #Process that scraps the data of all the active pleayers of the current season from baseball-reference.com website
    try:
    
        hoy = datetime.today()
        hoy = hoy - timedelta(days=1)
        print('Scraping field players stats from Baseball-Reference, updated to {}...'.format(hoy.strftime('%d/%m/%Y')))
        
        # Read config file values to work with
        currSeason = VP.currentSeason()
        batHeader = VP.headerHitters()
        pitHeader = VP.headerPitchers()
        playersPath = VP.hittersPath()
        pitchersPath = VP.pitcherPath()
        finalPath = VP.finalPath()
        backupPath = VP.backupPath()
        batWeb = 'https://www.baseball-reference.com/leagues/majors/' + str(currSeason) + '-standard-batting.shtml'
        pitWeb = 'https://www.baseball-reference.com/leagues/majors/' + str(currSeason) + '-standard-pitching.shtml'
        
        lstPaths = [playersPath, pitchersPath]
        for item in lstPaths:
            if os.path.exists(item + '\\' + str(currSeason) + '.csv'):
                os.rename(item + '\\' + str(currSeason) + '.csv', 
                          item + '\\' + str(currSeason) + '_.csv')
                
        ###################
        ##### HITTERS #####
        ###################

        # Send a GET request to the URL and create a BeautifulSoup object and parse the HTML content
        response = requests.get(batWeb)
        soup = BeautifulSoup(response.text, "html.parser")
        soup_str = str(soup)
        soup_str = soup_str.split('\n')
        tag = 'data-stat="ranker" >'
        notTag = '<tr class="league_average_table" >'
        contador = 1
        lstList = []
        for line in soup_str:
            if tag in line and not line.startswith(notTag):
                # Parse the line and ind all the td elements with data-stat attribute
                soup2 = BeautifulSoup(line, 'html.parser')
                td_elements = soup2.find_all('td', {'data-stat': True})
                linea = []
                first = True
                # Extract the data-stat value and the text within the tag
                for td in td_elements:
                    data_stat_value = td['data-stat']
                    if first:
                        data_append_csv_value = td.get('data-append-csv')
                        linea.append(contador)
                        first = False
                    value = td.text.strip()
                    linea.append(value.replace('\xa0', ' '))

                linea.append(data_append_csv_value)
                lstList.append(linea)
                contador +=1
        
        dfH = pd.DataFrame(lstList, columns = batHeader.split(','))
        
        ####################
        ##### PITCHERS #####
        ####################

        print('Scraping pitcher players stats from Baseball-Reference, updated to {}...'.format(hoy.strftime('%d/%m/%Y')))
        
        # Send a GET request to the URL and create a BeautifulSoup object and parse the HTML content
        response = requests.get(pitWeb)
        soup = BeautifulSoup(response.text, "html.parser")
        soup_str = str(soup)
        soup_str = soup_str.split('\n')
        tag = 'data-stat="ranker" >'
        notTag = '<tr class="league_average_table'
        contador = 1
        lstList = []
        for line in soup_str:
            if tag in line and not line.startswith(notTag):
                # Parse the line and ind all the td elements with data-stat attribute
                soup2 = BeautifulSoup(line, 'html.parser')
                td_elements = soup2.find_all('td', {'data-stat': True})
                linea = []
                first = True
                # Extract the data-stat value and the text within the tag
                for td in td_elements:
                    data_stat_value = td['data-stat']
                    if first:
                        data_append_csv_value = td.get('data-append-csv')
                        linea.append(contador)
                        first = False
                    value = td.text.strip()
                    linea.append(value.replace('\xa0', ' '))

                linea.append(data_append_csv_value)
                lstList.append(linea)
                contador +=1
        
        dfP = pd.DataFrame(lstList, columns = pitHeader.split(','))
        
        # Save the dataframes into csv for historical purposes
        dfH.to_csv(playersPath + '\\' + str(currSeason) + '.csv', index = False)  
        dfP.to_csv(pitchersPath + '\\' + str(currSeason) + '.csv', index = False) 

        # Delete the backup copies
        for item in lstPaths:
            if os.path.exists(item + '\\' + str(currSeason) + '_.csv'):
                os.remove(item + '\\' + str(currSeason) + '_.csv')
        
        print('Players stats scrap completed!')
        
        return dfH, dfP
        
    except Exception as e:
            
        for item in lstPaths:
            os.rename(item + '\\' + str(currSeason) + '_.csv', 
                      item + '\\' + str(currSeason) + '.csv')

        print('\nExecution interrupted by an exception!!!\n')
        print(e)    
        