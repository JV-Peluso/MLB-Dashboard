#######################################################################
########################## SCRAPE GAMES 2023 ##########################
#######################################################################

import pandas as pd
import re
import os
import shutil
import requests
from bs4 import BeautifulSoup
from datetime import date, timedelta, datetime
import time
import warnings
import pathVP as VP

warnings.filterwarnings('ignore')

def scrapGames():

    try:
        # Define path to save the scraped data files
        finalPath = VP.finalPath()
        backupPath = VP.backupPath()
        fileName = VP.fileName('G')
        teamFile = VP.fileName('AG')
        #DF_GAMES_CS = pd.DataFrame(columns = ['GAME_ID','DATE','TEAM_ID','H_A','W','RUNS', 
        #                                     'BAR_RUNS', 'TEAM_GAME', 'TOOLTIP'])
        DF_GAMES_CS = pd.read_csv(finalPath + '\\' + fileName + '.csv')

        # Backup the current file
        if os.path.exists(finalPath + '\\' + fileName + '.csv'):
            if os.path.exists(backupPath + '\\' + fileName + '.csv'):
                os.remove(backupPath + '\\' + fileName + '.csv')
            shutil.copy(finalPath + '\\' + fileName + '.csv', 
                        backupPath + '\\' + fileName + '.csv')  
            os.rename(finalPath + '\\' + fileName + '.csv', 
                      finalPath + '\\' + fileName + '_.csv')

        # Get the list of teams from the columns of the old seasons file
        DF = pd.read_csv(finalPath + '\\' + teamFile + '.csv', sep = ";")
        teams = DF.columns.tolist()[2:]

        # Create the list with the string that will be searched with RegEx in the website
        lstTeams = []
        dctGames = {}
        for team in teams:
            lstTeams.append("teams/"+team)
            dctGames[team] =  DF_GAMES_CS[DF_GAMES_CS['TEAM_ID'] == team]['TEAM_GAME'].max()        
            #dctGames[team] = 0

        # Read the current games scraped, take the MAX(gameID), and max(DATE) 
        gameID = DF_GAMES_CS['GAME_ID'].max()+1
        last_date = DF_GAMES_CS[DF_GAMES_CS['GAME_ID'] == gameID-1]['DATE'].max()
        if last_date[:3] == '202':
            start_date = date(int(last_date[:4]), int(last_date[5:7]), int(last_date[8:]))
        else:
            start_date = date(int(last_date[6:]), int(last_date[3:5]), int(last_date[:2]))
        start_date = start_date + timedelta(days=1)
        #start_date = date(2023, 3, 30)  
        #gameID = 1
        end_date = date.today()  
        end_date = end_date - timedelta(days=1)
        

        # Iterate over the date range
        current_date = start_date
        old_date = current_date
        contador = 0
        print('Scraping games starting from date {}, from Baseball-Reference website...'.format(start_date.strftime('%d/%m/%Y')))

        while current_date <= end_date:
            day = current_date.day
            month = current_date.month
            year = current_date.year

            # Define the URL
            url = "https://www.baseball-reference.com/boxes/?month=" + str(month) + "&day=" + str(day) + "&year=" + str(year)

            # Send a GET request to the URL and create a BeautifulSoup object and parse the HTML content
            response = requests.get(url)
            soup = BeautifulSoup(response.text, "html.parser")
            td_elements = soup.find_all('td')

            # Regex pattern to find the codes
            pattern = r'\b(?:' + '|'.join(lstTeams) + r')\b'

            # Define variable to be used in the scrape process
            blnToRes = False
            blnNextGame = False
            homeT = ''
            awayT = ''
            runsH = 0
            runsA = 0
            equipo = 0
            barRunsH = 0
            barRunsA = 0
            # For each line search the line with the team and runs scored, and create the line to add it to the dataframe
            for line in td_elements:
                match = re.search(pattern, str(line))
                if match:
                    if equipo == 0:
                        contador +=1
                        awayT = match.group()[6:]
                        dctGames[awayT] +=1
                    else:
                        homeT = match.group()[6:]
                        dctGames[homeT] +=1
                    blnToRes = True
                else:
                    if blnToRes:
                        if equipo == 0:
                            if str(line)[19:20] == '>' or str(line)[19:20] == '<' :
                                runsA = int(str(line)[18:19])
                            else:
                                runsA = int(str(line)[18:20])
                        else:
                            if str(line)[19:20] == '>' or str(line)[19:20] == '<' :
                                runsH = int(str(line)[18:19])
                            else:
                                runsH = int(str(line)[18:20])
                        equipo +=1
                        blnToRes = False
                if equipo > 1:
                    equipo = 0
                    homeW = 0
                    awayW = 0
                    if runsH > runsA:
                        homeW = 1
                        barRunsH = runsH - runsA
                        barRunsA = -1*barRunsH
                    else:
                        awayW = 1
                        barRunsA = runsA - runsH
                        barRunsH = -1*barRunsA
                    
                    if awayW == 1:
                        TTA = '{} {} {} {}-{}'.format(awayT, 'beat', homeT, max(runsA, runsH), min(runsA, runsH))
                        TTH = '{} {} {} {}-{}'.format(homeT, 'lost to', awayT, min(runsA, runsH), max(runsA, runsH))
                    else:
                        TTA = '{} {} {} {}-{}'.format(awayT, 'lost to', homeT,  min(runsA, runsH), max(runsA, runsH))
                        TTH = '{} {} {} {}-{}'.format(homeT, 'beat', awayT, max(runsA, runsH), min(runsA, runsH))
                    recordH = {'GAME_ID' : gameID, 'DATE' : current_date, 'TEAM_ID' : awayT, 'H_A' : 'A', 
                               'W' : awayW, 'RUNS' : runsA, 'BAR_RUNS' : barRunsA, 'TEAM_GAME' : dctGames[awayT],
                               'TOOLTIP' : TTA}
                    DF_GAMES_CS = DF_GAMES_CS.append(recordH, ignore_index=True)

                    recordA = {'GAME_ID' : gameID, 'DATE' : current_date, 'TEAM_ID' : homeT, 'H_A' : 'H', 
                               'W' : homeW, 'RUNS' : runsH, 'BAR_RUNS' : barRunsH, 'TEAM_GAME' : dctGames[homeT],
                               'TOOLTIP' : TTH}
                    DF_GAMES_CS = DF_GAMES_CS.append(recordA, ignore_index=True)
                    gameID += 1

            # Move to the next date
            old_date = current_date
            current_date += timedelta(days=1)

            # Add a pause to avoid scrap block from the website
            time.sleep(5)

        # Once finished the scrap process, export the result into a CSV file 
        if os.path.exists(finalPath + '\\' + fileName + '_.csv'):
            os.remove(finalPath + '\\' + fileName + '_.csv')     
        DF_GAMES_CS.to_csv(finalPath + '\\' + fileName + '.csv', index = False)

        print('Games up to {} recorded correctly scrapped and saved!\n'.format(end_date.strftime('%d/%m/%Y')))

    except Exception as e:

        os.rename(finalPath + '\\' + fileName + '_.csv', 
                  finalPath + '\\' + fileName + '.csv')

        print('\nExecution interrupted by an exception!!!\n')
        print(e)  
        
def scrapLeaders():        

    try:
    
        # Define path to save the scraped data files
        finalPath = VP.finalPath()
        backupPath = VP.backupPath()
        fileName = VP.fileName('SL')
        
        # Backup the current file
        if os.path.exists(finalPath + '\\' + fileName + '.csv'):
            if os.path.exists(backupPath + '\\' + fileName + '.csv'):
                os.remove(backupPath + '\\' + fileName + '.csv')
            shutil.copy(finalPath + '\\' + fileName + '.csv', 
                        backupPath + '\\' + fileName + '.csv')  
            os.rename(finalPath + '\\' + fileName + '.csv', 
                      finalPath + '\\' + fileName + '_.csv')
        
        print('Scraping league leaders from Baseball-Reference website...')
        
        lstRecords = []
        lstFeat = ['BA', 'OPS', 'H', 'HR', 'RBI', 'SB']
        lstFeat2 = ['ERA', 'W', 'WHIP', 'SV', 'IP', 'SO']
        player = ''
        value = ''

        # Send a GET request to the URL and create a BeautifulSoup object and parse the HTML content
        url = "https://www.baseball-reference.com/leagues/majors/2023-batting-leaders.shtml"
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        td_elements = soup.find_all('td')
        
        pattern = r'<td class="rank">1\.'
        pattern2 = r'>(.*?)<'
        prox = False
        valor = False
        contador = 0
        lstCont = 0

        for line in td_elements:
            match = re.search(pattern, str(line))
            if match:
                prox = True
                contador +=1
            if not match and prox and contador in [5,8,13,17,18,21]:
                player = line.find('a')['title'] + " - " + line.find('span', class_='desc').text.strip()
                valor = True
            if not match and not prox and valor and contador in [5,8,13,17,18,21]:
                match2 = re.search(pattern2, str(line))
                if match2:
                    valor = match2.group(1).strip()
                    lstRecords.append([lstFeat[lstCont], player, valor])
                    lstCont +=1
                valor = False
            if prox and valor:
                prox = False

        url = "https://www.baseball-reference.com/leagues/majors/2023-pitching-leaders.shtml"

        # Send a GET request to the URL and create a BeautifulSoup object and parse the HTML content
        response = requests.get(url)
        soup = BeautifulSoup(response.text, "html.parser")
        td_elements = soup.find_all('td')

        pattern = r'<td class="rank">1\.'
        pattern2 = r'>(.*?)<'
        prox = False
        valor = False
        contador = 0
        lstCont = 0

        for line in td_elements:
            match = re.search(pattern, str(line))
            if match:
                prox = True
                contador +=1
            if not match and prox and contador in [3,4,6,11,12,13]:
                player = line.find('a')['title'] + " - " + line.find('span', class_='desc').text.strip()
                valor = True
            if not match and not prox and valor and contador in [3,4,6,11,12,13]:
                match2 = re.search(pattern2, str(line))
                if match2:
                    valor = match2.group(1).strip()
                    lstRecords.append([lstFeat2[lstCont], player, valor])
                    lstCont +=1
                valor = False
            if prox and valor:
                prox = False

        df = pd.DataFrame(lstRecords, columns = ['FEAT', 'PLAYER', 'VALUE'])
        
        # Once finished the scrap process, export the result into a CSV file 
        if os.path.exists(finalPath + '\\' + fileName + '_.csv'):
            os.remove(finalPath + '\\' + fileName + '_.csv')     
        df.to_csv(finalPath + '\\' + fileName + '.csv', index = False)

        print('League leaders correctly scrapped and saved!\n')

    except Exception as e:

        os.rename(finalPath + '\\' + fileName + '_.csv', 
                  finalPath + '\\' + fileName + '.csv')

        print('\nExecution interrupted by an exception!!!\n')
        print(e)  