import pandas as pd
import numpy as np
import warnings
import requests
from bs4 import BeautifulSoup
import json
import re
from datetime import date, timedelta, datetime
import time
import os
import math
import pathVP as VP

warnings.filterwarnings('ignore')

def scrapeInfo(player_id):

    # URL of the webpage to scrape
    # Base --> https://www.baseball-reference.com/players 
    # Then, lower case of the first letter of the players id --> /a/
    # Finally, the players id and the shtml extention --> acunaro01.shtml
    url = r"https://www.baseball-reference.com/players/" + player_id[0] + "/" + player_id + ".shtml"

    # Send a GET request to the URL
    response = requests.get(url)

    # Create a BeautifulSoup object to parse the HTML content
    soup = BeautifulSoup(response.text, "html.parser")
    script_tag = soup.find('script', {'type': 'application/ld+json'})

    # Extract the JSON data from the script tag
    json_data = script_tag.string

    # Parse the JSON data
    data = json.loads(json_data)

    # Access and work with the parsed JSON data
    country = data['birthPlace'].split(',')[-1].strip()
    tmpBDate = data['birthDate'].split('-')
    bdate = date(int(tmpBDate[0]), int(tmpBDate[1]), int(tmpBDate[2]))
    try:
        photo = data['image']['contentUrl']
    except:
        photo = ''
    state = ''
    
    # If is northamerican saves the state, else put a hyphen
    if country == 'United States':
        state = data['birthPlace'].split(',')[-2].strip()
    else:
        state = '-'

    # Creates a pause to avoid baseball-reference block the process and returns the data
    time.sleep(6)
    return [bdate, country, state, photo]
    
def cleanDF(df, isPitcher):

    # Takes the csv file and applies process to clean the data
    df = df.fillna(0)
    df['Tm'] = df['Tm'].apply(changeTeamID)
    if isPitcher:
        df['POS'] = 'P'
    else:
        df['POS'] = df['Pos Summary'].apply(playerPos)
    df['LRS'] = df['Name'].apply(LRS)
    df['Name'] = df['Name'].apply(cleanName)
    
    return df
    
def playerPos(position):

    #Creates a more clearer position for the players than the one provided by the csv
    finalPos = ''
    cnt = 0
    tmpPos = str(position).replace('/','').replace('H','').replace('*','')[:3]
    dctPos = {'1' : 'P', '2' : 'C', '3' : '1B', '4' : '2B',
              '5' : '3B', '6' : 'SS', '7' : 'LF', '8' : 'CF',
              '9' : 'RF', 'D' : 'DH', '0' : 'PH'}
              
    for pos in tmpPos:
        if len(finalPos) > 0:
            finalPos = finalPos + ','
        finalPos = finalPos + dctPos[pos]
        
    return finalPos
    
def cleanName(name):
    
    #Clean the players name
    return name.replace('*','').replace('#','').replace('+','')

def changeTeamID(team):
    
    # As there has been changes of teams names and codes, this process update them to the current name and code
    dctTeams = {'CAL' : 'LAA', 'ANA' : 'LAA', 'MON' : 'WSN', 'TBD' : 'TBR', 'FLA' : 'MIA'}
    if team not in dctTeams.keys():
        return team
    else:
        return dctTeams[team]

def teamName(team):

    # Creates a dictionary with the teams full name
    DCT = {'ARI' : 'Arizona Diamondbacks', 'ATL' : 'Atlanta Braves', 'BAL' : 'Baltimore Orioles', 'BOS' : 'Boston Red Sox', 
           'CHC' : 'Chicago Cubs', 'CHW' : 'Chicago White Sox', 'CIN' : 'Cincinnati Reds', 'CLE' : 'Cleveland Guardians', 
           'COL' : 'Colorado Rockies', 'DET' : 'Detroit Tigers', 'HOU' : 'Houston Astros', 'KCR' : 'Kansas City Royals', 
           'LAA' : 'Los Angeles Angels', 'LAD' : 'Los Angeles Dodgers', 'MIA' : 'Miami Marlins', 'MIL' : 'Milwaukee Brewers', 
           'MIN' : 'Minnesota Twins', 'NYM' : 'New York Mets', 'NYY' : 'New York Yankees', 'OAK' : 'Oakland Athletics', 
           'PHI' : 'Philadelphia Phillies', 'PIT' : 'Pittsburgh Pirates', 'SDP' : 'San Diego Padres', 'SFG' : 'San Francisco Giants', 
           'SEA' : 'Seattle Mariners', 'STL' : 'St. Louis Cardinals', 'TBR' : 'Tampa Bay Rays', 'TEX' : 'Texas Rangers', 
           'TOR' : 'Toronto Blue Jays', 'WSN' : 'Washington Nationals', 'MON' : 'Montreal Expos'}
           
    return DCT[team]
    
def LRS(name):
    
    # Depending on the special character on the name of the player, defines if is lefty, righty or switcher
    if '*' in name:
        return 'L'
    elif '#' in name:
        return 'S'
    else:
        return 'R'
 
def checkPlayer(df, row, season):
    
    # Function that checks if a players exist in the PLAYERS dataframe.
    # If it doesn't exist, create it.
    # If exists, update the non demographic data.
    ACT = 'Active'
    maxSeason = VP.currentSeason() 
    if season < maxSeason:
        ACT = 'Retired'
        
    result = df['PLAYER_ID'].str.contains(row['Name-additional'], case=False)
    if result.any() :
        # Update current team
        df.loc[df['PLAYER_ID'] == row['Name-additional'], 'TEAM_ID'] = row['Tm']
        df.loc[df['PLAYER_ID'] == row['Name-additional'], 'ACTIVE'] = ACT
        return df, False
    else:
        # Creation of the record
        demoInfo = scrapeInfo(row['Name-additional']) 
        record = {'PLAYER_ID' : row['Name-additional'], 'FULL_NAME': row['Name'], 
                  'BDATE' : demoInfo[0], 'COUNTRY_ID' : demoInfo[1], 'STATE_ID' : demoInfo[2], 
                  'TEAM_ID' : row['Tm'], 'L_R_S' : row['LRS'], 'POS' : row['POS'],
                  'PHOTO' : demoInfo[3], 'ACTIVE' : ACT}
        df = df.append(record, ignore_index=True)
        return df, True
    
def checkHitter(df, row, season):
    # Function that checks if a player season hitter data exist in the HITTERS dataframe.
    # If it doesn't exist, create it.
    # If exists, skips the process
    result = df[(df['PLAYER_ID'] == row['Name-additional']) & (df['SEASON'] == season) & (df['TEAM_ID'] == row['Tm'])].any(axis=None)
    
    if result.any() :
        return df
    else:           
        record = {'PLAYER_ID' : row['Name-additional'], 'SEASON': season, 'G' : row['G'], 'AB' : row['AB'], 
                  'R' : row['R'], 'H' : row['H'], '2B' : row['2B'], '3B' : row['3B'], 'HR' : row['HR'],
                  'RBI' : row['RBI'], 'SB' : row['SB'], 'CS' : row['CS'], 'BB' : row['BB'], 'SO' : row['SO'], 
                  'AVG' : row['BA'], 'OBP' : row['OBP'], 'SLG' : row['SLG'], 'OPS' : row['OPS'],'TB' : row['TB'], 
                  'GIDP' : row['GDP'], 'HBP' : row['HBP'], 'SF' : row['SF'] + row['SH'], 'IBB' : row['IBB'],
                  'TEAM_ID' : row['Tm']}
    
        df = df.append(record, ignore_index=True)
        return df

def checkPitcher(df, row, season):
    # Function that checks if a player season pitcher data exist in the PITCHERS dataframe.
    # If it doesn't exist, create it.
    # If exists, skips the process
    result = df[(df['PLAYER_ID'] == row['Name-additional']) & (df['SEASON'] == season) & (df['TEAM_ID'] == row['Tm'])].any(axis=None)
    
    if result.any() :
        return df
    else:           
        record = {'PLAYER_ID' :  row['Name-additional'], 'SEASON' :  season, 'G' :  row['G'], 'GS' :  row['GS'], 
                  'CG' :  row['CG'], 'W' :  row['W'], 'L' :  row['L'], 'ERA' :  row['ERA'], 'SHO' :  row['SHO'], 
                  'SV' :  row['SV'], 'IP' :  row['IP'], 'H' :  row['H'], 'R' :  row['R'], 'ER' :  row['ER'], 
                  'HR' :  row['HR'], 'BB' :  row['BB'], 'IBB' :  row['IBB'], 'SO' :  row['SO'], 'HBP' :  row['HBP'], 
                  'BK' :  row['BK'], 'WP' :  row['WP'], 'BF' :  row['BF'], 'WHIP' :  row['WHIP'], 'H9' :  row['H9'], 
                  'HR9' :  row['HR9'], 'BB9' :  row['BB9'], 'SO9' :  row['SO9'], 'TEAM_ID' : row['Tm']}
        
        df = df.append(record, ignore_index=True)
        return df
        
def teamTotalsH(df):  
    # Creates summarized stats totals by team and season
    COLS = ['SEASON', 'TEAM_ID', 'AB', 'R', 'H', '2B', 
            '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 'SO', 
            'TB', 'GIDP', 'HBP', 'SF']

    df = df[COLS]
    groupedDF = df.groupby(['SEASON', 'TEAM_ID']).sum().reset_index()
    groupedDF['GAMES'] = groupedDF['SEASON'].apply(getGames)
    groupedDF = getGamesCS(groupedDF, groupedDF['SEASON'].max())
    
    return groupedDF

def teamTotalsP(df): 
    # Creates summarized stats totals by team and season
    COLS = ['TEAM_ID', 'SEASON', 'G', 'GS', 'CG', 'W', 'L', 'SHO', 
            'SV', 'H', 'R', 'ER', 'HR', 'BB', 'IBB', 
            'SO', 'HBP', 'BK', 'WP', 'BF', 'IP']

    lstTeams = df['TEAM_ID'].unique().tolist()
    groupedDF = df[COLS]
    groupedDF = groupedDF.groupby(['SEASON', 'TEAM_ID']).sum().reset_index()
    seasonInt = groupedDF['SEASON'].max() + 1
    
    for season in range(1983, seasonInt):
        for team in lstTeams:
            IP = calculateIP(df, team, season)
            groupedDF.loc[(groupedDF['TEAM_ID'] == team) & (groupedDF['SEASON'] == season),'IP'] = IP
    
    seasDF = groupedDF.groupby('SEASON').sum().reset_index()
    for season in range(1983, seasonInt):
        IP = calculateIP(groupedDF, 'NO', season)
        seasDF.loc[seasDF['SEASON'] == season,'IP'] = IP
    
    COLS = ['SEASON', 'TEAM_ID', 'P_G', 'P_GS', 'P_CG', 'P_W', 'P_L', 
            'P_SHO', 'P_SV', 'P_H', 'P_R', 'P_ER', 'P_HR', 'P_BB', 'P_IBB', 
            'P_SO', 'P_HBP', 'P_BK', 'P_WP', 'P_BF', 'P_IP']

    groupedDF.columns = COLS
    return groupedDF, seasDF       
    
def calculateIP(df, team, season):

    # Calculates the total Innings pitched, as the traditional sum won't work (1/3 vs 1/10)
    IP=0.0
    thirds = 0.0
    
    if team == 'NO':
        df = df[df['SEASON'] == season]
    else:
        df = df[(df['TEAM_ID'] == team) & (df['SEASON'] == season)]
        
    for index, row in df.iterrows():
        if IP > 0.0:
            loopTpl = math.modf(IP)
            IP = int(loopTpl[1])
            thirds += round(loopTpl[0],1)
        rowTpl = math.modf(row['IP'])
        IP += int(rowTpl[1])
        thirds += round(rowTpl[0],1)
        if thirds > 0.2:
            IP += 1
            thirds = thirds - 0.3
        IP += thirds
        thirds = 0.0

    return IP
    
def calculateIP_Player(Innings):

    # Calculates the total Innings pitched per player, as the traditional sum won't work (1/3 vs 1/10)    
    IP=0.0
    thirds = 0.0
    tplIP = math.modf(Innings)
    IP = tplIP[1]
    thirds = round(tplIP[0],1)
    
    if (thirds > 0.2) & (thirds < 0.6):
        IP +=1
        thirds = thirds - 0.3
    elif (thirds > 0.5) & (thirds < 0.9):
        IP +=2
        thirds = thirds - 0.6
    else:
        IP = IP
        thirds = thirds
        
    return IP + thirds
    
def getGames(season):

    # Get the total games per team and season
    maxSeason = VP.currentSeason()
    if season == 1994 : 
        return 114
    elif season == 1995 : 
        return 144
    elif season == 2020 : 
        return 60
    elif season == maxSeason:
        return 0
    else:
        return 162

def TotalizeHitters(df):

    # As there're players that change team(s) in mid season, to be able to have the accurate stats
    # we have to totalize the values per player and season
    dctTeams = {}
    flag = False
    COLS = ['PLAYER_ID', 'SEASON', 'G', 'AB', 'R', 
            'H', '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 
            'BB', 'SO', 'TB', 'GIDP', 'HBP', 'SF', 'IBB']

    # Summarizes the values that can be added
    seasonRange = range(df['SEASON'].min(), df['SEASON'].max()+1)
    for season in seasonRange:

        dfS = df[df['SEASON'] == season]
        dctTeams = currTeam(dfS)
        df2 = (dfS.groupby(['PLAYER_ID', 'SEASON'], as_index=False)
                  .agg({'G': 'sum', 'AB': 'sum', 'R': 'sum', 'H': 'sum', '2B': 'sum', '3B': 'sum',
                        'HR': 'sum', 'RBI': 'sum', 'SB': 'sum', 'CS': 'sum', 'BB': 'sum', 'SO': 'sum',
                        'TB': 'sum', 'GIDP': 'sum', 'HBP': 'sum', 'SF': 'sum', 'IBB': 'sum'}))

        # Calculates the values that cannot be added       
        df2['AVG'] = round((df2['H']/df2['AB']),3)
        df2['OBP'] = round((df2['H']+df2['BB']+df2['HBP'])/(df2['AB']+df2['BB']+df2['HBP']+df2['SF']),3)
        df2['SLG'] = round(((df2['H']-(df2['2B']+df2['3B']+df2['HR']))+(2*df['2B'])+(3*df2['3B'])+(4*df2['HR']))/df2['AB'],3)
        df2['OPS'] = df2['OBP'] + df2['SLG']
        df2['TEAM_ID'] = df2['PLAYER_ID'].map(dctTeams)
        
        if flag:
            finalDF = pd.concat([finalDF, df2], ignore_index=True)
        else:
            flag = True
            finalDF = df2
        
        # Replaces NaN and inf values and return the new dataframe
        finalDF = finalDF.fillna(0)
        finalDF['SLG'] = finalDF['SLG'].replace([np.inf, -np.inf], 0)
        
    return finalDF

def TotalizePitchers(df):

    # As there're players that change team(s) in mid season, to be able to have accurate stats
    # we have to totalize the values per player and season
    dctTeams = {}
    flag = False
    COLS = ['PLAYER_ID', 'SEASON', 'G', 'GS', 'CG', 'W',
            'L', 'SHO', 'SV', 'H', 'R', 'ER', 'HR', 'BB', 
            'IBB', 'SO', 'HBP', 'BK', 'WP', 'BF', 'IP']

    # Summarizes the values that can be added
    seasonRange = range(df['SEASON'].min(), df['SEASON'].max()+1)
    for season in seasonRange:

        dfS = df[df['SEASON'] == season]
        dctTeams = currTeam(dfS)
        df2 = (dfS.groupby(['PLAYER_ID', 'SEASON'], as_index=False)
                  .agg({'G': 'sum', 'GS': 'sum', 'CG': 'sum', 'W': 'sum', 'L': 'sum', 'SHO': 'sum',
                        'SV': 'sum', 'H': 'sum', 'R': 'sum', 'ER': 'sum', 'HR': 'sum', 'BB': 'sum',
                        'IBB': 'sum', 'SO': 'sum', 'HBP': 'sum', 'BK': 'sum', 'WP': 'sum', 
                        'BF' : 'sum', 'IP' : 'sum'}))

        # Calculates the values that cannot be added and updates the IP
        
        df2['IP'] = df2['IP'].apply(calculateIP_Player)
        df2['ERA'] = round((9*df2['ER']/df2['IP']),2)
        df2['WHIP'] = round((df2['H']+df2['BB'])/df2['IP'],3)
        df2['TEAM_ID'] = df2['PLAYER_ID'].map(dctTeams)
        
        if flag:
            finalDF = pd.concat([finalDF, df2], ignore_index=True)
        else:
            flag = True
            finalDF = df2
        
        # Replaces NaN and inf values and return the new dataframe
        finalDF = finalDF.fillna(0)
        finalDF['WHIP'] = finalDF['WHIP'].replace([np.inf, -np.inf], 0)
        
    return finalDF    
    
def getGamesCS(dfG, season):
    
    # Get the current season games per team
    gamesPath = VP.finalPath() + '\\' + VP.fileName('G') + '.csv'
    df = pd.read_csv(gamesPath)
    dct = df['TEAM_ID'].value_counts()
    for index, row in dfG[dfG['SEASON'] == season].iterrows():
        dfG.loc[(dfG['TEAM_ID'] == row['TEAM_ID']) & (dfG['SEASON'] == season),'GAMES'] = dct[row['TEAM_ID']]
            
    return dfG

def currTeam(df):

    # Returns the final player team, useful when a player is traded
    dctTeams = {}
    for index, row in df.iterrows():
        dctTeams[row['PLAYER_ID']] = row['TEAM_ID']
        
    return dctTeams
    
def hitLeaders(df, currS_Games):
    
    # Process that finds the hitting leaders ina the whole span of seasons
    maxSeason = VP.currentSeason()
    COLS = ['SEASON', 'FEAT', 'PLAYER_ID', 'TEAM_ID', 
            'AVG', 'OPS', 'H', 'HR', 'RBI', 'SB']
            
    dfLead = pd.DataFrame(columns = COLS)
    
    record = {}
    seasonRange = range(df['SEASON'].min(), df['SEASON'].max()+1)
    for season in seasonRange:
        seasonDF = df[df['SEASON'] == season]
        record['SEASON'] = season
        
        if season == maxSeason:
            seasonDF['minPA'] = currS_Games*3.1
        else:
            seasonDF['minPA'] = seasonDF['SEASON'].apply(getGames)
            seasonDF['minPA'] = seasonDF['minPA']*3.1
            
        seasonDF['PA'] = seasonDF['AB']+seasonDF['BB']+seasonDF['HBP']+seasonDF['SF']
        seasonDF['CanLead'] = (seasonDF['PA']>seasonDF['minPA'])
        
        record['AVG'] = 0.0
        record['OPS'] = 0.0
        record['H'] = 0.0
        record['HR'] = 0.0
        record['RBI'] = 0.0
        record['SB'] = 0.0
        record['FEAT'] = 'H'
        record['H'] = seasonDF['H'].max()
        if int(seasonDF[seasonDF['H'] == seasonDF['H'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['H'] == seasonDF['H'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['H'] == seasonDF['H'].max()]['PLAYER_ID'].values[0]
            record['TEAM_ID'] = seasonDF[seasonDF['H'] == seasonDF['H'].max()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
        
        record['AVG'] = 0.0
        record['OPS'] = 0.0
        record['H'] = 0.0
        record['HR'] = 0.0
        record['RBI'] = 0.0
        record['SB'] = 0.0
        record['FEAT'] = 'HR'    
        record['HR'] = seasonDF['HR'].max()
        if int(seasonDF[seasonDF['HR'] == seasonDF['HR'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['HR'] == seasonDF['HR'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['HR'] == seasonDF['HR'].max()]['PLAYER_ID'].values[0]
            record['TEAM_ID'] = seasonDF[seasonDF['HR'] == seasonDF['HR'].max()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
        
        record['AVG'] = 0.0
        record['OPS'] = 0.0
        record['H'] = 0.0
        record['HR'] = 0.0
        record['RBI'] = 0.0
        record['SB'] = 0.0
        record['FEAT'] = 'RBI'            
        record['RBI'] = seasonDF['RBI'].max()
        if int(seasonDF[seasonDF['RBI'] == seasonDF['RBI'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['RBI'] == seasonDF['RBI'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['RBI'] == seasonDF['RBI'].max()]['PLAYER_ID'].values[0]
            record['TEAM_ID'] = seasonDF[seasonDF['RBI'] == seasonDF['RBI'].max()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
        
        record['AVG'] = 0.0
        record['OPS'] = 0.0
        record['H'] = 0.0
        record['HR'] = 0.0
        record['RBI'] = 0.0
        record['SB'] = 0.0
        record['FEAT'] = 'SB'        
        record['SB'] = seasonDF['SB'].max()
        if int(seasonDF[seasonDF['SB'] == seasonDF['SB'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['SB'] == seasonDF['SB'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['SB'] == seasonDF['SB'].max()]['PLAYER_ID'].values[0]
            if (seasonDF[seasonDF['SB'] == seasonDF['SB'].max()]['TEAM_ID'].values[0] == 'WSN') & (int(season) <= 2004):
                record['TEAM_ID'] = 'MON'
            else:
                record['TEAM_ID'] = seasonDF[seasonDF['SB'] == seasonDF['SB'].max()]['TEAM_ID'].values[0]
            
        dfLead = dfLead.append(record, ignore_index=True)
         
        seasonDF = seasonDF[seasonDF['CanLead'] == True]
        
        record['AVG'] = 0.0
        record['OPS'] = 0.0
        record['H'] = 0.0
        record['HR'] = 0.0
        record['RBI'] = 0.0
        record['SB'] = 0.0
        record['FEAT'] = 'AVG'
        record['AVG'] = seasonDF['AVG'].max()
        if int(seasonDF[seasonDF['AVG'] == seasonDF['AVG'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['AVG'] == seasonDF['AVG'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['AVG'] == seasonDF['AVG'].max()]['PLAYER_ID'].values[0]
            record['TEAM_ID'] = seasonDF[seasonDF['AVG'] == seasonDF['AVG'].max()]['TEAM_ID'].values[0]
            
        dfLead = dfLead.append(record, ignore_index=True)
        
        record['AVG'] = 0.0
        record['OPS'] = 0.0
        record['H'] = 0.0
        record['HR'] = 0.0
        record['RBI'] = 0.0
        record['SB'] = 0.0
        record['FEAT'] = 'OPS'    
        record['OPS'] = seasonDF['OPS'].max()
        if int(seasonDF[seasonDF['OPS'] == seasonDF['OPS'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(round(seasonDF[seasonDF['OPS'] == seasonDF['OPS'].max()].shape[0],0)) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['OPS'] == seasonDF['OPS'].max()]['PLAYER_ID'].values[0]
            record['TEAM_ID'] = seasonDF[seasonDF['OPS'] == seasonDF['OPS'].max()]['TEAM_ID'].values[0]
    
        dfLead = dfLead.append(record, ignore_index=True)

    return dfLead

def pitLeaders(df, currS_Games):
    
    # Process that finds the hitting leaders ina the whole span of seasons
    COLS = ['SEASON', 'FEAT', 'PLAYER_ID', 'TEAM_ID', 
            'W', 'ERA', 'WHIP', 'SO', 'IP', 'SV']
    
    dfLead = pd.DataFrame(columns = COLS)
    
    record = {}
    seasonRange = range(df['SEASON'].min(), df['SEASON'].max()+1)
    for season in seasonRange:
        seasonDF = df[df['SEASON'] == season]
        record['SEASON'] = season
        minG = getGames(season)
        if minG == 0:
            minG = currS_Games
            
        record['W'] = 0.0
        record['ERA'] = 0.0
        record['WHIP'] = 0.0
        record['SO'] = 0.0
        record['IP'] = 0.0
        record['SV'] = 0.0
        record['FEAT'] = 'W'
        record['W'] = seasonDF['W'].max()
        if int(seasonDF[seasonDF['W'] == seasonDF['W'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['W'] == seasonDF['W'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['W'] == seasonDF['W'].max()]['PLAYER_ID'].values[0]
            if (seasonDF[seasonDF['W'] == seasonDF['W'].max()]['TEAM_ID'].values[0] == 'WSN') & (int(season) <= 2004):
                record['TEAM_ID'] = 'MON'
            elif seasonDF[seasonDF['W'] == seasonDF['W'].max()]['TEAM_ID'].values[0] == 'FLA':
                record['TEAM_ID'] = 'MIA'
            elif seasonDF[seasonDF['W'] == seasonDF['W'].max()]['TEAM_ID'].values[0] == 'TBD':
                record['TEAM_ID'] = 'TBR'    
            else:
                record['TEAM_ID'] = seasonDF[seasonDF['W'] == seasonDF['W'].max()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
        
        record['W'] = 0.0
        record['ERA'] = 0.0
        record['WHIP'] = 0.0
        record['SO'] = 0.0
        record['IP'] = 0.0
        record['SV'] = 0.0
        record['FEAT'] = 'SO'
        record['SO'] = seasonDF['SO'].max()
        
        record['SO'] = seasonDF['SO'].max()
        if int(seasonDF[seasonDF['SO'] == seasonDF['SO'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['SO'] == seasonDF['SO'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['SO'] == seasonDF['SO'].max()]['PLAYER_ID'].values[0]
            if (seasonDF[seasonDF['SO'] == seasonDF['SO'].max()]['TEAM_ID'].values[0] == 'WSN') & (int(season) <= 2004):
                record['TEAM_ID'] = 'MON'
            elif seasonDF[seasonDF['SO'] == seasonDF['SO'].max()]['TEAM_ID'].values[0] == 'FLA':
                record['TEAM_ID'] = 'MIA'
            elif seasonDF[seasonDF['SO'] == seasonDF['SO'].max()]['TEAM_ID'].values[0] == 'TBD':
                record['TEAM_ID'] = 'TBR'    
            else:
                record['TEAM_ID'] = seasonDF[seasonDF['SO'] == seasonDF['SO'].max()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
        
        record['W'] = 0.0
        record['ERA'] = 0.0
        record['WHIP'] = 0.0
        record['SO'] = 0.0
        record['IP'] = 0.0
        record['SV'] = 0.0
        record['FEAT'] = 'SV'
        record['SV'] = seasonDF['SV'].max()
        if int(seasonDF[seasonDF['SV'] == seasonDF['SV'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['SV'] == seasonDF['SV'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['SV'] == seasonDF['SV'].max()]['PLAYER_ID'].values[0]
            if (seasonDF[seasonDF['SV'] == seasonDF['SV'].max()]['TEAM_ID'].values[0] == 'WSN') & (int(season) <= 2004):
                record['TEAM_ID'] = 'MON'
            elif seasonDF[seasonDF['SV'] == seasonDF['SV'].max()]['TEAM_ID'].values[0] == 'FLA':
                record['TEAM_ID'] = 'MIA'
            elif seasonDF[seasonDF['SV'] == seasonDF['SV'].max()]['TEAM_ID'].values[0] == 'TBD':
                record['TEAM_ID'] = 'TBR'    
            else:
                record['TEAM_ID'] = seasonDF[seasonDF['SV'] == seasonDF['SV'].max()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
        
        record['W'] = 0.0
        record['ERA'] = 0.0
        record['WHIP'] = 0.0
        record['SO'] = 0.0
        record['IP'] = 0.0
        record['SV'] = 0.0
        record['FEAT'] = 'IP'
        
        record['IP'] = seasonDF['IP'].max()
        if int(seasonDF[seasonDF['IP'] == seasonDF['IP'].max()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['IP'] == seasonDF['IP'].max()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['IP'] == seasonDF['IP'].max()]['PLAYER_ID'].values[0]
            if (seasonDF[seasonDF['IP'] == seasonDF['IP'].max()]['TEAM_ID'].values[0] == 'WSN') & (int(season) <= 2004):
                record['TEAM_ID'] = 'MON'
            elif seasonDF[seasonDF['IP'] == seasonDF['IP'].max()]['TEAM_ID'].values[0] == 'FLA':
                record['TEAM_ID'] = 'MIA'
            elif seasonDF[seasonDF['IP'] == seasonDF['IP'].max()]['TEAM_ID'].values[0] == 'TBD':
                record['TEAM_ID'] = 'TBR'    
            else:
                record['TEAM_ID'] = seasonDF[seasonDF['IP'] == seasonDF['IP'].max()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
        
        seasonDF = seasonDF[seasonDF['IP'] >= minG]
        
        record['W'] = 0.0
        record['ERA'] = 0.0
        record['WHIP'] = 0.0
        record['SO'] = 0.0
        record['IP'] = 0.0
        record['SV'] = 0.0
        record['FEAT'] = 'ERA'
 
        record['ERA'] = seasonDF['ERA'].min()
        if int(seasonDF[seasonDF['ERA'] == seasonDF['ERA'].min()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['ERA'] == seasonDF['ERA'].min()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['ERA'] == seasonDF['ERA'].min()]['PLAYER_ID'].values[0]
            if (seasonDF[seasonDF['ERA'] == seasonDF['ERA'].min()]['TEAM_ID'].values[0] == 'WSN') & (int(season) <= 2004):
                record['TEAM_ID'] = 'MON'
            elif seasonDF[seasonDF['ERA'] == seasonDF['ERA'].min()]['TEAM_ID'].values[0] == 'FLA':
                record['TEAM_ID'] = 'MIA'
            elif seasonDF[seasonDF['ERA'] == seasonDF['ERA'].min()]['TEAM_ID'].values[0] == 'TBD':
                record['TEAM_ID'] = 'TBR'    
            else:
                record['TEAM_ID'] = seasonDF[seasonDF['ERA'] == seasonDF['ERA'].min()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
            
        record['W'] = 0.0
        record['ERA'] = 0.0
        record['WHIP'] = 0.0
        record['SO'] = 0.0
        record['IP'] = 0.0
        record['SV'] = 0.0
        record['FEAT'] = 'WHIP'
 
        record['WHIP'] = seasonDF['WHIP'].min()
        if int(seasonDF[seasonDF['WHIP'] == seasonDF['WHIP'].min()].shape[0]) > 1 :
            record['PLAYER_ID'] = str(seasonDF[seasonDF['WHIP'] == seasonDF['WHIP'].min()].shape[0]) + ' players'
            record['TEAM_ID'] = 'Various'
        else:
            record['PLAYER_ID'] = seasonDF[seasonDF['WHIP'] == seasonDF['WHIP'].min()]['PLAYER_ID'].values[0]
            if (seasonDF[seasonDF['WHIP'] == seasonDF['WHIP'].min()]['TEAM_ID'].values[0] == 'WSN') & (int(season) <= 2004):
                record['TEAM_ID'] = 'MON'
            elif seasonDF[seasonDF['WHIP'] == seasonDF['WHIP'].min()]['TEAM_ID'].values[0] == 'FLA':
                record['TEAM_ID'] = 'MIA'
            elif seasonDF[seasonDF['WHIP'] == seasonDF['WHIP'].min()]['TEAM_ID'].values[0] == 'TBD':
                record['TEAM_ID'] = 'TBR'    
            else:
                record['TEAM_ID'] = seasonDF[seasonDF['WHIP'] == seasonDF['WHIP'].min()]['TEAM_ID'].values[0]
        
        dfLead = dfLead.append(record, ignore_index=True)
    
    return dfLead
    
def allTimeLeadersH(df, dct):

    # Creates a dataframe with the top-10 all period leaders of some features
    # With additional data that will be used in the dashboard
    
    # These are the features, and as a comment the extra data, and the aggregation method (if needed)
    lstFeat = ['AVG',  # ['AVG', 'H', 'AB', 'SEASON'], ['AVG', 'SUM', 'SUM', 'COUNT']
               'OPS', # ['HR', 'RBI', 'SEASON'], ['-', '-', '-']
               'H',   # ['H - (2B+3B+HR)','2B', '3B', 'HR'], ['-', '-', '-', '-']
               'HR',  # ['HR', 'IBB', 'SEASON'], ['SUM', 'SUM', 'COUNT']
               'RBI', # ['HR', 'OPS', 'AB', 'SEASON'], ['-', '-', '-', '-']
               'SB']  # ['SB', 'CS', 'SEASON'], ['SUM', 'SUM', COUNT']
           
    #The columns of the new dataframe
    COLS = ['FEAT', 'RANK', 'PLAYER_ID', 'NAME', 'AVG', 'OPS',
            'H', 'HR', 'RBI', 'SB', 'SEASON', 'TEAM_ID', 'TXT']

    # Some useful variables to calculate some totals 
    df['minPA'] = df['SEASON'].apply(getGames)
    df['minPA'] = df['minPA']*3.1
    df['PA'] = df['AB']+df['BB']+df['HBP']+df['SF']
    df['CanLead'] = (df['PA']>df['minPA'])
    lstTxt = []

    # Creates a filtered dataframe, for the BA and OPS, as for leaders is
    # needed to have more PA than Team games * 3.1
    df2 = df[df['CanLead'] == True]
    rnk = 1
    record = {}
    dfAllLead = pd.DataFrame(columns = COLS)

    # Loops through the list pf features and extract the top-10 data
    for feat in lstFeat:
        
        record['AVG'] = 0.0
        record['OPS'] = 0.0
        record['H'] = 0.0
        record['HR'] = 0.0
        record['RBI'] = 0.0
        record['SB'] = 0.0
        
        if feat == 'AVG':

            dfTmp = df2.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['RANK'] = rnk
                record['FEAT'] = feat
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['AVG'] = "{:.3f}".format(row['AVG'])
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(round(df[df['PLAYER_ID'] == record['PLAYER_ID']]['H'].sum()/df[df['PLAYER_ID'] == record['PLAYER_ID']]['AB'].sum(),3))
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['H'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['AB'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SEASON'].count())
                    record['TXT'] = '{} had a career BA of {}, with {} hits in {} at-bats through {} seasons.'.format(record['NAME'],
                                                                                                                          lstTxt[0],
                                                                                                                          lstTxt[1],
                                                                                                                          lstTxt[2],
                                                                                                                          lstTxt[3])
                else:
                    lstTxt = []
                    record['TXT'] = ''
                    
                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)

        elif feat == 'OPS':

            dfTmp = df2.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['RANK'] = rnk
                record['FEAT'] = feat
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['OPS'] =  "{:.3f}".format(row['OPS'])
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['HR'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['RBI'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['AB'].to_list()[0])
                    record['TXT'] = '{} batted {} HR and drove in {} runs in {} at-bats that season.'.format(record['NAME'],
                                                                                                             lstTxt[0],
                                                                                                             lstTxt[1],
                                                                                                             lstTxt[2])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)

        elif feat == 'H':

            dfTmp = df.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            eb = 0
            for index, row in dfTmp.iterrows():

                record['RANK'] = rnk
                record['FEAT'] = feat
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['H'] = row['H']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    eb = df[(df['PLAYER_ID'] == record['PLAYER_ID']) & (df['SEASON'] == record['SEASON'])][['2B', '3B', 'HR']].values.tolist()[0] 
                    lstTxt.append(row['H'] - (eb[0]+eb[1]+eb[2]))
                    record['TXT'] = 'Of those {} hits, {} hitted {} singles, {} doubles, {} triples and {} HR.'.format(row['H'],
                                                                                                                       record['NAME'],
                                                                                                                       lstTxt[0],
                                                                                                                       eb[0],eb[1],eb[2])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)    

        elif feat == 'HR':

            dfTmp = df.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['RANK'] = rnk
                record['FEAT'] = feat
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['HR'] = row['HR']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['HR'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['IBB'].sum())
                    record['TXT'] = 'Through his career, {} batted {} HR and recieved {} intentional walks (both MLB record).'.format(record['NAME'],
                                                                                                                               lstTxt[0],
                                                                                                                               lstTxt[1])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)

        elif feat == 'RBI':

            dfTmp = df.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['RANK'] = rnk
                record['FEAT'] = feat
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['RBI'] = row['RBI']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[(df['PLAYER_ID'] == record['PLAYER_ID']) & (df['SEASON'] == record['SEASON'])]['HR'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == record['PLAYER_ID']) & (df['SEASON'] == record['SEASON'])]['AB'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == record['PLAYER_ID']) & (df['SEASON'] == record['SEASON'])]['OPS'].to_list()[0])
                    record['TXT'] = 'To drive in that runs, {} crushed {} HR on {} at-bats, reaching {} OPS that season.'.format(record['NAME'],
                                                                                                                                     lstTxt[0],
                                                                                                                                     lstTxt[1],
                                                                                                                                     lstTxt[2])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)

        elif feat == 'SB':

            dfTmp = df.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['RANK'] = rnk
                record['FEAT'] = feat
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['SB'] = row['SB']
                record['SEASON'] = row['SEASON']
                if (row['TEAM_ID'] == 'WSN') & (int(row['SEASON']) <= 2004):
                    record['TEAM_ID'] = 'MON'
                else:
                    record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[(df['PLAYER_ID'] == record['PLAYER_ID']) & (df['SEASON'] == record['SEASON'])]['SB'].to_list()[0])
                    lstTxt.append(lstTxt[0] + df[(df['PLAYER_ID'] == record['PLAYER_ID']) & (df['SEASON'] == record['SEASON'])]['CS'].to_list()[0])
                    lstTxt.append(round((lstTxt[0]/lstTxt[1])*100,1))
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SEASON'].count())
                    record['TXT'] = '{} robbed {} bases in {} attempts, having a {}% of success, on {} seasons.'.format(record['NAME'],
                                                                                                                            lstTxt[0],
                                                                                                                            lstTxt[1],
                                                                                                                            lstTxt[2],
                                                                                                                            lstTxt[3])
                else:
                    lstTxt = []
                    record['TXT'] = ''
                   
                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)
                
    return dfAllLead
    
def allTimeLeadersP(df, dct):

    # Creates a dataframe with the top-10 all period leaders of some features
    # With additional data that will be used in the dashboard
    
    # These are the features, and as a comment the extra data, and the aggregation method (if needed)
    lstFeat = ['W',   # ['W', 'GS', 'SEASON'], ['SUM', 'SUM', 'COUNT']
              'ERA',  # ['ERA', 'W', 'SO', 'SEASON'], ['AVG', 'SUM', 'SUM', 'COUNT']
              'WHIP', # ['IP', 'H', 'BB', 'G'] ['-', '-', '-', '-']
              'SO',  # ['GS','IP','BB'], ['-', '-', '-']
              'IP',   # ['G', 'W', 'L', 'ERA'], ['-', '-', '-', '-']
              'SV']   # ['SV', 'G', 'ERA', 'SEASON'], ['SUM', 'SUM', 'AVG', 'COUNT']
           
    #The columns of the new dataframe
    COLS = ['FEAT', 'RANK', 'PLAYER_ID',  'NAME', 'W', 'ERA', 
            'WHIP', 'SO', 'IP', 'SV', 'SEASON', 'TEAM_ID', 'TXT']

    # Some useful variables to calculate some totals 
    df['minIP'] = df['SEASON'].apply(getGames)
    df['CanLead'] = (df['IP']>df['minIP'])
    lstTxt = []

    # Creates a filtered dataframe, for the BA and OPS, as for leaders is
    # needed to have more IP than Team games
    df2 = df[df['CanLead'] == True]
    rnk = 1
    record = {}
    dfAllLead = pd.DataFrame(columns = COLS)

    # Loops through the list pf features and extract the top-10 data
    for feat in lstFeat:

        record['W'] = 0.0
        record['ERA'] = 0.0
        record['WHIP'] = 0.0
        record['SO'] = 0.0
        record['IP'] = 0.0
        record['SV'] = 0.0
        
        if feat == 'W':

            dfTmp = df.sort_values(by=feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['FEAT'] = feat
                record['RANK'] = rnk
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['W'] = row['W']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['W'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['GS'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SEASON'].count())
                    record['TXT'] = '{} reached {} wins, having started {} games through {} seasons.'.format(record['NAME'],
                                                                                                             lstTxt[0],
                                                                                                             lstTxt[1],
                                                                                                             lstTxt[2])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)

        elif feat == 'ERA':

            dfTmp = df2.sort_values(by= feat, ascending=True).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():
                
                record['FEAT'] = feat
                record['RANK'] = rnk
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['ERA'] = row['ERA']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    
                    lstTxt.append(round((df[df['PLAYER_ID'] == record['PLAYER_ID']]['ER'].sum()*9)/df[df['PLAYER_ID'] == record['PLAYER_ID']]['IP'].sum(),2))
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['W'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SO'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SEASON'].count())
                    record['TXT'] = '{} had a career ERA of {}, with a {} wins and {} strikeouts in {} seasons.'.format(record['NAME'],
                                                                                                                        lstTxt[0],
                                                                                                                        lstTxt[1],
                                                                                                                        lstTxt[2],
                                                                                                                        lstTxt[3])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)
        
        elif feat == 'WHIP':

            dfTmp = df2.sort_values(by= feat, ascending=True).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():
                
                record['FEAT'] = feat                
                record['RANK'] = rnk
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['WHIP'] = row['WHIP']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['IP'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['G'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['H'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['BB'].to_list()[0])
                    record['TXT'] = '{} pitched {} innings in {} games, receiving {} hits and giving {} walks.'.format(record['NAME'],
                                                                                                                       lstTxt[0],
                                                                                                                       lstTxt[1],
                                                                                                                       lstTxt[2],
                                                                                                                       lstTxt[3])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)
                
        elif feat == 'SO':

            dfTmp = df.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():
                                    
                record['FEAT'] = feat        
                record['RANK'] = rnk
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['SO'] = row['SO']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SO'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['BF'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SEASON'].count())
                    record['TXT'] = 'The Big Unit delivered {}Ks, facing {} batters in {} seasons.'.format(lstTxt[0],
                                                                                                           lstTxt[1],
                                                                                                           lstTxt[2])
                else:
                    lstTxt = []
                    record['TXT'] = ''
                    
                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)    

        elif feat == 'IP':

            dfTmp = df.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['FEAT'] = feat
                record['RANK'] = rnk
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['IP'] = row['IP']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['G'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['W'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['L'].to_list()[0])
                    lstTxt.append(df[(df['PLAYER_ID'] == row['PLAYER_ID']) & (df['SEASON'] == row['SEASON'])]['ERA'].to_list()[0])
                    record['TXT'] = 'To be on top, {} pitched {} games, with a record of {}-{} and an ERA of {}.'.format(record['NAME'],
                                                                                                                  lstTxt[0],
                                                                                                                  lstTxt[1],
                                                                                                                  lstTxt[2],
                                                                                                                  lstTxt[3])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)

        elif feat == 'SV':

            dfTmp = df.sort_values(by= feat, ascending=False).head(10)
            rnk = 1
            for index, row in dfTmp.iterrows():

                record['FEAT'] = feat
                record['RANK'] = rnk
                record['PLAYER_ID'] = row['PLAYER_ID']
                record['NAME'] = dct[row['PLAYER_ID']]
                record['SV'] = row['SV']
                record['SEASON'] = row['SEASON']
                record['TEAM_ID'] = row['TEAM_ID']

                if rnk == 1:
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SV'].sum())
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['G'].sum())
                    lstTxt.append(round((df[df['PLAYER_ID'] == record['PLAYER_ID']]['ER'].sum()*9)/df[df['PLAYER_ID'] == record['PLAYER_ID']]['IP'].sum(),2))
                    lstTxt.append(df[df['PLAYER_ID'] == record['PLAYER_ID']]['SEASON'].count())
                    record['TXT'] = '{} saved {} games, having played in {}. Finished his {} years career with an ERA of {}.'.format(record['NAME'],
                                                                                                                                     lstTxt[0],
                                                                                                                                     lstTxt[1],
                                                                                                                                     lstTxt[3],
                                                                                                                                     lstTxt[2])
                else:
                    lstTxt = []
                    record['TXT'] = ''

                rnk+=1
                dfAllLead = dfAllLead.append(record, ignore_index=True)
                
    return dfAllLead        
    
def CS_TeamPos(dfH, dfP):

    # Process that calculate the position of the team in several hitting/pitching features 
    dfH['BA'] = round(dfH['H']/dfH['AB'],3)
    dfH['OBP'] = round((dfH['H']+dfH['BB']+dfH['HBP'])/(dfH['AB']+dfH['BB']+dfH['HBP']+dfH['SF']),3)
    dfH['SLG'] = round(((dfH['H']-(dfH['2B']+dfH['3B']+dfH['HR']))+(2*dfH['2B'])+(3*dfH['3B'])+(4*dfH['HR']))/dfH['AB'],3)
    dfH['OPS'] = round((dfH['OBP'] + dfH['SLG']),3)

    dfP['ERA'] = round((9*dfP['P_ER']/dfP['P_IP']),2)
    dfP['WHIP'] = round((dfP['P_H']+dfP['P_BB'])/dfP['P_IP'],3)

    cols = ['BA', 'OPS', 'H', 'HR', 'SB', 'TB','ERA', 'WHIP', 'P_W', 'P_H', 'P_R', 'P_HR']
    colsN = ['H-BA', 'H-OPS', 'H-H', 'H-HR', 'H-SB', 'H-TB','P-ERA', 'P-WHIP', 'P-W', 'P-HA', 'P-RA', 'P-HRA']
    colsD = ['Batting Average', 'On-base Plus Slugging', 'Hits', 'Home Runs', 'Stolen Bases', 'Total Bases',
             'Earned Runs Average', 'Walks & Hits per Inning', 'Games Won', 'Hits Against', 'Runs Against', 'Home Runs Against']
    dfH = dfH[['TEAM_ID', 'BA', 'OPS', 'H', 'HR', 'SB', 'TB']]
    dfP = dfP[['TEAM_ID', 'ERA', 'WHIP', 'P_W', 'P_H', 'P_R', 'P_HR']]
    df = pd.merge(dfH, dfP, on=['TEAM_ID'], how='inner')

    lstCols = []
    for col in cols:
        if col in ['ERA', 'WHIP', 'P_H', 'P_R', 'P_HR']:
            df = df.sort_values(by=col, ascending=False)
        else:
            df = df.sort_values(by=col, ascending=True)
        newCol = col + "_POS"
        lstCols.append(newCol)
        df[newCol] = range(1, 31)

    df = df.sort_values(by='TEAM_ID', ascending=True)
    
    dfF = pd.DataFrame(columns = ['TEAM_ID', 'FEAT', 'FEAT_DESC', 'VALUE', 'POS'])
    record = {}
    for index, row in df.iterrows():
        for i in range(0,len(cols)):
            record['TEAM_ID'] = row['TEAM_ID']
            record['FEAT'] = colsN[i]
            record['FEAT_DESC'] = colsD[i]
            record['VALUE'] = row[cols[i]]
            record['POS'] = row[lstCols[i]]
            dfF = dfF.append(record, ignore_index=True)
            record = {}
            
    return dfF