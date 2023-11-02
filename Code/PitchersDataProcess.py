import pandas as pd
import warnings
from datetime import date, timedelta, datetime
import time
import os
import pathVP as VP
import PitchersFunctions as PF

warnings.filterwarnings('ignore')

# To record the process total time, we get the start time
startTime = datetime.now()

# Define the path and file names that will be read/created
pitchersPath = VP.pitcherPath()
finalPath = VP.finalPath()
playersFile = VP.fileName('A')
pitchersFile = VP.fileName('P')
maxSeason = VP.currentSeason()
currSeason = 1983

# Load/Define the dataframes where the players data will be inserted
colPlayers = ['PLAYER_ID', 'FULL_NAME', 'BDATE', 'COUNTRY_ID',
              'STATE_ID', 'TEAM_ID', 'L_R_S', 'POS', 'PHOTO', 'ACTIVE'] 
colPitchers = ['PLAYER_ID', 'SEASON', 'TEAM_ID', 'G', 'GS', 'CG', 
               'W', 'L', 'ERA', 'SHO', 'SV', 'IP', 'H', 'R', 'ER', 
               'HR', 'BB', 'IBB', 'SO', 'HBP', 'BK', 'WP', 'BF', 
               'WHIP', 'H9', 'HR9', 'BB9', 'SO9']

# Process the players data files from 1983 until 2022
while currSeason < maxSeason: 

    print('\n###########################################################')
    print('Processing of {} season started at {} of {}'.format(currSeason, datetime.now().strftime('%H:%M:%S'),
                                                               date.today().strftime('%d/%m/%Y')))
    print('###########################################################\n')

    # If there're existing data files, read them into a dataframe, rename them as backups, 
    # for then be able write them with the new data
    if os.path.exists(finalPath + '\\' + playersFile + '.csv'):
        dfPlayers = pd.read_csv(finalPath + '\\' + playersFile + '.csv')
        if os.path.exists(finalPath + '\\' + playersFile + 'Backup.csv'):
            os.remove(finalPath + '\\' + playersFile + 'Backup.csv')
        os.rename(finalPath + '\\' + playersFile + '.csv', 
                  finalPath + '\\' + playersFile + 'Backup.csv')
    else:
        dfPlayers = pd.DataFrame(columns = colPlayers)

    if os.path.exists(finalPath + '\\' + pitchersFile + '.csv'):
        dfPitchers = pd.read_csv(finalPath + '\\' + pitchersFile + '.csv')
        if os.path.exists(finalPath + '\\' + pitchersFile + 'Backup.csv'):
            os.remove(finalPath + '\\' + pitchersFile + 'Backup.csv')
        os.rename(finalPath + '\\' + pitchersFile + '.csv', 
                  finalPath + '\\' + pitchersFile + 'Backup.csv')
        lastSeason = dfPitchers['SEASON'].max()
    else:
        dfPitchers = pd.DataFrame(columns = colPitchers)

    # Define variables needed for the process
    playerCreated = 0
    playerUpdated = 0
    blnCoU = False
    blnTest = False
    fileName = pitchersPath + '\\' + str(currSeason) + '.csv'

    # Read the csv file and clean the raw data before process it
    tmpDF = pd.read_csv(fileName)
    tmpDF = PF.cleanDF(tmpDF)
    tmpDF = tmpDF[tmpDF['Tm'] != 'TOT']

    # Iterate and analyze each player to create it or update it in both dataframes
    for index, row in tmpDF.iterrows():

        # Process of creation or update of the pitcher data
        dfPlayers, blnCoU = PF.checkPlayer(dfPlayers, row, currSeason)
        if blnCoU:
            playerCreated += 1
        else:
            playerUpdated += 1

        # Process of creation or update of the field player seasonal data.
        dfPitchers = PF.checkPitcher(dfPitchers, row, currSeason)

        if (playerCreated + playerUpdated) % 50 == 0:
            print('*- {} new players created, {} players updated and season data created.'.format(playerCreated,
                                                                                                  playerUpdated))

    # Once finished the process, export the result into a CSV file  
    print('*- {} new players created, {} players updated and season data created.'.format(playerCreated,
                                                                                          playerUpdated))
    
    dfPlayers.to_csv(finalPath + '\\' + playersFile + '.csv', index = False)  
    dfPitchers.to_csv(finalPath + '\\' +  pitchersFile + '.csv', index = False)
    
    print('\n#########################################################')
    print('Processing of {} season ended at {} of {}'.format(currSeason, datetime.now().strftime('%H:%M:%S'),
                                                             date.today().strftime('%d/%m/%Y')))
    print('#########################################################\n')
    
    # Move up the season
    currSeason += 1

# Once finished all 40 season long, calculate the time spent and print it
endTime = datetime.now()
totalTime = endTime - startTime

m, s = divmod(int(totalTime.total_seconds()), 60)
h, m = divmod(m, 60)
formatted_time = f"{h:02d}:{m:02d}:{s:02d}"
        
print('\n\n ### Total duration of the process --> {} ###'.format(formatted_time))