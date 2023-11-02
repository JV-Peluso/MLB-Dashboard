import pandas as pd
import warnings
from datetime import date, timedelta, datetime
import time
import os
import pathVP as VP
import HittersFunctions as HF

warnings.filterwarnings('ignore')

# To record the process total time, we get the start time
startTime = datetime.now()

# Define the path and file names that will be read/created
playersPath = VP.hittersPath()
finalPath = VP.finalPath()
playersFile =  VP.fileName('A')
hittersFile =  VP.fileName('B')
maxSeason = VP.currentSeason()
currSeason = 1983

# Load/Define the dataframes where the players data will be inserted
colPlayers = ['PLAYER_ID', 'FULL_NAME', 'BDATE', 'COUNTRY_ID',
              'STATE_ID', 'TEAM_ID', 'L_R_S', 'POS', 'PHOTO', 'ACTIVE'] 
colHitters = ['PLAYER_ID', 'SEASON', 'TEAM_ID', 'G', 'AB', 'R', 
              'H', '2B', '3B', 'HR', 'RBI', 'SB', 'CS', 'BB', 
              'SO', 'AVG', 'OBP', 'SLG', 'OPS', 'TB', 'GIDP', 'HBP', 'SF', 'IBB']

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

    if os.path.exists(finalPath + '\\' + hittersFile + '.csv'):
        dfHitters = pd.read_csv(finalPath + '\\' + hittersFile + '.csv')
        if os.path.exists(finalPath + '\\' + hittersFile + 'Backup.csv'):
            os.remove(finalPath + '\\' + hittersFile + 'Backup.csv')
        os.rename(finalPath + '\\' + hittersFile + '.csv', 
                  finalPath + '\\' + hittersFile + 'Backup.csv')
        lastSeason = dfHitters['SEASON'].max()
    else:
        dfHitters = pd.DataFrame(columns = colHitters)

    # Define variables needed for the process
    playerCreated = 0
    playerUpdated = 0
    blnCoU = False
    blnTest = False
    fileName = playersPath + '\\' + str(currSeason) + '.csv'

    # Read the csv file and clean the raw data before process it
    tmpDF = pd.read_csv(fileName)
    tmpDF = HF.cleanDF(tmpDF)
    tmpDF = tmpDF[tmpDF['Tm'] != 'TOT']

    # Iterate and analyze each player to create it or update it in both dataframes
    for index, row in tmpDF.iterrows():

        # Process of creation or update of the Player data
        dfPlayers, blnCoU = HF.checkPlayer(dfPlayers, row, currSeason)
        if blnCoU:
            playerCreated += 1
        else:
            playerUpdated += 1

        # Process of creation or update of the field player seasonal data.
        dfHitters = HF.checkHitter(dfHitters, row, currSeason)

        if (playerCreated + playerUpdated) % 50 == 0:
            print('*- {} new players created, {} players updated and season data created.'.format(playerCreated,
                                                                                                  playerUpdated))

    # Once finished the process, export the result into a CSV file  
    dfPlayers.to_csv(finalPath + '\\' + playersFile + '.csv', index = False)  
    dfHitters.to_csv(finalPath + '\\' +  hittersFile + '.csv', index = False)
    
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