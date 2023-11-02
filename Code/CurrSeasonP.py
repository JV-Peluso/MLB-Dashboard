import pandas as pd
import traceback
import warnings
from datetime import date, timedelta, datetime
import time
import os
import pathVP as VP
import PlayersFunctions as AF
import shutil
import scrapStats as SS
import ScrapeSeasonGames as SSG

warnings.filterwarnings('ignore')

def updatePlayers():

    try:
        
        # Define the path and file names that will be read/created
        playersPath = VP.hittersPath()
        pitchersPath = VP.pitcherPath()
        finalPath = VP.finalPath()
        backupPath = VP.backupPath()
        playersFile =  VP.fileName('A')
        hittersFile =  VP.fileName('B')
        pitchersFile = VP.fileName('P')
        teamsFile = VP.fileName('T1')
        seasonFile = VP.fileName('S1')
        leadersFile = VP.fileName('Y1')
        AllTimeFile = VP.fileName('F1')
        playersTidyFile = VP.fileName('PT')
        lastUpdateFile = VP.fileName('U')
        seasonPosFile = VP.fileName('SP')
        currSeason = VP.currentSeason()

        #############################################################
        ########################## HITTERS ##########################
        #############################################################

        print('\n#################################################################################')
        print('Processing FIELD players data of current season ({}). Updated today {}.'.format(currSeason, date.today().strftime('%d/%m/%Y')))
        print('#################################################################################\n')

        # If there're existing data files, read them into a dataframe (if needed),
        # rename them as backups, to be able write them with the new data
        dfPlayers = pd.read_csv(finalPath + '\\' + playersFile + '.csv')
        dfHitters = pd.read_csv(finalPath + '\\' + hittersFile + '.csv')
        lastSeason = dfHitters['SEASON'].max()

        lstFiles = [playersFile, hittersFile, teamsFile, leadersFile, AllTimeFile, seasonFile, seasonPosFile]
        for item in lstFiles:
            if os.path.exists(finalPath + '\\' + item + '.csv'):
                if os.path.exists(backupPath + '\\' + item + '.csv'):
                    os.remove(backupPath + '\\' + item + '.csv')
                shutil.copy(finalPath + '\\' + item + '.csv', 
                            backupPath + '\\' + item + '.csv')  
                os.rename(finalPath + '\\' + item + '.csv', 
                          finalPath + '\\' + item + '_.csv')
                          
        # Define variables needed for the process
        playerCreated = 0
        playerUpdated = 0
        blnCoU = False
        blnTest = False
        fileName = playersPath + '\\' + str(currSeason) + '.csv'

        # Read the csv file and clean the raw data before process it
        tmpDF = pd.read_csv(fileName)
        tmpDF = AF.cleanDF(tmpDF, False)
        tmpDF = tmpDF[tmpDF['Tm'] != 'TOT']

        # Delete record from 2023 to be able to create them again
        dfHitters = dfHitters[dfHitters['SEASON'] < currSeason]

        # Iterate and analyze each player to create it or update it in both dataframes
        for index, row in tmpDF.iterrows():

            # Process of creation or update of the Player data
            dfPlayers, blnCoU = AF.checkPlayer(dfPlayers, row, currSeason)
            if blnCoU:
                playerCreated += 1
            else:
                playerUpdated += 1
            
            # Process of creation or update of the season data
            dfHitters = AF.checkHitter(dfHitters, row, currSeason)

            if (playerCreated + playerUpdated) % 50 == 0:
                print('*- {} new players created, {} players updated and season data created.'.format(playerCreated,
                                                                                                      playerUpdated))
        # Once finished the process, export the result into a CSV file  
        print('*- {} new players created, {} players updated and season data created.'.format(playerCreated,
                                                                                              playerUpdated))

        # Create grouped totals based on the needs of the dashboard
        dctPlayers = dfPlayers[['PLAYER_ID', 'FULL_NAME']].set_index('PLAYER_ID')['FULL_NAME'].to_dict()
        dfTeams = AF.teamTotalsH(dfHitters)
        dfHST = dfTeams        
        dfSeasTot = dfTeams.groupby('SEASON').sum().reset_index()
        dfTOT = AF.TotalizeHitters(dfHitters)
        dfHitLeaders = AF.hitLeaders(dfTOT, dfTeams[dfTeams['SEASON'] == currSeason]['GAMES'].mean())
        dfAllTimeLeaders = AF.allTimeLeadersH(dfTOT[dfTOT['SEASON'] < currSeason], dctPlayers)
         
        # Save dataframes to CSV files and remove previous versions
        dfPlayers.to_csv(finalPath + '\\' + playersFile + '.csv', index = False)  
        dfHitters.to_csv(finalPath + '\\' +  hittersFile + '.csv', index = False)
        dfTeams.to_csv(finalPath + '\\' +  teamsFile + '.csv', index = False)
        dfSeasTot.to_csv(finalPath + '\\' +  seasonFile + '.csv', index = False)
        dfHitLeaders.to_csv(finalPath + '\\' +  leadersFile + '.csv', index = False)
        dfAllTimeLeaders.to_csv(finalPath + '\\' +  AllTimeFile + '.csv', index = False)

        for item in lstFiles:
            if os.path.exists(finalPath + '\\' + item + '_.csv'):
                os.remove(finalPath + '\\' + item + '_.csv')  
        
        print('\n#################################################################')
        print('Processing data of FIELD players current season ({}) completed!'.format(currSeason))
        print('#################################################################\n')
        
        ##############################################################
        ########################## PITCHERS ##########################
        ##############################################################

        # Update the path and file names that will be read/created
        teamsFile = VP.fileName('T2')
        seasonFile = VP.fileName('S2')
        leadersFile = VP.fileName('Y2')
        AllTimeFile = VP.fileName('F2')

        print('\n##########################################################')
        print('Processing PITCHER players data of current season ({}).'.format(currSeason))
        print('##########################################################\n')

        # If there're existing data files, read them into a dataframe (if needed),
        # rename them as backups, to be able write them with the new data
        dfPlayers = pd.read_csv(finalPath + '\\' + playersFile + '.csv')
        dfPitchers = pd.read_csv(finalPath + '\\' + pitchersFile + '.csv')
        lastSeason = dfPitchers['SEASON'].max()

        lstFiles = [playersFile, pitchersFile, teamsFile, leadersFile, AllTimeFile, seasonFile, playersTidyFile]
        for item in lstFiles:
            if os.path.exists(finalPath + '\\' + item + '.csv'):
                if os.path.exists(backupPath + '\\' + item + '.csv'):
                    os.remove(backupPath + '\\' + item + '.csv')
                shutil.copy(finalPath + '\\' + item + '.csv', 
                            backupPath + '\\' + item + '.csv')  
                os.rename(finalPath + '\\' + item + '.csv', 
                          finalPath + '\\' + item + '_.csv')

        # Define variables needed for the process
        playerCreated = 0
        playerUpdated = 0
        blnCoU = False
        blnTest = False
        fileName = pitchersPath + '\\' + str(currSeason) + '.csv'

        # Read the csv file and clean the raw data before process it
        tmpDF = pd.read_csv(fileName)
        tmpDF = AF.cleanDF(tmpDF, True)
        tmpDF = tmpDF[tmpDF['Tm'] != 'TOT']

        # Delete record from 2023 to be able to create them again
        dfPitchers = dfPitchers[dfPitchers['SEASON'] < currSeason]

        # Iterate and analyze each player to create it or update it in both dataframes
        for index, row in tmpDF.iterrows():

            # Process of creation or update of the pitcher data
            dfPlayers, blnCoU = AF.checkPlayer(dfPlayers, row, currSeason)
            if blnCoU:
                playerCreated += 1
            else:
                playerUpdated += 1

            dfPitchers = AF.checkPitcher(dfPitchers, row, currSeason)

            if (playerCreated + playerUpdated) % 50 == 0:
                print('*- {} new players created, {} players updated and season data created.'.format(playerCreated,
                                                                                                      playerUpdated))
        # Once finished the process, export the result into a CSV file  
        print('*- {} new players created, {} players updated and season data created.'.format(playerCreated,
                                                                                              playerUpdated))
                                                                                              
        # Create grouped totals based on the needs of the dashboard
        dctPlayers = dfPlayers[['PLAYER_ID', 'FULL_NAME']].set_index('PLAYER_ID')['FULL_NAME'].to_dict()
        dfTeams, dfSeasTot = AF.teamTotalsP(dfPitchers)
        dfTOT = AF.TotalizePitchers(dfPitchers)
        dfPitLeaders = AF.pitLeaders(dfTOT, dfTeams[dfTeams['SEASON'] == currSeason]['P_GS'].mean())   
        dfAllTimeLeaders = AF.allTimeLeadersP(dfTOT[dfTOT['SEASON'] < currSeason], dctPlayers) 
        
        # Create a dataframe with only the PLAYER_ID, TEAM_ID, TEAM_NAME for all players
        df1 = dfHitters.drop_duplicates(subset=['PLAYER_ID', 'TEAM_ID'], keep='last')[['PLAYER_ID', 'TEAM_ID']]
        df2 = dfPitchers.drop_duplicates(subset=['PLAYER_ID', 'TEAM_ID'], keep='last')[['PLAYER_ID', 'TEAM_ID']]
        dfTidy = pd.concat([df1,df2], ignore_index=True)
        dfTidy = dfTidy.drop_duplicates(subset=['PLAYER_ID', 'TEAM_ID'], keep='last')
        dfTidy['TEAM_NAME'] = dfTidy['TEAM_ID'].apply(AF.teamName)
        
        df3 = dfHitters.groupby(['PLAYER_ID', 'TEAM_ID'])['SEASON'].nunique().reset_index()
        df4 = dfPitchers.groupby(['PLAYER_ID', 'TEAM_ID'])['SEASON'].nunique().reset_index()
        df5 = pd.concat([df3,df4], ignore_index=True)
        df5 = df5.drop_duplicates(subset=['PLAYER_ID', 'TEAM_ID'], keep='last')
        dfTidy2 = pd.merge(dfTidy, df5, on=['PLAYER_ID', 'TEAM_ID'], how='inner')
        
        # Create a dataframe with the position on the league for the teams on features like BA or ERA (1-30)
        dfSeasPos = AF.CS_TeamPos(dfHST[dfHST['SEASON'] == dfHST['SEASON'].max()],
                                  dfTeams[dfTeams['SEASON'] == dfTeams['SEASON'].max()])
        
        # Export data to csv files.
        dfPlayers.to_csv(finalPath + '\\' + playersFile + '.csv', index = False)  
        dfPitchers.to_csv(finalPath + '\\' +  pitchersFile + '.csv', index = False)
        dfTeams.to_csv(finalPath + '\\' +  teamsFile + '.csv', index = False)
        dfSeasTot.to_csv(finalPath + '\\' +  seasonFile + '.csv', index = False)
        dfPitLeaders.to_csv(finalPath + '\\' +  leadersFile + '.csv', index = False)
        dfAllTimeLeaders.to_csv(finalPath + '\\' +  AllTimeFile + '.csv', index = False)
        dfTidy2.to_csv(finalPath + '\\' +  playersTidyFile + '.csv', index = False)
        dfSeasPos.to_csv(finalPath + '\\' +  seasonPosFile + '.csv', index = False)
        
        for item in lstFiles:
            if os.path.exists(finalPath + '\\' + item + '_.csv'):
                os.remove(finalPath + '\\' + item + '_.csv')  

        print('\n###################################################################')
        print('Processing data of PITCHER players current season ({}) completed!'.format(currSeason))
        print('###################################################################\n')

        # Updates the txt file with today value to know when the last update was made
        if os.path.exists(finalPath + '\\' + lastUpdateFile + '.txt'):
            os.remove(finalPath + '\\' + lastUpdateFile + '.txt')
            
        with open(finalPath + '\\' + lastUpdateFile + '.txt', 'w') as LU:
            LU.write(date.today().strftime('%d/%m/%Y'))
        
    except Exception as e:
        
        for item in lstFiles:
            os.rename(finalPath + '\\' + item + '_.csv', 
                      finalPath + '\\' + item + '.csv')
                      
        print('\nExecution interrupted by an exception!!!\n')
        print(traceback.format_exc())
        print(e)
        