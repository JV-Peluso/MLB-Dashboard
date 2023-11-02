import pandas as pd
import warnings
warnings.filterwarnings('ignore')

# Read the csv file where there're the games with the teams as columns and the seasons as index
DF = pd.read_csv(r"C:\Users\VPelu\Documents\Data Science\Baseball-Dashboard\Data\Final Data\csv\AllGames.csv", sep = ";")
teams = DF.columns.tolist()[2:]
DF = DF.fillna(0)

# Create the melted dataframe
DF_GAMES = pd.DataFrame(columns=['SEASON', 'TEAM_ID', 'W', 'L', 'W-L'])
games = 0
season = 0

# Iterate and create the new dataframe
for index, rows in DF.iterrows():
    season = int(rows['Year'])
    games = rows['G']
    for team in teams:
        if rows[team] > 0:
            wins = rows[team]
            loses = games - wins
            wl = round((wins/games),3)
            record = {'SEASON': season, 'TEAM_ID': team, 'W': wins, 'L': loses, 'W-L' : wl}
            DF_GAMES = DF_GAMES.append(record, ignore_index = True)

# Export it into a csv file to finish the process
DF_GAMES.to_csv(r'C:\Users\VPelu\Documents\Data Science\Baseball-Dashboard\Data\Final Data\csv\Season_Games.csv', index = False)
print('Games of 1983-2022 seasons formated and downloaded in the csv file \'Season_Games\'!')