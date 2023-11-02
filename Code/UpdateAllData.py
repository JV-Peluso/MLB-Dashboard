import warnings
from datetime import date, timedelta, datetime
import scrapStats as SS
import ScrapeSeasonGames as SSG
import CurrSeasonP as CSP

# To record the process total time, we get the start time
startTime = datetime.now()

# Calls the process that read the games and the csv of the players for the current season
# and updates the records

SSG.scrapGames()
SSG.scrapLeaders()
SS.scrapData()
CSP.updatePlayers()

# Calculates the total time spent in the process and print it
totalTime = datetime.now() - startTime
m, s = divmod(int(totalTime.total_seconds()), 60)
h, m = divmod(m, 60)
formatted_time = f"{h:02d}:{m:02d}:{s:02d}"
        
print('\n ### Total duration of the process --> {} ###'.format(formatted_time))