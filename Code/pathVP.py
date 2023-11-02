def pitcherPath():

    return r'C:\Users\VPelu\Documents\Data Science\Baseball-Dashboard\Data\Raw Data\PitStats'
    
def finalPath():

    return r'C:\Users\VPelu\Documents\Data Science\Baseball-Dashboard\Data\Final Data\csv'
    
def hittersPath():

    return r'C:\Users\VPelu\Documents\Data Science\Baseball-Dashboard\Data\Raw Data\BatStats'
    
def backupPath():

    return r'C:\Users\VPelu\Documents\Data Science\Baseball-Dashboard\Data\Final Data\csv\Backup'
    
def currentSeason():

    return 2023
    
def fileName(tipo):

    if tipo == 'A':
        return 'AllPlayers'
    elif tipo == 'B':
        return 'BatStats'
    elif tipo == 'P':
        return 'PitStats'
    elif tipo == 'U':
        return 'LastUpdate'
    elif tipo == 'G':
        return 'Current_Games'    
    elif tipo == 'T1':
        return 'seasonTeamStats_B' 
    elif tipo == 'T2':
        return 'seasonTeamStats_P'
    elif tipo == 'Y1':
        return 'seasonLeaders_B'
    elif tipo == 'Y2':
        return 'seasonLeaders_P'
    elif tipo == 'F1':
        return 'ATLeaders_B'
    elif tipo == 'F2':
        return 'ATLeaders_P'
    elif tipo == 'S1':
        return 'SeasTot_B'
    elif tipo == 'S2':
        return 'SeasTot_P'
    elif tipo == 'PT':
        return 'playersTidy'
    elif tipo == 'AG':
        return 'AllGames'  
    elif tipo == 'SL':
        return 'SeasonLeaders'    
    elif tipo == 'SP':
        return 'SeasonPosition' 
    else:
        return ''
        
def headerHitters():

    return 'Rk,Name,Age,Tm,Lg,G,PA,AB,R,H,2B,3B,HR,RBI,SB,CS,BB,SO,BA,OBP,SLG,OPS,OPS+,TB,GDP,HBP,SH,SF,IBB,Pos Summary,Name-additional'
    
def headerPitchers():

    return 'Rk,Name,Age,Tm,Lg,W,L,W-L%,ERA,G,GS,GF,CG,SHO,SV,IP,H,R,ER,HR,BB,IBB,SO,HBP,BK,WP,BF,ERA+,FIP,WHIP,H9,HR9,BB9,SO9,SO/W,Name-additional'