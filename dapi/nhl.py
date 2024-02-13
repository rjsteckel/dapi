import os
import requests
import time
from abc import ABC
import pandas as pd
from enum import Enum


class Game(Enum):
    PRESEASON = 1
    REGULAR = 2
    POSTSEASON = 3



class NHLClient(ABC):

    def __init__(self) -> None:
        self.api_host = 'api.nhle.com'

    def _request(self, url_path):
        url = f'https://{self.api_host}/{url_path}'
        print(url)
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception(f'Status: {response.status_code}\n\n{response.content}')

        return response.json()

    def _batch_requests(self, path, cayenneExp, size=500) -> pd.DataFrame:
        batches = []    
        index=0
        for _ in range(1000):        
            expr = f'start={index}&limit={size}&factCayenneExp={cayenneExp}'
            url = f'https://{self.api_host}/{path}?{expr}'
            print(url)
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception(f'Status: {response.status_code}\n\n{response.content}')

            df = pd.DataFrame(response.json()['data'])
            if df.shape[0] == 0:
                return pd.concat(batches)

            batches.append(df)
            index = index + size
            time.sleep(1)
        
        return pd.concat(batches)


    # ---------- Teams stats/rest/en/team ----------------
    def get_teams(self) -> pd.DataFrame:
        path = 'stats/rest/en/team'
        teams = self._request(path)
        return pd.DataFrame(teams['data'])

    def get_team(self, teamCode) -> pd.DataFrame:
        expr = 'triCode={teamCode}'
        path = f'stats/rest/en/team?cayenneExp={expr}'
        teams = self._request(path)
        return pd.DataFrame(teams['data'])

    def get_team_summary(self, seasonId, seasonType=2) -> pd.DataFrame:
        expr = f'gameTypeId={seasonType} and seasonId<={seasonId} and seasonId>={seasonId}'
        path = f'stats/rest/en/team/summary?cayenneExp={expr}'
        season = self._request(path)
        return pd.DataFrame(season['data'])

    # ---------- Players stats/rest/en/skater ----------------
    def get_players(self) -> pd.DataFrame:
        exprs = [
            #'isAggregate=false',
            #'isGame=false',
            '''sort=[{"property":"goals","direction":"DESC"},
                     {"property":"goals","direction":"DESC"}]
            ''',
            'start=0',
            'limit=100',
            'factCayenneExp=gamesPlayed>=1&cayenneExp=gameTypeId=2 and seasonId>=19961997 and seasonId<=19981999'
        ]
        expr = '&'.join(exprs)
        path = f'stats/rest/en/skater/summary?{expr}'
        players = self._request(path)
        return pd.DataFrame(players['data'])

    def get_players_by_season(self, season, gameType=2) -> pd.DataFrame:
        path = 'stats/rest/en/skater/summary'
        cayenneExp = f'gamesPlayed>=1&cayenneExp=gameTypeId={gameType} and seasonId>={season} and seasonId<={season}'

        players = self._batch_requests(path, cayenneExp)
        return players
    
    # ---------- Shifts stats/rest/en/game ----------------
    def get_shifts(self, gameId):
        expr = f'cayenneExp=gameId={gameId}'
        path = f'stats/rest/en/shiftcharts?{expr}'
        shifts = self._request(path)
        return pd.DataFrame(shifts['data'])

    # ---------- Games stats/rest/en/game ----------------
    def get_games(self, seasonId=None, gameTypeId=2) -> pd.DataFrame:
        if seasonId:
            expr = f'season={seasonId} and gameType={gameTypeId}'
            path = f'stats/rest/en/game?cayenneExp={expr}'
        else:
            path = f'stats/rest/en/game'
        
        game = self._request(path)

        df = pd.DataFrame(game['data'])        
        df['easternStartTime'] = pd.to_datetime(df.easternStartTime)
        df['gameDate'] = pd.to_datetime(df.gameDate)
        return df
    
    # ---------- Seasons ----------------
    def get_season_summary(self) -> pd.DataFrame:
        path = 'stats/rest/en/season?sort=[{"property":"id","direction":"DESC"}]'
        teams = self._request(path)
        return pd.DataFrame(teams['data'])




