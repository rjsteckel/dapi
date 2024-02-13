import pandas as pd
import os
import json
import requests
import zipfile
import io
import glob
import tempfile
import logging


logger = logging.getLogger(__name__)



class DataLoader:

    def __init__(self, params: dict) -> None:
        self.baseurl = 'https://www.waterqualitydata.us'
        self.queryoptions = 'mimeType=csv&zip=yes'
        self.dataurl = f'{self.baseurl}/data/Result/search?{self.queryoptions}'
        self.stationurl = f'{self.baseurl}/data/Station/search?{self.queryoptions}'
        self.headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/zip'
        }
        # hardcode for easy testing
        self.params = {
            "statecode": ["US:48"], 
            "countycode": ["US:48:439"],
            "providers": ["NWIS","STEWARDS","STORET"],
            "startDateLo":"01-01-2018",
            "startDateHi":"01-01-2024"
        }

    def _ziptodf(self, response) -> pd.DataFrame:

        if response.status_code != 200:
            raise Exception('Error getting data')

        with tempfile.TemporaryDirectory() as tempdir:
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                z.extractall(tempdir)

                datafiles = glob.glob(os.path.join(tempdir, '*.csv'))
                if len(datafiles) == 1:
                    df = pd.read_csv(datafiles[0])
                    return df
                else:
                    raise Exception(f'Data not found.')

    def water_quality_df(self) -> pd.DataFrame:
        params = self.params.copy()
        params['dataProfile'] = "resultPhysChem"
        querydata = json.dumps(params)

        response = requests.post(self.dataurl, 
                      headers=self.headers, 
                      data=querydata)

        return self._ziptodf(response)

    def station_df(self) -> pd.DataFrame:
        querydata = json.dumps(self.params)

        response = requests.post(self.stationurl, 
                      headers=self.headers, 
                      data=querydata)

        return self._ziptodf(response)


