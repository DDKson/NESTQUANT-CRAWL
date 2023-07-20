import os
import requests
from requests.exceptions import HTTPError

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

class Base:
    NESTQUANT_API_ENDPOINT = "https://api-dev.nestquant.com/"

    def __init__(self, api_key):
        self._api_key = api_key

    def _get(self, url: str):
        """ Interact with GET request """
        res = requests.get(url)
        if res.status_code != 200:
            raise HTTPError(f"HTTP error status code - {res.status_code}: {res.text}")
        return res

    def _delete(self, url: str):
        """ Interact with GET request """
        res = requests.delete(url)
        if res.status_code != 200:
            raise HTTPError(f"HTTP error status code - {res.status_code}: {res.text}")
        return res

    def _post(self, url: str, data: dict):
        """ Interact with GET request """
        res = requests.post(url, data=data)
        if res.status_code != 200:
            raise HTTPError(f"HTTP error status code - {res.status_code}: {res.text}")
        return res



import os
import requests

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())

import zipfile, io

class Crawler(Base):
    def __init__(self, api_key):
        super().__init__(api_key=api_key)
        self._get_download_link_url = Crawler.NESTQUANT_API_ENDPOINT + 'data/api/download_link?category=%s&symbol=%s&api_key=' + self._api_key
        self._get_lastest_data_url = Crawler.NESTQUANT_API_ENDPOINT + 'data/api/lastest?category=%s&symbol=%s&api_key=' + self._api_key

    def _check_location(self, location: str):
        """ Verify the presence of a folder and create it if it is absent. """
        try:
            os.makedirs(location)
        except FileExistsError:
            print(f"Folder '{location}' already exists")

    def _get_data_response(
        self,
        category: str,
        symbol: str,
        get_historical_data: bool
    ) -> requests.Response:
        """
            Retrieve the data response by generating the URL for the query procedure.

            Parameters
            -----------
                category: str,
                    data category
                symbol: str,
                    data symbol, be careful that the symbol is case sensitive
                get_historical_data: bool
                    if the value is True, retrieve the historical data in a compressed zip format
                    if the value is False, return the 10 most recent data entries.

            Returns
            ----------
                a Response object
        """
        if get_historical_data:
            download_link = self._get_download_link_url % (category, symbol)
            url = self._get(download_link).text[1:-1]
        else:
            url = self._get_lastest_data_url % (category, symbol)
        return self._get(url)

    def download_historical_data(
        self,
        category: str,
        symbol: str,
        location: str
    ):
        """
            Download and extract the historical data, then save it to the specified 'location'.

            Parameters
            -----------
                category: str,
                    data category
                symbol: str,
                    data symbol, be careful that the symbol is case sensitive
                location: str
                    the destination where the data should be saved.
        """
        data_response = self._get_data_response(category, symbol, get_historical_data=True)
        location = os.path.join(location, data_response.headers['content-disposition'].split(';')[1].split('=')[1].split('.')[0])
        self._check_location(location)
        z = zipfile.ZipFile(io.BytesIO(data_response.content))
        z.extractall(location)

    def get_lastest_data(
        self,
        category: str,
        symbol: str
    ) -> dict:
        """
            Retrieve the lastest data in JSON format.

            Parameters
            -----------
                category: str,
                    data category
                symbol: str,
                    data symbol, be careful that the symbol is case sensitive

            Returns
            -----------
                data in dict format
        """
        data_response = self._get_data_response(category, symbol, get_historical_data=False)
        return data_response.json()

from typing import List

from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())


class Submission(Base):
    def __init__(self, api_key):
        super().__init__(api_key=api_key)
        self._cur_round = self._get(Submission.NESTQUANT_API_ENDPOINT + 'competition/nestquant_tournament_2023/current-round').json()['Current round']
        self._records_url = Submission.NESTQUANT_API_ENDPOINT + f'competition/nestquant_tournament_2023/records/api?api_key=' + self._api_key
        self._result_url = Submission.NESTQUANT_API_ENDPOINT + f'competition/nestquant_tournament_2023/result/api?&api_key=' + self._api_key
        self._submit_url = Submission.NESTQUANT_API_ENDPOINT + f'competition/nestquant_tournament_2023/submit/api?api_key=' + self._api_key
    
    def __convert_dict_to_url_str(
        self,
        d: List[dict]
    ) -> str:
        """
            Convert list of dictionary to string - this is a helper function for submit method

            Parameters
            -----------
                d: List[dict],
                    data in list of dictionary format

            Returns
            ----------
                data in string format
        """
        return str(d).replace("'", '"')

    def submit(
        self,
        is_backtest: bool,
        data: List[dict],
        symbol: str=None
    ) -> int:
        """
            Submit model's output

            Parameters
            -----------
                is_backtest: bool,
                    whether we choose to submit for backtesting or not
                data: List[dict],
                    data in list of dictionary format
                symbol: str, default None,
                    provided symbol, exclusively utilized for the purpose of backtesting

            Returns
            ----------
                submission_time: int
                    recorded submission time
        """
        if is_backtest:
            res = self._post(self._submit_url + f'&submission_type=backtest&symbol={symbol}', data=self.__convert_dict_to_url_str(data))
        else:
            res = self._post(self._submit_url + f'&submission_type={self._cur_round}', data=self.__convert_dict_to_url_str(data))
        submission_time = res.json()['Submisstion time']
        return submission_time

    def get_submission_time(
        self,
        is_backtest: bool, 
        symbol: str=None
    ) -> dict:
        """
            Get all the recorded submission time

            Parameters
            -----------
                is_backtest: bool,
                    whether we choose to submit for backtesting or not
                symbol: str, default None,
                    provided symbol, exclusively utilized for the purpose of backtesting

            Returns
            ----------
                res: dict
                    all recorded submission time
        """
        if is_backtest:
            res = self._get(self._records_url + f'&submission_type=backtest&symbol={symbol}')
        else:
            res = self._get(self._records_url + f'&submission_type={self._cur_round}')
        return res.json()

    def get_result(
        self,
        is_backtest: bool, 
        submission_time: int,
        symbol: str = None
    ) -> dict:
        """
            Get model's performance

            Parameters
            -----------
                is_backtest: bool,
                    whether we choose to submit for backtesting or not
                submission_time: int,
                    recorded submission time
                symbol: str, default None,
                    provided symbol, exclusively utilized for the purpose of backtesting

            Returns
            ----------
                res: dict
                    all scores of the model
        """
        if is_backtest:
            res = self._get(self._result_url + f'&submission_type=backtest&symbol={symbol}&submission_time={submission_time}')
        else:
            res = self._get(self._result_url + f'&submission_type={self._cur_round}&submission_time={submission_time}')
        return res.json()
    
    def delete_record(
        self,
        is_backtest: bool, 
        submission_time: int,
        symbol: str=None
    ) -> str:
        """
            Delete model's record

            Parameters
            -----------
                is_backtest: bool,
                    whether we choose to submit for backtesting or not
                submission_time: int,
                    recorded submission time
                symbol: str, default None,
                    provided symbol, exclusively utilized for the purpose of backtesting

            Returns
            ----------
                If successful, return "Delete record successfully"
        """
        if is_backtest:
            res = self._delete(self._records_url + f'&submission_type=backtest&symbol={symbol}&submission_time={submission_time}')
        else:
            res = self._delete(self._records_url + f'&submission_type={self._cur_round}&submission_time={submission_time}')
        return res.text
