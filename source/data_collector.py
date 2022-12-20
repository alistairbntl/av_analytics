"""
The module is designed to support api requests to differen 
economic and financial data sources.
"""

import pandas as pd
import requests
import json
import data_config # this module has api keys
from fredapi import Fred
from functools import reduce

class FRED_DataCollector():
    """
    Class to support downloading from FRED API.
    """
    
    def __init__(self):
        self.fred = FRED(api_key=data_config.fred_key)

class BLS_DataCollector():
    """
    The Bureau of Labor Statistics is an import source of economic data.  Two
    of the more important data series released by the BLS are the core price
    index and the monthly jobs report.  

    This class provides support for downloading and organizing BLS data-requests.

    A BLS API key is necessary to make requests from this class and can be obtained
    from the BLS website.

    An example use case would take the form:

    data_series_dict = {'CUSR0000SA0': 'cpi_all_item', 
                        'CUSR0000SA0L1E': 'cpi_all_item_less_food_energy',
                        'CUSR0000SETA02': 'cpi_used_cars_and_trucks'}

    bls_collector = BLS_DataCollector()
    df_ = bls_collector(list(data_series_dict.keys()), 2016, 2020)

    NOTE: this class has been designed and tested for monthly data-series.  Other
    data frequencies should work as well, but are untested and remain feature for 
    future versions.
    """
    API_URL = 'https://api.bls.gov/publicAPI/v2/timeseries/data/'
    KEY = f'?registrationkey={data_config.bls_key}'
    HEADERS = {'Content-type': 'application/json'}
    COL_DTYPE_MAPS = {'value': float}
    JOIN_COLS = ['year', 'period', 'periodName']

    def requested_data(self,
                       data_series: list,
                       start_yr: int,
                       end_yr: int) -> str:
        """
        Returns json.dumps with the dataseries to be requested
        """
        return json.dumps({"seriesid": data_series,
                           "startyear": str(start_yr),
                           "endyear": str(end_yr)})

    def process_raw_request(self,
                            raw_request : requests.models.Response) -> list:
        """
        This method takes the output of the bls get request
        and transforms the data into a dictionary of data-frames
        where the keys are the individual series requested.

        Columns dtypes are also recast according to the mapping
        in the COL_DTYPE_MAPS dictionary.
        """
        data_dict = raw_request.json()['Results']['series']
    
        df_lst = []
        for series in data_dict:
            df_lst.append(self.pre_process_df(pd.DataFrame(series['data']),
                                              seriesID = series['seriesID']))
        return df_lst

    def pre_process_df(self,
                       df_: pd.DataFrame,
                       seriesID: str,
                       log=print) -> pd.DataFrame:
        """
        This method handles any necessary processing involved in setting up
        the dataframe immediately after loading it from the raw data..

        Currently, there are two processing steps: (i) casting string columns
        into their correct type and (ii) appending series ids to non-join
        columns.
        """
        for col in df_.columns:
            # type cast select columns
            if col in self.COL_DTYPE_MAPS:
                df_[col] = df_[col].astype(self.COL_DTYPE_MAPS[col])
            # append series ids to non-join columns
            if col not in self.JOIN_COLS:
                df_ = df_.rename(columns={col : col + '_' + seriesID})
        return df_

    def create_merged_df(self, df_lst: list) -> pd.DataFrame:
        """
        This method creates, merges and modifies the dataframes in df_lst into
        a final state for performing data analysis.
        """
        # merge dfs
        return_df = reduce(lambda x, y: pd.merge(x, y, on=self.JOIN_COLS), df_lst)
        # create multiindex columns with data and seriesID values
        return_df = self.create_index(return_df)
        midx = pd.MultiIndex.from_tuples([col.split('_') for col in return_df.columns],
                                         names=['data', 'seriesID'])
        return_df.columns = midx
        return return_df

    def create_index(self, df_: pd.DataFrame) -> pd.DataFrame:
        """
        Create a new data-based index column and remove JOIN_COLS from df_.
        """
        df_['dates'] = self._get_date_idx(df_)
        df_ = df_.set_index('dates')
        df_ = df_.drop(columns = BLS_DataCollector.JOIN_COLS)
        return df_
    
    def _get_date_idx(self, df_):
        # create a string version of the dates
        dates = df_.apply(lambda row: self._process_row(row), axis=1)
        # return the pandas datetime index
        return pd.to_datetime(dates)

    def _process_row(self, row):
        """processes columns to create date formatted string """
        try:
            return f"""{row['period'][-2:]}/01/{row['year']}"""
        except KeyError:
            """Expected columns are missing from df_ when converting join columns to dateIndex"""
    
    def post_request(self,
                     data_series: list,
                     start_yr: int,
                     end_yr: int) -> requests.models.Response:
        """
        submits the bls post request for the requested data
        """
        request = requests.post(self.API_URL + self.KEY,
                                data=self.requested_data(data_series,
                                                         start_yr,
                                                         end_yr),
                             headers = self.HEADERS)
        assert request.status_code == 200, f"post request failed with status_code {request.status_code}"
        return request

    def get_df(self,
               data_series_dict: dict,
               start_yr: int,
               end_yr: int,
               log=print) -> pd.DataFrame:
        """
        downloads and sets up a pandas dataframe with data downloaded from
        BLS api.
        
        The downloaded series will be the keys in the data_series_dict
        e.g. {'CUSR0000SA0': 'cpi_all_item'} with data from start_yr -> end_yr.
        """
        log("sending post request...")
        raw_data = self.post_request(data_series_dict, start_yr, end_yr)
        log("processing data from post request...")
        df_list = self.process_raw_request(raw_data)
        log("returning data...")
        return self.create_merged_df(df_list)
