####################################
# Author: Jon Willinger
# Date: 2024-11-21
# Notes: 
# bloomberg_currencies_url = "https://www.bloomberg.com/markets/api/comparison/data?securities=EURUSD%3ACUR,USDCAD%3ACUR&securityType=CURRENCY&locale=en"
# bloomberg_energy_url = "https://www.bloomberg.com/markets/api/comparison/data?securities=CL1%3ACOM,CO1%3ACOM,NG1%3ACOM&securityType=COMMODITY&locale=en"
####################################

import os, csv, re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import datetime
import pandas as pd

# CME Datamine does not support OAuth.
FULL_COLUMN_COUNT = 11

_26_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES = "26 Crude Oil Last Day Financial Futures"                    # WTI
B0_MONT_BELVIEU_LDH_PROPANE_OPIS_FUTURES = "B0 Mont Belvieu LDH Propane (OPIS) Futures"                 # Propane
BZ_BRENT_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES = "BZ Brent Crude Oil Last Day Financial Futures"         # Brent
C0_MONT_BELVIEU_ETHANE_OPIS_FUTURES = "C0 Mont Belvieu Ethane (OPIS) Futures"                           # Ethane
C1_CANADIAN_DOLLAR_US_DOLLAR_CAD_USD_FUTURES = "C1 Canadian Dollar/U.S. Dollar (CAD/USD) Futures"       # US to CA
EC_EURO_US_DOLLAR_EUR_USD_FUTURES = "EC Euro/U.S. Dollar (EUR/USD) Futures"
NG_HENRY_HUB_NATURAL_GAS_FUTURES = "NG Henry Hub Natural Gas Futures"


class CMEDatamineAPI:

    def __init__(self):
        self.api_id = os.environ["CME_API_ID"]
        self.api_pw = os.environ["CME_API_PW"]
        self.base_endpoint = "https://datamine.cmegroup.com/cme/api/v1/download"

    def get_dfs_from_fid_dict(self, fid_dict):
        '''
            Calls download and processes
            the files into dfs for upsert.
            Deletes the file as cleanup.
        '''
        def _process_file_into_df(file, data_set):
            
            def __search_string_in_file_get_header_footer(file_path, search_header_str, search_footer_str):
                """Searches for a string in a text file and prints matching lines."""
                with open(file_path, 'r') as file:
                    for row_number, line in enumerate(file, 1):
                        if search_header_str in line:
                            print(line.strip())
                            header_row_number = row_number

                            for r_n, line2 in enumerate(file, row_number):
                                if search_footer_str in line2:
                                    print(line2.strip())
                                    footer_row_number = r_n
                                    break
                            break
                    file.close()
                return header_row_number, footer_row_number
            
            def __extract_subset_file_to_df(file_path, data_set, header_row_number, footer_row_number):
                '''Extract specific subset; simplifies dataframe creation'''

                def ___get_trimmed_line_list(data_set, line):
                    '''
                        This is specific to the dataset.
                        Each new dataset needs its own
                        logic.

                        CSV reader delimiter requires 1 character
                        string, special implementation logic necessary.
                    '''

                    def ____define_null_column_handlers_list(data_set):
                        ''' Parse logic params: 
                            {n1:[(i11, k11), (i12, k12), ...], n2:[(i21, k21), (i22, k22), ...], ...}
                            n = Number of Columns with Data.
                            i_ = Column iterated upon from 0 to len -1.
                            k_ = number of '' columns to add to list
                        '''

                        # Split for customization.
                        if data_set == _26_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == B0_MONT_BELVIEU_LDH_PROPANE_OPIS_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == BZ_BRENT_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == C0_MONT_BELVIEU_ETHANE_OPIS_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == C1_CANADIAN_DOLLAR_US_DOLLAR_CAD_USD_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == EC_EURO_US_DOLLAR_EUR_USD_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        elif data_set == NG_HENRY_HUB_NATURAL_GAS_FUTURES: list_handler = {10:[(7,1)], 9:[(6, 1), (7, 1)], 8:[(6, 1), (7, 2)]}
                        return list_handler
                    
                    # Write data, csv style:
                    list_h = ____define_null_column_handlers_list(data_set) # dataset level
                    line_list = line.split()
                    list_count = len(line_list)

                    # trim with new_line_list:
                    if list_count < FULL_COLUMN_COUNT:
                        new_line_list = []
                        for i_field, field in enumerate(line_list, 0):
                            if list_count not in list_h.keys(): 
                                new_line_list.append(field)
                            else:
                                new_line_list.append(field)
                                for i_col, ncol in list_h[list_count]:
                                    if i_col == i_field:
                                        # doesn't pass on wrong field--good.
                                        for _ in range(ncol): 
                                            new_line_list.append('')
                        line_list = new_line_list
                    else:
                        pass
                    trimmed_line = ",".join(line_list)
                    trimmed_line = f"\n{trimmed_line}"
                    return trimmed_line
                
                outfile = f"Output_{file_path}"
                # Write header, csv style:
                with open(file_path, 'r') as infile, open(outfile, 'w') as outfile:
                    columns = ["MTH_STRIKE", "DAILY_OPEN", "DAILY_HIGH", "DAILY_LOW", "DAILY_LAST", 
                               "SETT", "PNT_CHGE", "ACT_EST_VOL", "PREV_DAY_SETT", "PREV_DAY_VOL", "PREV_DAY_INT"]
                    for col in columns[:-1]: 
                        col_ = f"{col},"
                        outfile.write(col_)
                    outfile.write(columns[-1])

                    # Write data, csv style:
                    for i_row, line in enumerate(infile):
                        if i_row >= header_row_number and i_row < footer_row_number:
                            print(line)
                            line_list = line.split()
                            trimmed_line = ",".join(line_list)
                            trimmed_line = f"\n{trimmed_line}"

                            trimmed_line = ___get_trimmed_line_list(data_set, line) # trim it.
                            outfile.write(trimmed_line)
                    infile.close()
                    outfile.close()
                outfile = f"Output_{file_path}" # reinitialize.
                df = pd.read_csv(filepath_or_buffer=outfile, delimiter=',')
                os.remove(f"Output_{file_path}") # remove extraction file.

                return df
            
            def __clean_inconsistent_columns(df):
                ''' 
                Only Columns 0 to 6 are useable and 
                necessary. Others require more
                extensive parsing.
                '''
                df_ = df.iloc[:, :6]
                return df_

            # Search file
            header, footer = __search_string_in_file_get_header_footer(file_path=file, search_header_str=data_set, search_footer_str="TOTAL")
            df = __extract_subset_file_to_df(file_path=file, data_set=data_set, header_row_number=header, footer_row_number=footer )
            df = __clean_inconsistent_columns(df)
            return df
        
        # Entry:
        # ``````
        dict_dfs = {}
        for fid, data in fid_dict.items():
            file_name = self.download_and_get_file(fid=fid)

            for data_set in data:
                df = _process_file_into_df(file_name, data_set)
                dict_dfs[data_set] = df

            os.remove(file_name)
        return dict_dfs
    
    def download_and_get_file(self, fid):
        '''
            Receives fid and looks back to previous
            days, accounting for weekends and 
            holidays and downloads most recent
            file.
        '''

        def _execute_call(fid_endpoint):
            # Define the retry strategy.
            retry_strategy = Retry(
                total=4,  # Maximum number of retries.
                status_forcelist=[429, 500, 502, 503, 504],  # the HTTP status codes to retry on.
            )

            adapter = HTTPAdapter(max_retries=retry_strategy)
            
            # Create a new session object.
            session = requests.Session()
            session.mount("https://", adapter)

            # Make a request using the session object.
            url = f"{self.base_endpoint}?fid={fid_endpoint}"
            response = session.get(url, auth=(self.api_id, self.api_pw))
            return response, url
        
        def _get_last_business_day(today_datetime, n_past_days):
            # Corrects for Holidays.
            check_datetime = today_datetime - datetime.timedelta(days=n_past_days)
            
            if check_datetime.weekday() == 6:
                n_past_days = n_past_days+2
                last_bus_datetime = today_datetime - datetime.timedelta(days=(n_past_days+2))
            elif check_datetime.weekday() == 5:
                n_past_days = n_past_days+1
                last_bus_datetime = today_datetime - datetime.timedelta(days=(n_past_days+1))
            else:
                last_bus_datetime = datetime.datetime.now() - datetime.timedelta(days=n_past_days)
            return n_past_days, last_bus_datetime
        
        # Entry: 7-day lookback for Holidays.
        today_datetime = datetime.datetime.now()

        for i in range(7):
            i, last_bus_datetime = _get_last_business_day(today_datetime=today_datetime, n_past_days=i)
            # Force here:
            fid_date = last_bus_datetime.strftime("%Y%m%d")
            fid_endpoint = f"{fid_date}-{fid}"
            response, url = _execute_call(fid_endpoint)

            if response.status_code == 200:
                # File downloaded successfully.
                fid_datetime = today_datetime.strftime("%Y-%m-%d_%H-%M-%S")
                file_name = "_".join((fid.replace(" ", ""), f"{fid_datetime}.txt"))
                with open(file_name, 'wb') as f:
                    f.write(response.content)
                    f.close()
                print(f"File downloaded successfully from {url}.")
                break

            else:
                print("Error:", response.status_code)
                file_name = ""

        return file_name

    def trim_top_month_on_dfs(self, dict_dfs):
        dict_dfs_ = dict()
        for k, df in dict_dfs.items():
            dict_dfs_[k] = df.head(1)
        return dict_dfs_
    
    def concat_dfs_into_sum_df(self, dict_dfs):
        for n, data in enumerate(dict_dfs.items()):
            data[1]["DATA_SET"] = data[0]
            if n == 0: df_sum = data[1]
            else: df_sum = pd.concat([df_sum, data[1]], ignore_index=True)
        return df_sum

    def clean_df(self, df, columns_to_keep):
        '''
            Remove non-numeric columns.
        '''
        
        df = df[columns_to_keep]
        df = df.astype(str)
        for col in df.columns:
            if col not in ["MTH_STRIKE", "DATA_SET"]:
                # df[col] = "0.771A" # test -- good
                df[col] = df[col].str.replace(r'^\.', '0.', regex=True)
                df[col] = df[col].str.extract(r'(\d+\.?\d*)', expand=False).astype(float)
        return df
    
    def transform_df_for_azure_upsert(self, df):

        def _set_short_names(data_set):
            def __take_inverse(num: float) -> float:
                if num != 0.0:
                    return 1/num;
                else: return num
            df = pd.DataFrame({"Date":[datetime.datetime.now().strftime("%Y-%m-%d")]});
            for n, data in enumerate(data_set):
                if data[1] == "26 Crude Oil Last Day Financial Futures":
                    df["WTI Crude Oil"] = [data[3]] # Settlement
                elif data[1] == "B0 Mont Belvieu LDH Propane (OPIS) Futures":
                    df["Propane"] = [data[3]] # Settlement
                elif data[1]  == "BZ Brent Crude Oil Last Day Financial Futures":
                    df["Brent Crude Oil"] = [data[3]] # Settlement
                elif data[1]  == "C0 Mont Belvieu Ethane (OPIS) Futures":
                    df["Ethane"] = [data[3]] # Settlement
                elif data[1]  == "C1 Canadian Dollar/U.S. Dollar (CAD/USD) Futures":
                    df["US to CA$"] = [__take_inverse(float(data[4]))] # Last
                elif data[1]  == "EC Euro/U.S. Dollar (EUR/USD) Futures":
                    df["Euro to $US"] = [data[4]] # Last
                elif data[1]  == "NG Henry Hub Natural Gas Futures":
                    df["Nat. Gas"] = [data[3]] # Settlement
            return df

        records = df.to_records();
        df = _set_short_names(records)
        return df


    # Batch download endpoint:
    # 'https://datamine.cmegroup.com/cme/api/v1/batchdownload?dataset=eod&yyyymmdd=20241120&period=f'
    url = 'https://datamine.cmegroup.com/cme/api/v1/batchdownload?dataset=eod&yyyymmdd=20241120&period=f'

if __name__ == "__main__":

    fid_date = datetime.datetime.now().strftime("%Y%m%d")

    fid_dict = {"STLBASIC_NYMEX_STLCPC_EOM_0": [_26_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES,
                                                B0_MONT_BELVIEU_LDH_PROPANE_OPIS_FUTURES,
                                                BZ_BRENT_CRUDE_OIL_LAST_DAY_FINANCIAL_FUTURES,
                                                C0_MONT_BELVIEU_ETHANE_OPIS_FUTURES
                                               ],
                "STLBASIC_SETLCUR_EOM_SUM_0": [C1_CANADIAN_DOLLAR_US_DOLLAR_CAD_USD_FUTURES,
                                               EC_EURO_US_DOLLAR_EUR_USD_FUTURES
                                              ],
                "STLBASIC_NYMEX_EOM_SUM_0": [NG_HENRY_HUB_NATURAL_GAS_FUTURES
                                             ]
    }
    
    cme = CMEDatamineAPI()
    dict_dfs = cme.get_dfs_from_fid_dict(fid_dict=fid_dict)
    dict_dfs = cme.trim_top_month_on_dfs(dict_dfs=dict_dfs)
    df = cme.concat_dfs_into_sum_df(dict_dfs)
    df = cme.clean_df(df, ["DATA_SET", "MTH_STRIKE", "SETT", "DAILY_LAST"])
    df.rename(columns={"DATA_SET":"Data_Set", "MTH_STRIKE":"Month", "SETT":"Settlement_Price", "DAILY_LAST":"Last_Price"}, inplace=True)
    df = cme.transform_df_for_azure_upsert(df)
    print(df)

#  yyyymmdd-dataset_exch_symbol_foi_spread-venue
# payload = open("request.json")
# headers = {'content-type': 'application/json', 'Accept-Charset': 'UTF-8'}
# r = requests.post(url, data=payload, headers=headers)
