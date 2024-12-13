####################################
# Author: Jon Willinger
# Date: 2024-11-21
# Notes: 
# api key registration url: https://www.eia.gov/opendata/register.php (api license terms here).
# api key name: Jon Willinger (Can be anyone; email is available to the team).
# api key registration email: resinsmartauto@rtiglobal.com
# eia front url: https://www.eia.gov/dnav/pet/PET_SUM_SNDW_A_(NA)_YUP_PCT_W.htm
#
# Notes: Percent Utilization is calculated as gross inputs divided by the latest 
# reported monthly operable capacity (using unrounded numbers).  See Definitions, 
# Sources, and Notes link above for more information on this table.
#
####################################

import os, csv, re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import datetime
import pandas as pd

YUP = "YUP"
YRL = "YRL"
GINP = "EPXXX2"

class eiaapi():

    def __init__(self):
        def _define_datasets():
            dataset=[{"process":YUP}, {"process":YRL}, {"product":GINP}]
            return dataset
        
        self.base_url = "https://api.eia.gov/v2/petroleum/"
        self.dataset = _define_datasets()
        

    def get_data(self):

        def _execute_calls_get_objects():
                # Define the retry strategy.
                retry_strategy = Retry(
                    total=4,  # Maximum number of retries.
                    status_forcelist=[429, 500, 502, 503, 504],  # the HTTP status codes to retry on.
                )
                
                adapter = HTTPAdapter(max_retries=retry_strategy)
                
                # Create a new session object.
                session = requests.Session()
                session.mount("https://", adapter)

                # gross input: https://api.eia.gov/v2/petroleum/pnp/wiup/data/?frequency=weekly&data[0]=value&facets[product][]=EPXXX2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000
                # ref op cap: 
                responses_dict = {}
                for data in self.dataset:
                    keys = [k for k in data.keys()]; key = keys[0]
                    endpoint = f"pnp/wiup/data/?api_key={os.environ['EIA_API_KEY']}&frequency=weekly&data[0]=value&facets[{key}][]={data[key]}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=12"
                    url = self.base_url + endpoint
                    response = session.get(url)
                    status_code = response.status_code
                    if status_code == 200:
                        print(f"Success: {status_code}. {url}")
                        responses_dict[data[key]]=response
                    else:
                         print(f"Failed: {status_code}. {url}")
                         responses_dict[data[key]] = []

                return responses_dict
    
        def _return_most_recent_friday__from_date(datetime_day):
            weekday = datetime_day.weekday()
            if weekday < 4: datetime_friday = datetime_day - datetime.timedelta(days=(weekday+3))
            elif weekday == 4: datetime_friday = datetime_day
            else: datetime_friday = datetime_day - datetime.timedelta(days=(weekday-4))
            return datetime_friday

        def _process_into_utilization(df_dict):
            
            df_yup = pd.DataFrame({})
            df_yrl = pd.DataFrame({})
            df_ginp = pd.DataFrame({})

            for k in df_dict.keys():
                if k == YUP: 
                    df_yup = df_dict[k]
                elif k == YRL: 
                    df_yrl = df_dict[k]
                elif k == GINP: 
                    df_ginp = df_dict[k]

            columns = ["period", "duoarea", "area-name", "series-description", "value", "units"]
            df_yup_ = df_yup[columns]
            area_names = ["PADD 3", "U.S."]
            merge_columns = columns[0:3]; columns.append("product-name")
            df_ = df_yrl.merge(right=df_ginp[columns], how="inner", on=merge_columns, suffixes=("", ".ginp"))
            filter_columns = columns.copy(); filter_columns.extend(["series-description.ginp", "value.ginp", "units.ginp"])
            df_eia = df_[filter_columns]
            df_eia.drop(labels=["product-name"], axis=1, inplace=True)
            map_names = {"series-description":"output-description", "value":"output-value", "units":"output-units", 
                         "series-description.ginp":"input-description", "value.ginp":"input-value", "units.ginp":"input-units"}
            map = {"output-description":str, "output-value":float, "output-units":str, 
                         "input-description":str, "input-value":float, "input-units":str}
            df_eia.rename(columns=map_names, inplace=True)
            df_eia = df_eia.astype(map)
            df_eia["percent-utilization"] = (df_eia["input-value"]/df_eia["output-value"])*100
            df_eia["percent-utilization"] = df_eia["percent-utilization"].round(2)
            
            datetime_friday = _return_most_recent_friday__from_date(datetime.datetime.today()); datestr_friday = datetime_friday.strftime("%Y-%m-%d")
            datetime_prev_friday = datetime_friday - datetime.timedelta(days=7); datestr_prev_friday = datetime_prev_friday.strftime("%Y-%m-%d")
            merge_columns.append("percent-utilization")
            df_eia_fil = df_eia[(df_eia["period"]==datetime_friday) & (df_eia["area-name"].isin(area_names))][merge_columns]
            if df_eia_fil.empty:
                df_eia_fil = df_eia[(df_eia["period"]==datestr_prev_friday) & (df_eia["area-name"].isin(area_names))][merge_columns]
            
            return df_eia_fil

        # Entry:
        # ``````
        responses_dict = _execute_calls_get_objects()
        df_dict = {}
        for k in responses_dict:
            response_object = responses_dict[k]
            data_records = response_object.json()["response"]["data"]
            df = pd.DataFrame(data=data_records)
            df_dict[k] = df
        
        df = _process_into_utilization(df_dict)

        return df


if __name__ == "__main__":
     eia = eiaapi()

     df = eia.get_data()
     print(df)