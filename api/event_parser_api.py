import os.path
from fastapi import FastAPI
from definitions import ROOT_DIR
import uvicorn, datetime
from s3_parser import TrackingParser
from basic import filterListofDictByDict, datetime_to_str, to_datetime
from db import DBhelper
description = """
# fetch raw event by web_id and event_type
"""
tags_metadata = [
    {
        "name": "Fetch events from local by date, hour (check for collecting step)",
        "description": "event_type: load, leave, addCart, removeCart, purchase, acceptCoupon, discardCoupon",
    },
    {
        "name": "Fetch events from local by date (check for collecting step)",
        "description": "event_type: load, leave, addCart, removeCart, purchase, acceptCoupon, discardCoupon",
    },
    {
        "name": "Fetch events from sql by date, hour (check for cleaning step)",
        "description": "event_type: load, leave, addCart, removeCart, purchase, acceptCoupon, discardCoupon",
    },
    {
        "name": "Fetch latest events from sql (check for cleaning step)",
        "description": "event_type: load, leave, addCart, removeCart, purchase, acceptCoupon, discardCoupon",
    },

    {
        "name": "Fetch status of web_id",
        "description": "enable_tracking: tracking code is on or off, enable_analysis: data import sql or not, enable_addfan: sending coupon",
    },

]
app = FastAPI(title="EventCheckApp", description=description, openapi_tags=tags_metadata)

datetime_utc8 = datetime.datetime.utcnow() + datetime.timedelta(hours=8)

date_now = datetime_to_str(datetime_utc8)
hour_last = int(datetime_to_str(datetime_utc8, "%H"))-1
ROOT_DIR = ROOT_DIR

@app.get("/event/local_hour", tags=["Fetch events from local by date, hour (check for collecting step)"])
def get_local_events(date:str=date_now , hour:int=hour_last, web_id: str='nineyi11', event_type: str='purchase', n_limit: int=1):
    data = TrackingParser.get_file_byHour(date, hour, utc=8)
    if len(data)==0:
        return {"message": "no available data", "data": ""}
    data_list_filter = filterListofDictByDict(data,
                        dict_criteria={"web_id": web_id, "event_type": event_type})
    return {"message": "success", "data": data_list_filter[:n_limit]}

@app.get("/event/local_date", tags=["Fetch events from local by date (check for collecting step)"])
def get_local_events(date:str=date_now , web_id: str='nineyi11', event_type: str='purchase', n_limit: int=1):
    tracking = TrackingParser(web_id, date, date)
    data = filterListofDictByDict(tracking.data_list, dict_criteria={"web_id": web_id, "event_type":event_type})
    if len(data)==0:
        return {"message": "no available data", "data": ""}
    return {"message": "success", "data": data[:n_limit]}


@app.get("/event/sql", tags=["Fetch events from sql by date, hour (check for cleaning step)"])
def get_sql_events(date:str=date_now , hour:int=hour_last, web_id: str='nineyi11', event_type: str='purchase', n_limit: int=1):
    datetime_start = f"{date} {hour}:00:00"
    datetime_end = f"{date} {hour+1}:00:00"
    table_dict = {"load":"clean_event_load", "leave":"clean_event_leave",
                  "addCart":"clean_event_addCart", "removeCart":"clean_event_removeCart",
                  "purchase":"clean_event_purchase",
                  "acceptCoupon":"clean_event_acceptCoupon",
                  "discardCoupon":"clean_event_discardCoupon"}
    if event_type not in table_dict.keys():
        return {"message": "sql table not existed", "data": ""}
    query = f"""
                SELECT 
                    *
                FROM
                    tracker.{table_dict[event_type]}
                WHERE
                    date_time BETWEEN '{datetime_start}' AND '{datetime_end}'
                        AND web_id = '{web_id}'
                LIMIT {n_limit}
            """
    data = DBhelper("tracker").ExecuteSelect(query)

    if len(data)==0:
        return {"message": "no available data", "data": ""}

    return {"message": "success", "data": data}

@app.get("/event/latest_sql", tags=["Fetch latest events from sql (check for cleaning step)"])
def get_sql_latest_events(web_id: str='nineyi11', event_type: str='purchase', n_limit: int=1):

    table_dict = {"load":"clean_event_load", "leave":"clean_event_leave",
                  "addCart":"clean_event_addCart", "removeCart":"clean_event_removeCart",
                  "purchase":"clean_event_purchase",
                  "acceptCoupon":"clean_event_acceptCoupon",
                  "discardCoupon":"clean_event_discardCoupon"}
    if event_type not in table_dict.keys():
        return {"message": "sql table not existed", "data": ""}
    query = f"""
                SELECT 
                    *
                FROM
                    tracker.{table_dict[event_type]}
                WHERE web_id = '{web_id}' 
                ORDER BY id desc
                LIMIT {n_limit}
            """
    data = DBhelper("tracker").ExecuteSelect(query)

    if len(data)==0:
        return {"message": "no available data", "data": ""}

    return {"message": "success", "data": data}


@app.get("/status", tags=["Fetch status of web_id"])
def get_status(web_id: str='nineyi11'):

    query = f"""
                SELECT 
                    enable_tracking, enable_analysis, enable_addfan
                FROM
                    cdp_tracking_settings
                WHERE web_id = '{web_id}' 
            """
    data = DBhelper("rheacache-db0", is_ssh=True).ExecuteSelect(query)

    if len(data)==0:
        return {"message": "no available data", "data": ""}

    return {"message": "success", "data": data}

if __name__ == "__main__":
    uvicorn.run("api.event_parser_api:app", host="0.0.0.0", port=8001, log_level="info")