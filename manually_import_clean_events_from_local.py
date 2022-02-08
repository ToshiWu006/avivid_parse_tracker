from s3_parser import TrackingParser
from import_tracker_clean_data_yesterday import save_six_clean_events, fetch_enable_analysis_web_id
import datetime, argparse



if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Import data from local storage')
    parser.add_argument("-w", "--web_id", help="web_id wanted to be imported to sql, if None, fetch all")
    parser.add_argument("-dt1", "--date1_UTC8", help="starting date with formate 2022-01-01")
    parser.add_argument("-dt2", "--date2_UTC8", help="ending date with formate 2022-01-01")
    args = parser.parse_args()
    web_id = args.web_id
    date_start, date_end = args.date1_UTC8, args.date2_UTC8

    if date_start == None or date_end == None:
        ## default import last hour
        date_start = datetime.datetime.utcnow() + datetime.timedelta(days=-1, hours=8)
        date_end = datetime.datetime.utcnow() + datetime.timedelta(days=-1, hours=8)
        date_start, date_end = datetime.datetime.strftime(date_start, "%Y-%m-%d"), datetime.datetime.strftime(date_end, "%Y-%m-%d")
        # tracking = TrackingParser(web_id, date_utc8, date_utc8)

    if web_id==None:
        web_id_all = fetch_enable_analysis_web_id()
        for web_id in web_id_all:
            tracking = TrackingParser(web_id, date_start, date_end)
            save_six_clean_events(tracking)
    else:
        tracking = TrackingParser(web_id, date_start, date_end)
        save_six_clean_events(tracking)



