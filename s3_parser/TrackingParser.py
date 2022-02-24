from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict, to_datetime
from definitions import ROOT_DIR
from db import MySqlHelper
import datetime, os, pickle, json
import pandas as pd
import numpy as np

class TrackingParser:
    def __init__(self, web_id=None, date_utc8_start=None, date_utc8_end=None):
        self.web_id = web_id
        # self.use_db = use_db
        self.event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase']
        self.event_type_coupon_list = ['sendCoupon', 'acceptCoupon', 'discardCoupon']
        self.dict_object_key = {'addCart':'cart', 'removeCart':'remove_cart', 'purchase':'purchase'}
        self.dict_settings = self.fetch_parse_key_settings(web_id)
        self.date_utc8_start = date_utc8_start
        self.date_utc8_end = date_utc8_end
        self.data_list = self.get_data_by_daterange(date_utc8_start, date_utc8_end)
        ## use db or use local raw data
        # self.df_loaded = self.get_df_from_db('load') if use_db else self.get_df('load')
        # self.df_leaved = self.get_df_from_db('leave') if use_db else self.get_df('leave')
        # self.df_timeout = self.get_df_from_db('timeout') if use_db else self.get_df('timeout')
        # self.df_addCart = self.get_df_from_db('addCart') if use_db else self.get_df('addCart')
        # self.df_removeCart = self.get_df_from_db('removeCart') if use_db else self.get_df('removeCart')
        # self.df_purchased = self.get_df_from_db('purchase') if use_db else self.get_df('purchase')
        self.features = ['pageviews', 'time_pageview_total', 'click_count_total', 'landing_count', 'max_pageviews', 'device']

    def __str__(self):
        return "TrackingParser"

    def get_six_events_df(self, web_id=None, data_list=None, dict_settings=None, use_db=False):
        if web_id==None:
            web_id = self.web_id
        if data_list==None:
            data_list = self.data_list
        if dict_settings==None:
            dict_settings = self.dict_settings
        if web_id!=self.web_id:
            ## change to use dict_settings from web_id instead of self.web_id
            dict_settings = self.fetch_parse_key_settings(web_id)
        df_loaded = self.get_df_from_db('load') if use_db else self.get_df(web_id, data_list, 'load', dict_settings)
        df_leaved = self.get_df_from_db('leave') if use_db else self.get_df(web_id, data_list, 'leave', dict_settings)
        df_timeout = self.get_df_from_db('timeout') if use_db else self.get_df(web_id, data_list, 'timeout', dict_settings)
        df_addCart = self.get_df_from_db('addCart') if use_db else self.get_df(web_id, data_list, 'addCart', dict_settings)
        df_removeCart = self.get_df_from_db('removeCart') if use_db else self.get_df(web_id, data_list, 'removeCart', dict_settings)
        df_purchased = self.get_df_from_db('purchase') if use_db else self.get_df(web_id, data_list, 'purchase', dict_settings)
        # self.df_loaded, self.df_leaved, self.df_timeout = df_loaded, df_leaved, df_timeout
        # self.df_addCart, self.df_removeCart, self.df_purchased = df_addCart, df_removeCart, df_purchased
        return df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased

    ## get sendCoupon, acceptCoupon, discardCoupon events
    def get_three_coupon_events_df(self, web_id=None, data_list=None, use_db=False):
        if web_id==None:
            web_id = self.web_id
        if data_list==None:
            data_list = self.data_list
        df_sendCoupon = self.get_df_from_db('sendCoupon') if use_db else self.get_df(web_id, data_list, 'sendCoupon',)
        df_acceptCoupon = self.get_df_from_db('acceptCoupon') if use_db else self.get_df(web_id, data_list, 'acceptCoupon')
        df_discardCoupon = self.get_df_from_db('discardCoupon') if use_db else self.get_df(web_id, data_list, 'discardCoupon')
        return df_sendCoupon, df_acceptCoupon, df_discardCoupon
    #### main function to get clean data (load, leave, addCart, removeCart, purchase,
    #### sendCoupon, acceptCoupon, discardCoupon)
    @logging_channels(['clare_test'])
    def get_df(self, web_id, data_list, event_type, dict_settings=None):
        """

        Parameters
        ----------
        web_id: if None, use all web_id
        data_list: list of dict which to be appended
        event_type: load, leave, addCart, removeCart, purchase, sendCoupon, acceptCoupon, discardCoupon
        dict_settings: settings for addCar, removeCar and purchase events

        Returns: DataFrame, df
        -------

        """
        # if dict_settings==None:
        #     dict_settings = self.fetch_parse_key_settings(web_id)
        if web_id==None:
            data_list_filter = filterListofDictByDict(data_list,
                                                      dict_criteria={"event_type": event_type})
        else:
            data_list_filter = filterListofDictByDict(data_list, dict_criteria={"web_id": web_id, "event_type":event_type})
        dict_list = []
        if event_type=='load':
            for data_dict in data_list_filter:
                dict_list += self.fully_parse_loaded(data_dict)
        elif event_type=='leave' or event_type=='timeout':
            for data_dict in data_list_filter:
                dict_list += self.fully_parse_leaved_timeout(data_dict)
        ## addCart, removeCart, purchase
        # else:
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='purchase':
            if dict_settings == None:
                dict_settings = self.fetch_parse_key_settings(web_id)
            for data_dict in data_list_filter:
                dict_list += self.fully_parse_object(data_dict, event_type, dict_settings)
        ## sendCoupon, acceptCoupon, discardCoupon
        elif event_type=='sendCoupon' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            for data_dict in data_list_filter:
                dict_list += self.fully_parse_coupon(data_dict)
        else:
            print("not a valid event")
            return pd.DataFrame()  ## early return
        self.data_list_clean = dict_list
        if dict_list == []:
            return pd.DataFrame() ## early return
        else:
            df = pd.DataFrame(dict_list)
        df['date_time'] = [datetime.datetime.utcfromtimestamp(ts/1000)+datetime.timedelta(hours=8) for ts in df['timestamp']]
        if event_type=='purchase': ## unique key in table
            df.drop(columns=self._get_drop_col('purchase'), inplace=True)
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='leave' or event_type=='timeout':
            df['max_time_no_scroll_array'] = [','.join([str(i) for i in data]) for data in df['max_time_no_scroll_array']]
            df['max_time_no_scroll_depth_array'] = [','.join([str(i) for i in data]) for data in df['max_time_no_scroll_depth_array']]
        df.drop_duplicates(subset=self._get_unique_col(event_type), inplace=True)
        df.dropna(inplace=True)
        df = self.clean_before_sql(df, criteria_len={'web_id': 45, 'uuid': 36, 'ga_id': 45, 'fb_id': 45, 'timestamp': 16})
        return df

    @timing
    def get_df_from_db(self, event_type, columns=None):
        if columns==None:
            columns = self._get_df_event_col(event_type)
        table = self._get_event_table(event_type)
        query = f"""SELECT {','.join(columns)} FROM {table} WHERE date_time BETWEEN '{self.date_utc8_start}' and '{self.date_utc8_end}' 
                    and web_id='{self.web_id}'"""
        print(query)
        data = MySqlHelper('tracker').ExecuteSelect(query)
        df = pd.DataFrame(data, columns=columns)
        return df

    @staticmethod
    def clean_before_sql(df, criteria_len={'web_id': 45, 'uuid': 36, 'ga_id': 45, 'fb_id': 45, 'timestamp': 16,
                                           'event_type': 16}):
        """

        Parameters
        ----------
        df: DataFrame to enter sql table
        criteria_len: convert to str and map(len) <= criteria

        Returns
        -------

        """
        cols = criteria_len.keys()
        for col in cols:
            if col in df.columns:
                df[col] = df[col].astype(str)
                df = df[df[col].map(len) <= criteria_len[col]]
        return df

    ## addCart,removeCart,purchased events
    def fully_parse_object(self, data_dict, event_type, dict_settings):
        object_key = self.dict_object_key[event_type]
        key_join_list, key_rename_list = dict_settings[event_type]
        ## 1. parse common terms
        universial_dict = self.parse_rename_universial(data_dict)
        ## 2. parse record_user terms
        record_dict = self.parse_rename_record_user(data_dict)
        ## 3. parse cart, remove_cart or purchase terms
        object_dict_list = self.parse_rename_object(data_dict, key_join_list, key_rename_list, object_key)
        result_dict_list = []
        for object_dict in object_dict_list:
            object_dict.update(universial_dict)
            object_dict.update(record_dict)
            result_dict_list += [object_dict]
        return result_dict_list

    ## loaded event
    @staticmethod
    def fully_parse_loaded(data_dict):
        universial_dict = TrackingParser.parse_rename_universial(data_dict)
        key_list = ['dv', 'ul', 'un', 'm_t', 'i_l',
                    'ps', 't_p_t', 's_id', 's_idl', 'l_c',
                    's_h','w_ih','c_c_t', 'mt_nm', 'mt_ns',
                    'mt_nc', 'mps', 'mt_p', 'mt_p_t', 'ms_d',
                    'i_ac', 'i_rc', 'i_pb'] ## remove mt_nd(max_time_no_scroll_depth)
        key_rename_list = ['device', 'url_last', 'url_now', 'meta_title', 'is_landing',
                           'pageviews', 'time_pageview_total', 'session_id', 'session_id_last', 'landing_count',
                           'scroll_height', 'window_innerHeight', 'click_count_total', 'max_time_no_move_last', 'max_time_no_scroll_last',
                           'max_time_no_click_last', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth',
                           'is_addCart', 'is_removeCart', 'is_purchased_before']

        object_dict = data_dict['load']
        loaded_dict = {}
        for key, key_rename in zip(key_list, key_rename_list):
            if key not in object_dict.keys():
                loaded_dict.update({key_rename: -1})
            else:
                loaded_dict.update({key_rename: object_dict[key]})
        universial_dict.update(loaded_dict)
        return [universial_dict]

    ## leaved and timeout event
    @staticmethod
    def fully_parse_leaved_timeout(data_dict):
        universial_dict = TrackingParser.parse_rename_universial(data_dict)
        record_dict = TrackingParser.parse_rename_record_user(data_dict)
        universial_dict.update(record_dict)
        return [universial_dict]

    ## leaved and timeout event
    @staticmethod
    def fully_parse_coupon(data_dict):
        universial_dict = TrackingParser.parse_rename_universial(data_dict)
        coupon_info_dict = TrackingParser.parse_rename_coupon_info(data_dict)
        record_dict = TrackingParser.parse_rename_record_user(data_dict)
        universial_dict.update(coupon_info_dict)
        universial_dict.update(record_dict)
        return [universial_dict]

    @staticmethod
    def parse_rename_universial(data_dict):
        key_list = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'coupon']
        key_rename_list = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon']
        universial_dict = {}
        for key,key_rename in zip(key_list,key_rename_list):
            if key in data_dict.keys():
                universial_dict.update({key_rename: data_dict[key]})
            else:
                universial_dict.update({key_rename: '_'})
        return universial_dict

    @staticmethod
    def parse_rename_record_user(data_dict):
        record_user_dict = data_dict['record_user']
        if type(record_user_dict)==str:
            record_user_dict = json.loads(record_user_dict)
        key_list = ['dv', 'ul', 'un', 'm_t', 's_h',
                    'w_ih', 't_p', 's_d', 's_d_', 'c_c',
                    'c_c_t', 't_nm', 't_ns', 't_nc', 'mt_nm',
                    'mt_ns', 'mt_nsa', 'mt_nda', 'mt_nd', 'mt_nd_',
                    'mt_nc', 'i_l', 's_idl', 's_id', 'ps',
                    't_p_t', 't_p_tl', 'mps', 'mt_p', 'mt_p_t',
                    'ms_d', 'ms_d_p', 'ms_d_pl', 'mt_nml', 'mt_nsl',
                    'mt_ncl', 'ms_dl', 'l_c',
                    'i_ac', 'i_rc', 'i_pb']
        key_rename_list = ['device', 'url_last', 'url_now', 'meta_title', 'scroll_height',
                           'window_innerHeight', 'time_pageview', 'scroll_depth', 'scroll_depth_px', 'click_count',
                           'click_count_total', 'time_no_move', 'time_no_scroll', 'time_no_click', 'max_time_no_move',
                           'max_time_no_scroll', 'max_time_no_scroll_array', 'max_time_no_scroll_depth_array',
                           'max_time_no_scroll_depth', 'max_time_no_scroll_depth_px',
                           'max_time_no_click', 'is_landing', 'session_id_last', 'session_id', 'pageviews',
                           'time_pageview_total', 'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                           'max_time_pageview_total',
                           'max_scroll_depth', 'max_scroll_depth_page', 'max_scroll_depth_page_last',
                           'max_time_no_move_last', 'max_time_no_scroll_last',
                           'max_time_no_click_last', 'max_scroll_depth_last', 'landing_count',
                           'is_addCart', 'is_removeCart', 'is_purchased_before']
        record_dict = {}
        for key, key_rename in zip(key_list, key_rename_list):
            if key in record_user_dict.keys():
                record_dict.update({key_rename: record_user_dict[key]})
            else:
                record_dict.update({key_rename: -1})
        return record_dict


    ## main for parse and rename 'addcart', 'removeCart', 'purchase' event
    @staticmethod
    def parse_rename_object(data_dict, key_join_list, key_rename_list, object_key='purchase'):
        if object_key not in data_dict.keys():
            print(f"{object_key} not in {data_dict}, return []")
            return []
        collection_dict, dict_object = {}, json.loads(data_dict[object_key])
        ## for dealing with adding 'purchase' key in purchase (91app, lovingfamily)
        if key_join_list[0].split('.')[0] not in dict_object.keys():
            dict_object = list(dict_object.values())[0]
        ## skip dict_object is not dict
        if type(dict_object)!=dict:
            return []
        value_list = []
        n_list = 0
        # print(dict_object)
        ## parse dict type key and store list type key
        for key, key_rename in zip(key_join_list, key_rename_list):
            key_list = key.split('.')
            value = ''
            if len(key_list) == 1:  ##directly access dict
                for k in key_list: ## append -1 if key not found
                    collection_dict.update({key_rename: dict_object[k]}) if k in dict_object.keys() else collection_dict.update({key_rename: -1})
            else:  ## parse multiple layer
                for key_2nd in key_list:
                    if value == '':  ## 1st level
                        value = '_' if key_2nd == 'empty' else dict_object[key_2nd]
                    elif key_2nd=='json': ## use json.loads() => i3fresh case
                        value = json.loads(value)
                    elif type(value) == dict:  ## 2nd, 3rd... level
                        value = '_' if key_2nd == 'empty' else value[key_2nd]
                        collection_dict.update({key_rename: value})
                    elif type(value) == list:  ## 2nd, 3rd... level(parse list)
                        n_list = len(value)
                        for v in value: ## value: list [{k21:v21, k22:v22, k23:v23,...}]
                            if key_2nd in v.keys():
                                value = '_' if key == 'empty' else v[key_2nd]
                            else: ## not in k21,k22,k23...
                                value = '_'
                            value_list += [value]
                    else:
                        print(f'do nothing in {dict_object}')
        ## for parse multiple objects in a main_object
        if value_list == []:
            collection_purchase_dict_list = [collection_dict]
        else:
            # create multiple purchase record
            n_dict_key = len(collection_dict.keys())
            n_dict_list_key = int(len(value_list) / n_list)
            collection_purchase_dict_list = []
            if n_list != 0:
                for i in range(n_list):
                    temp_dict = {}
                    for j in range(n_dict_list_key):
                        temp_dict.update({key_rename_list[n_dict_key + j]: value_list[n_list * j + i]})
                    temp_dict.update(collection_dict)
                    collection_purchase_dict_list += [temp_dict]
            else:
                collection_purchase_dict_list = [collection_dict]
        return collection_purchase_dict_list

    @staticmethod
    def parse_rename_coupon_info(data_dict):
        if 'coupon_info' not in data_dict.keys():
            print(f"'coupon_info' not in {data_dict}, return []")
            return []
        dict_object = data_dict['coupon_info']
        key_list = ['p_p', 'c_t', 'c_d', 'c_c', 'c_st',
                    'c_ty', 'c_a', 'c_c_t', 'c_c_m', 'l_c']
        key_rename_list = ['prob_purchase', 'coupon_title', 'coupon_description', 'coupon_code', 'coupon_setTimer',
                           'coupon_type', 'coupon_amount', 'coupon_customer_type', 'coupon_code_mode', 'link_code']
        coupon_info_dict = {}
        for key,key_rename in zip(key_list,key_rename_list):
            if key in dict_object.keys():
                coupon_info_dict.update({key_rename: dict_object[key]})
            else:
                coupon_info_dict.update({key_rename: -1})
        return coupon_info_dict

    @staticmethod
    @timing
    def fetch_parse_key_settings(web_id):
        if web_id==None:
            print("no settings to fetch")
            return {}
        query = f"""SELECT parsed_purchase_key, parsed_purchase_key_rename, parsed_addCart_key, parsed_addCart_key_rename,
                            parsed_removeCart_key, parsed_removeCart_key_rename
                            FROM cdp_tracking_settings where web_id='{web_id}'"""
        print(query)
        data = MySqlHelper("rheacache-db0").ExecuteSelect(query)
        settings = [x.split(',') for x in data[0]]
        dict_settings = {}
        for i,event_type in enumerate(['purchase', 'addCart', 'removeCart']):
            dict_settings.update({event_type: settings[i*2:(i+1)*2]})
        return dict_settings


    ################################# get data using local storage #################################
    @staticmethod
    def get_file_byDatetime(datetime_utc0):
        """

        Parameters
        ----------
        datetime_utc0: with format, str:'2022-01-01 10:00:00' or datetime.datetime: 2022-01-01 10:00:00

        Returns: data_list
        -------

        """
        ## convert to datetime.datetime
        if type(datetime_utc0)==str:
            datetime_utc0 = datetime.datetime.strptime(datetime_utc0, "%Y-%m-%d %H:%M:%S")
        MID_DIR = datetime.datetime.strftime(datetime_utc0, format="%Y/%m/%d/%H")
        path = os.path.join(ROOT_DIR, "s3data", MID_DIR, "rawData.pickle")
        if os.path.isfile(path):
            with open(path, 'rb') as f:
                data_list = pickle.load(f)
        return data_list

    @staticmethod
    def get_file_byHour(date_utc0, hour_utc0='00'):
        """

        Parameters
        ----------
        date_utc0: with format, str:'2022-01-01' or str:'2022/01/01'
        hour_utc0: with format, str:'00'-'23' or int:0-23

        Returns: data_list
        -------

        """
        if type(date_utc0)==datetime.datetime:
            date_utc0 = datetime.datetime.strftime(date_utc0, '%Y-%m-%d')
        if type(hour_utc0)==int:
            hour_utc0 = f"{hour_utc0:02}"
        path = os.path.join(ROOT_DIR, "s3data", date_utc0.replace('-', '/'), hour_utc0, "rawData.pickle")
        if os.path.isfile(path):
            with open(path, 'rb') as f:
                data_list = pickle.load(f)
        return data_list

    @staticmethod
    def get_data_by_daterange(date_utc8_start='2022-01-01', date_utc8_end='2022-01-11'):
        if date_utc8_start==None or date_utc8_end==None:
            print("input date_utc8 range is None")
            return []
        num_days = (to_datetime(date_utc8_end) - to_datetime(date_utc8_start)).days+1
        date_utc8_list = [to_datetime(date_utc8_start) + datetime.timedelta(days=x) for x in range(num_days)]
        data_list = []
        for date_utc8 in date_utc8_list:
            data_list += TrackingParser.get_data_by_date(date_utc8)
        return data_list

    @staticmethod
    def get_data_by_date(date_utc8):
        file_list = TrackingParser.get_a_day_file_list(date_utc8)
        data_list = []
        for file in file_list:
            if os.path.isfile(file):
                with open(file, 'rb') as f:
                    data_list += pickle.load(f)
        return data_list

    @staticmethod
    def get_a_day_file_list(date_utc8):
        if type(date_utc8) == datetime.datetime:
            datetime_utc0 = date_utc8 + datetime.timedelta(hours=-8)
        else:
            datetime_utc0 = datetime.datetime.strptime(date_utc8, "%Y-%m-%d") + datetime.timedelta(hours=-8)
        datetime_list = [datetime_utc0 + datetime.timedelta(hours=x) for x in range(24)]
        file_list = [
            os.path.join(ROOT_DIR, "s3data", datetime_to_str(root_folder, pattern="%Y/%m/%d/%H"), "rawData.pickle") for
            root_folder in datetime_list]
        return file_list
    @staticmethod
    def _get_event_table(event_type):
        if event_type=='load':
            table = 'clean_event_load'
        elif event_type == 'leave':
            table = 'clean_event_leave'
        elif event_type=='timeout':
            table = 'clean_event_timeout'
        elif event_type=='addCart':
            table = 'clean_event_addCart'
        elif event_type=='removeCart':
            table = 'clean_event_removeCart'
        elif event_type=='purchase':
            table = 'clean_event_purchase'
        elif event_type=='sendCoupon':
            table = 'clean_event_sendCoupon'
        elif event_type=='acceptCoupon':
            table = 'clean_event_acceptCoupon'
        elif event_type=='discardCoupon':
            table = 'clean_event_discardCoupon'
        else:
            table = ''
        return table

    @staticmethod
    def _get_df_event_col(event_type):
        if event_type=='load':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'device', 'url_last', 'url_now', 'meta_title', 'is_landing',
                       'pageviews', 'time_pageview_total', 'session_id', 'session_id_last',
                       'landing_count', 'scroll_height', 'window_innerHeight',
                       'click_count_total', 'max_time_no_move_last', 'max_time_no_scroll_last',
                       'max_time_no_click_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'date_time']
        elif event_type=='leave':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'device', 'url_last', 'url_now', 'meta_title', 'scroll_height',
                       'window_innerHeight', 'time_pageview', 'scroll_depth',
                       'scroll_depth_px', 'click_count', 'click_count_total', 'time_no_move',
                       'time_no_scroll', 'time_no_click', 'max_time_no_move',
                       'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='timeout':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'device', 'url_last', 'url_now', 'meta_title', 'scroll_height',
                       'window_innerHeight', 'time_pageview', 'scroll_depth',
                       'scroll_depth_px', 'click_count', 'click_count_total', 'time_no_move',
                       'time_no_scroll', 'time_no_click', 'max_time_no_move',
                       'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='addCart':
            columns = ['product_category', 'product_category_name', 'product_id',
                       'product_name', 'product_price', 'product_quantity', 'web_id', 'uuid',
                       'ga_id', 'fb_id', 'timestamp', 'avivid_coupon', 'device', 'url_last',
                       'url_now', 'meta_title', 'scroll_height', 'window_innerHeight',
                       'time_pageview', 'scroll_depth', 'scroll_depth_px', 'click_count',
                       'click_count_total', 'time_no_move', 'time_no_scroll', 'time_no_click',
                       'max_time_no_move', 'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='removeCart':
            columns = ['product_id', 'product_name', 'product_quantity', 'product_price',
                       'sku_id', 'sku_name', 'currency', 'web_id', 'uuid', 'ga_id', 'fb_id',
                       'timestamp', 'avivid_coupon', 'device', 'url_last', 'url_now',
                       'meta_title', 'scroll_height', 'window_innerHeight', 'time_pageview',
                       'scroll_depth', 'scroll_depth_px', 'click_count', 'click_count_total',
                       'time_no_move', 'time_no_scroll', 'time_no_click', 'max_time_no_move',
                       'max_time_no_scroll', 'max_time_no_scroll_array',
                       'max_time_no_scroll_depth_array', 'max_time_no_scroll_depth',
                       'max_time_no_scroll_depth_px', 'max_time_no_click', 'is_landing',
                       'session_id_last', 'session_id', 'pageviews', 'time_pageview_total',
                       'time_pageview_total_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'max_scroll_depth_page',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='purchase':
            columns = ['product_id', 'product_name', 'product_price', 'product_quantity',
                       'product_category', 'product_variant', 'coupon', 'currency', 'order_id',
                       'total_price', 'shipping_price', 'web_id', 'uuid', 'ga_id', 'fb_id',
                       'timestamp', 'avivid_coupon', 'device', 'url_last', 'url_now',
                       'scroll_height', 'window_innerHeight', 'click_count_total',
                       'is_landing', 'session_id_last', 'session_id', 'pageviews',
                       'time_pageview_total', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth',
                       'max_scroll_depth_page_last', 'max_time_no_move_last',
                       'max_time_no_scroll_last', 'max_time_no_click_last',
                       'max_scroll_depth_last', 'landing_count', 'date_time']
        elif event_type=='sendCoupon' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            columns = ['web_id', 'uuid', 'ga_id', 'fb_id', 'timestamp', 'avivid_coupon',
                       'prob_purchase', 'coupon_title', 'coupon_description', 'coupon_code', 'coupon_setTimer',
                       'coupon_type', 'coupon_amount', 'coupon_customer_type', 'coupon_code_mode', 'link_code',
                       'device', 'url_last', 'url_now', 'meta_title', 'is_landing',
                       'pageviews', 'time_pageview_total', 'session_id', 'session_id_last',
                       'landing_count', 'scroll_height', 'window_innerHeight',
                       'click_count_total', 'max_time_no_move_last', 'max_time_no_scroll_last',
                       'max_time_no_click_last', 'max_pageviews', 'max_time_pageview',
                       'max_time_pageview_total', 'max_scroll_depth', 'date_time']
        else:
            columns = []
        return columns

    @staticmethod
    def _get_drop_col(event_type):
        if event_type=='purchase':
            drop_col_list = ['meta_title','time_pageview','scroll_depth','scroll_depth_px','click_count',
                            'time_no_move','time_no_scroll','time_no_click','max_time_no_move','max_time_no_scroll',
                            'max_time_no_scroll_array','max_time_no_scroll_depth_array','max_time_no_scroll_depth','max_time_no_scroll_depth_px',
                            'max_time_no_click','max_scroll_depth_page','time_pageview_total_last']
        return drop_col_list

    ## for df.drop_duplicates(subset), can be more than unique key in sql table
    @staticmethod
    def _get_unique_col(event_type):
        if event_type=='load':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        elif event_type=='purchase':
            subset = ['date_time', 'web_id', 'uuid', 'product_id', 'product_variant',
                      'product_quantity', 'product_category']
        elif event_type == 'addCart':
            subset = ['date_time', 'web_id', 'uuid', 'product_id', 'product_price',
                      'product_quantity', 'product_category']
        elif event_type=='removeCart':
            subset = ['date_time', 'web_id', 'uuid', 'product_id', 'sku_id']
        elif event_type=='leave':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        elif event_type == 'timeout':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        else:
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        return subset





if __name__ == "__main__":
    web_id = "nineyi11"
    date_utc8_start = "2022-02-24"
    date_utc8_end = "2022-02-24"
    tracking = TrackingParser(web_id, date_utc8_start, date_utc8_end)
    data_list = tracking.data_list
    event_type = "acceptCoupon"
    # df_addCart = tracking.get_df(web_id, data_list, 'purchase', tracking.dict_settings)
    data_list_filter = filterListofDictByDict(data_list, dict_criteria={"web_id": web_id, "event_type":event_type})
    # df = tracking.get_df(web_id, data_list_filter, event_type)

    df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = tracking.get_six_events_df()
    df_sendCoupon, df_acceptCoupon, df_discardCoupon = tracking.get_three_coupon_events_df()
    # result = []
    # for data in data_list_filter:
    #     result += tracking.fully_parse_coupon(data)
    #
    # df = pd.DataFrame(result)
    # df_coupon = tracking.get_df(web_id, data_list_filter, 'acceptCoupon')
    # df = tracking.get_df_from_db('purchase')


    # # df_loaded = tracking.df_loaded
    # # df_purchased = tracking.df_purchased
    # # df_addCart = tracking.df_addCart
    # # df_removeCart = tracking.df_removeCart
    # # df_leaved = tracking.df_leaved
    #
    # # df_loaded['date_time'] = [datetime.datetime.utcfromtimestamp(ts/1000)+datetime.timedelta(hours=8) for ts in df_loaded['timestamp']]
    # data_list_filter = filterListofDictByDict(tracking.data_list, dict_criteria={"event_type":"addCart", "web_id":'nineyi11'}) #"web_id":"nineyi11"
    #
    # ## timeout
    # query = MySqlHelper.generate_update_SQLquery(tracking.df_timeout, 'clean_event_timeout')
    # MySqlHelper('tracker').ExecuteUpdate(query, tracking.df_timeout.to_dict('records'))

    ## leave
    # query = MySqlHelper.generate_update_SQLquery(df_leaved, 'clean_event_leave')
    # MySqlHelper('tracker').ExecuteUpdate(query, df_leaved.to_dict('records'))

    # ## load
    # query = MySqlHelper.generate_update_SQLquery(df_loaded, 'clean_event_load')
    # MySqlHelper('tracker').ExecuteUpdate(query, df_loaded.to_dict('records'))

    ## removeCart
    # query = MySqlHelper.generate_update_SQLquery(df_removeCart, 'clean_event_removeCart', SQL_ACTION='INSERT INTO')
    # MySqlHelper('tracker').ExecuteUpdate(query, df_removeCart.to_dict('records'))

    ## addCart
    # query = MySqlHelper.generate_update_SQLquery(df_addCart, 'clean_event_addCart', SQL_ACTION='INSERT INTO')
    # MySqlHelper('tracker').ExecuteUpdate(query, df_addCart.to_dict('records'))

    ## purchase
    # query = MySqlHelper.generate_update_SQLquery(df_purchased, 'clean_event_purchase', SQL_ACTION='INSERT INTO')
    # MySqlHelper('tracker').ExecuteUpdate(query, df_purchased.to_dict('records'))

    ## currency,items.item_id,items.item_name,items.quantity,items.price,items.sku_id,items.sku_name
    ## currency,product_id,product_name,product_quantity,product_price,sku_id,sku_name

    # web_id = "94monster"
    # df = TrackingParser.fetch_six_events_data_by_daterange(web_id, date_utc8_start='2022-01-18', date_utc8_end='2022-01-18')

    # uuid_load = list(set(df_loaded['uuid']))
    # uuid_purchased = list(set(df_purchased['uuid']))
    # session_purchased = {row['uuid']:[row['session_id_last'],row['session_id']] for i,row in df_purchased.iterrows()}
    # dict_collect_list = []
    # keys_collect = ['uuid', 'device', 'pageviews', 'time_pageview_total', 'landing_count',
    #                 'click_count_total', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth']
    #
    # df_loaded_purchased = df_loaded.query(f"uuid in {uuid_purchased}").sort_values(by=['uuid', 'timestamp'])
    # # keys_collect = ['uuid', 'max_pageviews', 'max_time_pageview_total', 'click_count_total']
    # # for i,uuid in enumerate(uuid_load):
    # #     dict_collect = {}
    # #     df_query = df_loaded.query(f"uuid=='{uuid}'").iloc[-1]
    # #     for key in keys_collect:
    # #         vlaue = df_query[key]
    # #         dict_collect.update({key: vlaue})
    # #         if key=='uuid':
    # #             if vlaue in uuid_purchased:
    # #                 dict_append = {'is_purchased': 1}
    # #             else:
    # #                 dict_append = {'is_purchased': 0}
    # #             dict_collect.update(dict_append)
    # #
    # #     dict_collect_list += [dict_collect]
    # #     if i%100==0:
    # #         print(f"finish {i}")
    # # df_collect = pd.DataFrame(dict_collect_list)
    # df_collect = append_purchased_column(df_loaded, uuid_purchased)[keys_collect+['is_purchased']]
    #
    #
    # from sklearn.decomposition import PCA
    # # data_purchased = normalize_data(np.array(df_collect.query(f"is_purchased==1"))[:,2:])
    # data_purchased = np.array(df_collect.query(f"is_purchased==1"))[:,2:]
    #
    # pca = PCA(n_components=2)
    # result = pca.fit(data_purchased)
    # transform_purchased = result.transform(data_purchased)
    #
    # # data_not_purchased = normalize_data(np.array(df_collect.query(f"is_purchased==0"))[:,2:])
    # data_not_purchased = np.array(df_collect.query(f"is_purchased==0"))[:,2:]
    #
    # pca = PCA(n_components=2)
    # result = pca.fit(data_not_purchased)
    # transform_not_purchased = result.transform(data_not_purchased)
    # plt.figure()
    # plt.plot(transform_not_purchased[:,0], transform_not_purchased[:,1], 'bo')
    # plt.plot(transform_purchased[:,0], transform_purchased[:,1], 'ro')
    # plt.show()
    #
    #
    #
    # data = normalize_data(np.array(df_collect)[:,6:])
    # label = np.array(df_collect)[:,1]
    # pca = PCA(n_components=2)
    # result = pca.fit(data)
    # transform_data = result.transform(data)
    #
    # plt.figure()
    # plt.plot(transform_data[label==0,0], transform_data[label==0,1], 'bo')
    # plt.plot(transform_data[label==1,0], transform_data[label==1,1], 'r*')
    # plt.show()
    #
    #
    # ## clustering
    # from sklearn.mixture import GaussianMixture
    # data = np.array(df_collect)[:,6:]
    # data_nor = normalize_data(data)
    # label = np.array(df_collect)[:,1]
    # pca = PCA(n_components=2)
    # result = pca.fit(data_nor)
    # transform_data = result.transform(data_nor)
    #
    # gmm = GaussianMixture(n_components=2, tol=1e-5, init_params='random')
    # # model = Birch(threshold=0.05, n_clusters=5)
    # ##  fit the model
    # gmm.fit(transform_data)
    # ## assign a cluster to each example
    # label = gmm.predict(transform_data)
    # plt.figure()
    # data_passive, data_active = [], []
    # for i, (row,l) in enumerate(zip(transform_data,label)):
    #     if row[1]>0 and row[0]>0: ## class 1
    #         plt.plot(row[0], row[1], 'r*')
    #         data_passive += [data[i]]
    #     else:
    #         plt.plot(row[0], row[1], 'bo')
    #         data_active += [data[i]]
    # plt.show()
    # data_passive, data_active = np.array(data_passive).astype(int), np.array(data_active).astype(int)
    #
    # plt.figure()
    # for i, (row,l) in enumerate(zip(transform_data,label)):
    #     if l==1: ## class 1
    #         plt.plot(row[0], row[1], 'r*')
    #     else:
    #         plt.plot(row[0], row[1], 'bo')
    # plt.show()




    #
    #
    # web_id_list = fetch_91app_web_id()
    # # web_id_list = ['nineyi1105', 'nineyi11185', 'nineyi123', 'nineyi14267', 'nineyi1849']
    # date_utc8_start = "2022-01-19"
    # date_utc8_end = "2022-01-19"
    # df_loaded_all, df_purchased_all, len_loaded = collect_from_web_id_list(web_id_list, date_utc8_start, date_utc8_end)
    # keys_collect = ['uuid', 'device', 'pageviews', 'time_pageview_total', 'landing_count', 'click_count_total',
    #                 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth']
    # df_collect = collect_features(df_loaded_all, df_purchased_all, keys_collect=keys_collect)
    # df_collect['mean_max_time_per_page'] = df_collect['max_time_pageview_total']/df_collect['max_pageviews']
    # keys_select = ['max_pageviews', 'landing_count', 'click_count_total', 'max_time_pageview_total', 'max_scroll_depth']
    # X = np.array(df_collect[keys_select]).astype('int')
    # y = np.array(df_collect['is_purchased']).astype('int')
    #
    # model = LogisticRegression(random_state=0).fit(X, y)
    # prob = model.predict_proba(X)
    # predict = model.predict(X)
    # df_collect_predict = df_collect.copy()
    # df_collect_predict['prob'] = prob[:,1]
    # df_collect_predict['predict'] = predict
    #
    # model.score(X, y)
    # visualization_feature(df_collect, feature='device', binwidth=1, bin_start=0, bin_end=5, y_low=0, y_high=0.5)
    # visualization_feature(df_collect, feature='pageviews', binwidth=1, bin_start=0, bin_end=50, y_low=0, y_high=0.2)
    # visualization_feature(df_collect, feature='time_pageview_total', binwidth=50, bin_start=0, bin_end=1000, y_low=0, y_high=0.01)
    # visualization_feature(df_collect, feature='landing_count', binwidth=1, bin_start=0, bin_end=30, y_low=0, y_high=0.6)
    # visualization_feature(df_collect, feature='click_count_total', binwidth=2, bin_start=0, bin_end=150, y_low=0, y_high=0.1)
    # visualization_feature(df_collect, feature='max_pageviews', binwidth=1, bin_start=0, bin_end=50, y_low=0, y_high=0.2)
    # visualization_feature(df_collect, feature='max_time_pageview', binwidth=50, bin_start=0, bin_end=2000, y_low=0, y_high=0.015)
    # visualization_feature(df_collect, feature='max_time_pageview_total', binwidth=100, bin_start=0, bin_end=5000, y_low=0, y_high=0.008)
    # visualization_feature(df_collect, feature='max_scroll_depth', binwidth=1, bin_start=0, bin_end=100, y_low=0, y_high=0.8)
    #
    # visualization_feature(df_collect, feature='mean_max_time_per_page', binwidth=5, bin_start=0, bin_end=200, y_low=0, y_high=0.1)
    #
    # from sklearn.svm import SVC
    # from sklearn.pipeline import make_pipeline
    # from sklearn.preprocessing import StandardScaler
    # svm = make_pipeline(StandardScaler(), SVC(gamma='scale', kernel='rbf'))
    #
    # # svm = SVC()
    # svm.fit(X, y)
    # predictions_svm = svm.predict(X)
    # df_collect_predict['predict_svm'] = predictions_svm
    #
    # from sklearn.ensemble import RandomForestRegressor, RandomForestClassifier
    # from sklearn.model_selection import train_test_split
    #
    # X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 2)
    # RFC = RandomForestClassifier(n_estimators=100, criterion='gini')
    # RFC.fit(X_train, y_train)
    # predictions_rf = RFC.predict(X_test)
    #
    # result = np.empty((len(y_test), 2))
    # result[:,0], result[:,1] = y_test, RFC.predict(X_test)
    # result_train = np.empty((len(y_train), 2))
    # result_train[:,0], result_train[:,1] = y_train, RFC.predict(X_train)
    #
    # df_collect_predict['predict_rf'] = RFC.predict(X)
    # RFC.feature_importances_






    # df_binning = df_collect[df_collect['is_purchased']==1]
    # pageviews = np.array(df_binning)[:,2].astype('int')
    # pd1, center1, fig, ax = binning2(pageviews, binwidth=1, start=0, end=50, xlabel='pageviews', ylabel='probability density')
    #
    # df_binning = df_collect[df_collect['is_purchased']==0]
    # pageviews = np.array(df_binning)[:,2].astype('int')
    # pd0, center0, fig, ax = binning2(pageviews, binwidth=1, start=0, end=50, xlabel='pageviews', ylabel='probability density')
    #
    # fig, ax = plt.subplots(figsize=(10, 8))
    # ax.bar(center0, pd0, width=1, color="silver", edgecolor="white")
    # ax.bar(center1, pd1, width=1, color="red", edgecolor="white", alpha=0.5)
    # ax.set_xlabel(f'pageviews', fontsize=22)
    # ax.set_ylabel(f'probability density', fontsize=22)
    # # plt.xlim([0, 50])
    # plt.show()


    # web_id = "nineyi123"
    # date_utc8_start = "2022-01-19"
    # date_utc8_end = "2022-01-19"
    # tracking = TrackingParser(web_id, date_utc8_start, date_utc8_end)
    #
    # df_loaded = tracking.df_loaded.sort_values(by=['timestamp'])
    # df_loaded['uuid'] = df_loaded['uuid'].astype(str)
    # df_loaded = df_loaded[df_loaded['uuid'].map(len)==36]
    # df_purchased = tracking.df_purchased
    #
    #
    #
    # uuid_load = list(set(df_loaded['uuid']))
    # uuid_purchased = list(set(df_purchased['uuid']))
    # uuid_load_len = [len(uuid) for uuid in uuid_load]
    # dict_collect_list = []
    # # keys_collect = ['uuid', 'device', 'pageviews', 'time_pageview_total', 'landing_count',
    # #                 'click_count_total', 'max_pageviews', 'max_time_pageview', 'max_time_pageview_total', 'max_scroll_depth']
    # keys_collect = ['uuid', 'pageviews', 'time_pageview_total']
    # for uuid in uuid_load:
    #     dict_collect = {}
    #     df_query = df_loaded.query(f"uuid=='{uuid}'").iloc[-1]
    #     for key in keys_collect:
    #         vlaue = df_query[key]
    #         dict_collect.update({key: vlaue})
    #         if key=='uuid':
    #             if vlaue in uuid_purchased:
    #                 dict_append = {'is_purchased': 1}
    #             else:
    #                 dict_append = {'is_purchased': 0}
    #             dict_collect.update(dict_append)
    #
    #     dict_collect_list += [dict_collect]
    # df_collect = pd.DataFrame(dict_collect_list)



    # X = np.array(df_collect)[:,2:].astype('int')
    # Y = np.array(df_collect)[:,1].astype('int')
    #
    # model = LogisticRegression(random_state=0).fit(X, Y)
    # prob = model.predict_proba(X)
    # predict = model.predict(X)
    # df_collect['prob'] = prob[:,1]
    # df_collect['predict'] = predict
    #
    # model.score(X, Y)



    # df_purchased = tracking.df_purchased
    # df_purchased_unique = df_purchased.drop(columns=['']).drop_duplicates()
    # uuid_purchased = list(set(df_purchased['uuid']))


    # data_list = tracking.data_list
    # data_list_filter = filterListofDictByDict(data_list, dict_criteria={"web_id": web_id, "event_type":"purchase"})


