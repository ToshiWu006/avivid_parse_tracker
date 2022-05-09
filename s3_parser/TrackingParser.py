from basic import datetime_to_str, timing, logging_channels, datetime_range, filterListofDictByDict, to_datetime
from definitions import ROOT_DIR
from db import MySqlHelper
import datetime, os, pickle, json
import pandas as pd
import re
import numpy as np

class TrackingParser:
    def __init__(self, web_id=None, date_utc8_start=None, date_utc8_end=None):
        self.web_id = web_id
        # self.use_db = use_db
        self.event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase']
        self.event_type_coupon_list = ['sendCoupon', 'acceptCoupon', 'discardCoupon']
        self.dict_object_key = {'addCart':'cart', 'removeCart':'remove_cart', 'purchase':'purchase'}
        self.dict_settings = self.fetch_parse_key_settings(web_id)
        self.dict_settings_all = None
        # self.dict_settings_all = self.fetch_parse_key_all_settings()
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

    def get_six_events_df_all(self, data_list=None, use_db=False):
        if data_list==None:
            data_list = self.data_list
        event_type_list = ['load', 'leave', 'timeout', 'addCart', 'removeCart', 'purchase']
        df_list = []
        for event_type in event_type_list:
            df = self.get_df_from_db(event_type) if use_db else self.get_df_all(data_list, event_type)
            df_list += [df]
        df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = df_list
        return df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased

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
        df_loaded = self.get_df_from_db('load') if use_db else self.get_df(web_id, data_list, 'load')
        df_leaved = self.get_df_from_db('leave') if use_db else self.get_df(web_id, data_list, 'leave')
        df_timeout = self.get_df_from_db('timeout') if use_db else self.get_df(web_id, data_list, 'timeout')
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
        df_sendCoupon = self.get_df_from_db('sendCoupon') if use_db else self.get_df(web_id, data_list, 'sendCoupon')
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
                dict_list += self.fully_parse_coupon(data_dict, event_type)
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
            self.reformat_shipping_price(df, col='product_price', inplace=True)
            self.reformat_shipping_price(df, col='total_price', inplace=True)
            self.reformat_shipping_price(df, col='shipping_price', inplace=True)
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='leave' or event_type=='timeout':
            df['max_time_no_scroll_array'] = [','.join([str(i) for i in data]) for data in df['max_time_no_scroll_array']]
            df['max_time_no_scroll_depth_array'] = [','.join([str(i) for i in data]) for data in df['max_time_no_scroll_depth_array']]
        elif event_type=='sendCoupon':
            df.drop(columns=self._get_drop_col('sendCoupon'), inplace=True)
            df['model_keys'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_keys']]
            df['model_parameters'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_parameters']]
            df['model_X'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_X']]

        df.drop_duplicates(subset=self._get_unique_col(event_type), inplace=True)
        df.dropna(inplace=True)
        df = self.clean_before_sql(df, criteria_len={'web_id': 45, 'uuid': 36, 'ga_id': 45, 'fb_id': 45, 'timestamp': 16})
        return df

    @timing
    def get_df_from_db(self, event_type, columns=None):
        if columns==None:
            columns = self._get_df_event_col(event_type)
        table = self._get_event_table(event_type)
        if self.web_id==None:
            query = f"""SELECT {','.join(columns)} FROM {table} WHERE date_time BETWEEN '{self.date_utc8_start}' and '{self.date_utc8_end}' 
                     """
        else:
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

    @logging_channels(['clare_test'])
    def get_df_all(self, data_list, event_type):
        """

        Parameters
        ----------
        data_list: list of dict which to be appended
        event_type: load, leave, addCart, removeCart, purchase, sendCoupon, acceptCoupon, discardCoupon
        dict_settings: settings for addCar, removeCar and purchase events

        Returns: DataFrame, df of all web_id
        -------

        """
        data_list_filter = filterListofDictByDict(data_list,
                                                      dict_criteria={"event_type": event_type})

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
            if self.dict_settings_all == None:
                self.dict_settings_all = self.fetch_parse_key_all_settings()
            for data_dict in data_list_filter:
                dict_list += self.fully_parse_object_all(data_dict, event_type, self.dict_settings_all)
                # dict_list += self.parse_rename_object_all(data_dict, self.dict_settings_all, event_type)
        ## sendCoupon, acceptCoupon, discardCoupon
        elif event_type=='sendCoupon' or event_type=='acceptCoupon' or event_type=='discardCoupon':
            for data_dict in data_list_filter:
                dict_list += self.fully_parse_coupon(data_dict, event_type)
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
            self.reformat_shipping_price(df, col='product_price', inplace=True)
            self.reformat_shipping_price(df, col='total_price', inplace=True)
            self.reformat_shipping_price(df, col='shipping_price', inplace=True)
        elif event_type=='addCart' or event_type=='removeCart' or event_type=='leave' or event_type=='timeout':
            df['max_time_no_scroll_array'] = [','.join([str(i) for i in data]) for data in df['max_time_no_scroll_array']]
            df['max_time_no_scroll_depth_array'] = [','.join([str(i) for i in data]) for data in df['max_time_no_scroll_depth_array']]
        elif event_type=='sendCoupon':
            df.drop(columns=self._get_drop_col('sendCoupon'), inplace=True)
            df['model_keys'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_keys']]
            df['model_parameters'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_parameters']]
            df['model_X'] = [','.join([str(i) for i in data]) if type(data)==list else '_' for data in df['model_X']]
        # df.drop_duplicates(subset=self._get_unique_col(event_type), inplace=True)
        df.dropna(inplace=True)
        df = self.clean_before_sql(df, criteria_len={'web_id': 45, 'uuid': 36, 'ga_id': 45, 'fb_id': 45, 'timestamp': 16})
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

    ## addCart,removeCart,purchased events
    def fully_parse_object_all(self, data_dict, event_type, dict_settings_all):
        # object_key = self.dict_object_key[event_type]
        # key_join_list, key_rename_list = dict_settings[event_type]
        ## 1. parse common terms
        universial_dict = self.parse_rename_universial(data_dict)
        ## 2. parse record_user terms
        record_dict = self.parse_rename_record_user(data_dict)
        ## 3. parse cart, remove_cart or purchase terms
        object_dict_list = self.parse_rename_object_all(data_dict, dict_settings_all, event_type)
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
    def fully_parse_coupon(data_dict, event_type):
        universial_dict = TrackingParser.parse_rename_universial(data_dict)
        coupon_info_dict = TrackingParser.parse_rename_coupon_info(data_dict, event_type)
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
        if len(key_join_list[0].split('.'))>1:
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                dict_object = list(dict_object.values())[0]
        else:
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                print(f"{key_join_list[0].split('.')[0]} not in {dict_object}, return []")
                return []

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
                for k in key_list:  ## append -1 if key not found
                    temp = dict_object[k] if k in dict_object.keys() else -1
                    if type(temp) == str or type(temp) == int:
                        collection_dict.update({key_rename: temp})
                    elif temp is None:
                        collection_dict.update({key_rename: -1})
                    elif type(temp) == list:
                        value_list = temp
                        n_list = len(temp)
                # for k in key_list: ## append -1 if key not found
                #     collection_dict.update({key_rename: dict_object[k]}) if k in dict_object.keys() else collection_dict.update({key_rename: -1})
            else:  ## parse multiple layer
                for key_2nd in key_list:
                    if value == '':  ## 1st level
                        value = '_' if key_2nd == 'empty' else dict_object[key_2nd]
                    elif key_2nd=='json': ## use json.loads() => i3fresh case
                        ## may not json type
                        value = json.loads(value)

                    elif type(value) == dict:  ## 2nd, 3rd... level
                        value = '_' if key_2nd == 'empty' else value[key_2nd]
                        if value is None:
                            collection_dict.update({key_rename: -1})
                        else:
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
                        v = value_list[n_list * j + i]
                        if v is None:
                            temp_dict.update({key_rename_list[n_dict_key + j]: -1})
                        else:
                            temp_dict.update({key_rename_list[n_dict_key + j]: v})
                    temp_dict.update(collection_dict)
                    collection_purchase_dict_list += [temp_dict]
            else:
                collection_purchase_dict_list = [collection_dict]
        return collection_purchase_dict_list

    ## main for parse and rename 'addcart', 'removeCart', 'purchase' event
    @staticmethod
    def parse_rename_object_all(data_dict, dict_settings_all, event_type):
        dict_object_key = {'addCart': 'cart', 'removeCart': 'remove_cart', 'purchase': 'purchase'}
        object_key = dict_object_key[event_type]
        if 'web_id' not in data_dict.keys():
            print(f"web_id not in dict (not a valid format), return []")
            return []
        web_id = data_dict['web_id']
        if web_id not in dict_settings_all.keys():
            print(f"{web_id} not in dict_settings_all( do not analyze), return []")
            return []
        key_join_list, key_rename_list = dict_settings_all[web_id][event_type]
        if object_key not in data_dict.keys():
            print(f"{object_key} not in {data_dict}, return []")
            return []
        collection_dict, dict_object = {}, json.loads(data_dict[object_key])
        if len(key_join_list[0].split('.')) > 1:
            ## for dealing with adding 'purchase' key in purchase (91app, lovingfamily)
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                dict_object = list(dict_object.values())[0]
        else:
            if key_join_list[0].split('.')[0] not in dict_object.keys():
                print(f"{key_join_list[0].split('.')[0]} not in {dict_object}, return []")
                return []
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
                for k in key_list:  ## append -1 if key not found
                    temp = dict_object[k] if k in dict_object.keys() else -1
                    if type(temp) == str or type(temp) == int:
                        collection_dict.update({key_rename: temp})
                    elif temp is None:
                        collection_dict.update({key_rename: -1})
                    elif type(temp) == list:
                        value_list = temp
                        n_list = len(temp)
                # for k in key_list: ## append -1 if key not found
                #     collection_dict.update({key_rename: dict_object[k]}) if k in dict_object.keys() else collection_dict.update({key_rename: -1})
            else:  ## parse multiple layer
                for key_2nd in key_list:
                    if value == '':  ## 1st level
                        value = '_' if key_2nd == 'empty' or key_2nd not in dict_object.keys() else dict_object[key_2nd]
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
    def parse_rename_coupon_info(data_dict, event_type):
        if 'coupon_info' not in data_dict.keys():
            print(f"'coupon_info' not in {data_dict}, return []")
            return []
        dict_object = data_dict['coupon_info']
        if event_type=='sendCoupon':
            key_list = ['l_b', 'u_b', 'm_k', 'm_p', 'm_i',
                        'm_X', 'c_i', 'c_c_t', 'p_p']
            key_rename_list = ['lower_bound', 'upper_bound', 'model_keys', 'model_parameters', 'model_intercept',
                               'model_X', 'coupon_id', 'coupon_customer_type', 'prob_purchase']
        else:
            key_list = ['p_p', 'c_t', 'c_d', 'c_c', 'c_st',
                        'c_ty', 'c_a', 'c_c_t', 'c_c_m', 'l_c',
                        'c_i']
            key_rename_list = ['prob_purchase', 'coupon_title', 'coupon_description', 'coupon_code', 'coupon_setTimer',
                               'coupon_type', 'coupon_amount', 'coupon_customer_type', 'coupon_code_mode', 'link_code',
                               'coupon_id']
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
    @staticmethod
    @timing
    def fetch_parse_key_all_settings():
        query = f"""SELECT web_id, parsed_purchase_key, parsed_purchase_key_rename, 
                            parsed_addCart_key, parsed_addCart_key_rename,
                            parsed_removeCart_key, parsed_removeCart_key_rename
                            FROM cdp_tracking_settings where enable_analysis=1"""
        print(query)
        data = MySqlHelper("rheacache-db0").ExecuteSelect(query)
        dict_settings_all = {}
        for d in data:
            web_id = d[0]
            settings = [x.split(',') for x in d[1:]]
            dict_settings = {}
            for i, event_type in enumerate(['purchase', 'addCart', 'removeCart']):
                dict_settings.update({event_type: settings[i * 2:(i + 1) * 2]})
            dict_settings_all.update({web_id:dict_settings})
        return dict_settings_all

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
    def get_file_byHour(date, hour, utc=0, pattern='%Y-%m-%d'):
        """

        Parameters
        ----------
        date: with format, str:'2022-01-01'(default pattern) or str:'2022/01/01'
        hour: with format, int:0-23
        pattern: parse date, str: '%Y-%m-%d' or '%Y/%m/%d'

        Returns: data_list
        -------

        """
        path = TrackingParser.get_date_hour_filepath(date, hour, utc, pattern)

        if os.path.isfile(path):
            with open(path, 'rb') as f:
                data_list = pickle.load(f)
        else:
            data_list = []
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
    def reformat_shipping_price(df, col='shipping_price', inplace=False):
        if col in df.columns:
            if inplace:
                # df[col] = [0 if re.findall("[0-9]", str(x)) == [] else int(float(str(x).replace(',',''))) for x in df[col]]
                df[col] = [0 if re.findall("[0-9]", str(x)) == [] else int(float(''.join(re.findall("[0-9.]", str(x))))) for x in df[col]]

            else:
                df_copy = df.copy()
                # df_copy[col] = [0 if re.findall("[0-9]", str(x)) == [] else int(float(str(x).replace(',',''))) for x in df[col]]
                df_copy[col] = [0 if re.findall("[0-9]", str(x)) == [] else int(float(''.join(re.findall("[0-9.]", str(x))))) for x in df[col]]

                return df_copy
        else:
            return df


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
    def get_date_hour_filepath(date, hour, utc=8, pattern="%Y-%m-%d"):
        if type(date) == datetime.datetime:
            datetime_utc0 = date + datetime.timedelta(hours=-utc+hour)
        else:
            datetime_utc0 = datetime.datetime.strptime(date, pattern) + datetime.timedelta(hours=-utc+hour)
        path = os.path.join(ROOT_DIR, "s3data", datetime_to_str(datetime_utc0, pattern="%Y/%m/%d/%H"), "rawData.pickle")
        return path


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
            columns = ['product_id', 'product_name', 'product_price', 'product_quantity',
                       'product_category', 'product_category_name', 'product_variant',
                       'sku_id', 'sku_name', 'currency',
                       'web_id', 'uuid', 'ga_id', 'fb_id',
                       'timestamp', 'avivid_coupon', 'device', 'url_last',
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
            columns = ['product_id', 'product_name', 'product_price', 'product_quantity',
                       'product_category', 'product_category_name', 'product_variant',
                       'sku_id', 'sku_name', 'currency',
                       'web_id', 'uuid', 'ga_id', 'fb_id',
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
                       'product_category', 'product_category_name', 'product_variant',
                       'coupon', 'currency', 'order_id', 'total_price', 'shipping_price',
                       'web_id', 'uuid', 'ga_id', 'fb_id',
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
        elif event_type=='sendCoupon':
            drop_col_list = ['time_pageview','scroll_depth','scroll_depth_px','click_count', 'time_no_move',
                             'time_no_scroll','time_no_click','max_time_no_move','max_time_no_scroll', 'max_time_no_scroll_array',
                             'max_time_no_scroll_depth_array','max_time_no_scroll_depth','max_time_no_scroll_depth_px', 'max_time_no_click',
                             'max_scroll_depth_page','time_pageview_total_last']
        return drop_col_list

    ## for df.drop_duplicates(subset), can be more than unique key in sql table
    @staticmethod
    def _get_unique_col(event_type):
        if event_type=='load':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        elif event_type=='purchase':
            subset = ['date_time', 'web_id', 'uuid', 'product_id',
                      'product_variant', 'product_category', 'product_price', 'product_name']
            # subset = ['date_time', 'web_id', 'uuid', 'product_id', 'product_name']
        elif event_type == 'addCart':
            # subset = ['date_time', 'web_id', 'uuid', 'product_id', 'product_price',
            #           'product_quantity', 'product_category']
            subset = ['date_time', 'web_id', 'uuid', 'product_id']
        elif event_type=='removeCart':
            # subset = ['date_time', 'web_id', 'uuid', 'product_id', 'sku_id']
            subset = ['date_time', 'web_id', 'uuid', 'product_id']
        elif event_type=='leave':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        elif event_type == 'timeout':
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        else:
            subset = ['date_time', 'web_id', 'uuid', 'session_id']
        return subset


if __name__ == "__main__":
    web_id = "i3fresh" # chingtse, kava, draimior, magiplanet, i3fresh, wstyle, blueseeds, menustudy
    # # lovingfamily, millerpopcorn, blueseeds, hidesan, washcan, hito, fmshoes, lzl, ego, up2you
    # # fuigo, deliverfresh
    date_utc8_start = "2022-05-09"
    date_utc8_end = "2022-05-09"
    tracking = TrackingParser(web_id, date_utc8_start, date_utc8_end)
    data_list = tracking.data_list
    # order,amount,ship,order_coupon.json.total,bitem.json.itemid,bitem.json.empty,bitem.json.price,bitem.json.count,bitem.json.empty,bitem.json.empty,bitem.json.empty,bitem.json.empty
    # # # event_type = "acceptCoupon"
    data_list_filter = filterListofDictByDict(data_list, dict_criteria={"web_id": web_id, "event_type":'purchase'})
    # # data_list_filter = filterListofDictByDict(data_list, dict_criteria={"web_id": web_id})
    df = tracking.get_df(web_id, data_list_filter, 'purchase')



    df_all = TrackingParser().get_df_all(filterListofDictByDict(data_list, dict_criteria={"web_id": web_id, "event_type":'purchase'}),
                                         'purchase')
    #
    #
    # data_list_filter = filterListofDictByDict(data_list, dict_criteria={"event_type":'purchase'})
    # dict_settings_all = TrackingParser.fetch_parse_key_all_settings()
    # for data_dict in data_list_filter:
    #     object_dict_list = TrackingParser.parse_rename_object_all(data_dict, dict_settings_all, 'purchase')
    # web_id_all = list(set([data['web_id'] for data in data_list_filter]))
    # x = []
    # for web_id in web_id_all:
    #     df = tracking.get_df(web_id, data_list_filter, 'purchase')
    #     x += [df.shape]
    #     # df = TrackingParser().get_df_all(data_list_filter, 'purchase')
    #     print(f"web_id: {web_id}, size:{df.shape[0]}")
    # args = TrackingParser(None, date_utc8_start, date_utc8_end).get_six_events_df_all(use_db=False)
    # df_loaded, df_leaved, df_timeout, df_addCart, df_removeCart, df_purchased = TrackingParser(None, date_utc8, date_utc8).get_six_events_df_all(use_db=False)

    # file = TrackingParser.get_date_hour_file('2022-04-19', 10)
    # data = TrackingParser.get_file_byHour('2022-04-20', 13, utc=8)