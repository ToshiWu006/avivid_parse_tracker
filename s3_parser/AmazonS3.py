import boto3, json, os
from definitions import ROOT_DIR
from basic import datetime_to_str, get_today, filterListofDict, filterListofDictByList, filterListofDictByDict, timing, to_datetime
import datetime
import pickle
from pathlib import Path

class AmazonS3 :
    def __init__(self, bucket_name='elephants3', settings_filename='s3_settings.json'):
        self.bucket_name = bucket_name
        self.settings = self._LoadConfig(settings_filename)
        aws_connector = boto3.Session(
                                  aws_access_key_id=self.settings["access_key"],
                                  aws_secret_access_key=self.settings["access_secret"],
                                  region_name=self.settings["region_name"])
        s3 = aws_connector.resource("s3")
        self._bucket = s3.Bucket(bucket_name)

    def dumpDateDataFilter(self, date, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d"):
        data_list_filter = self.getDateDataFilter(date, dict_criteria)
        root_folder = datetime_to_str(to_datetime(date, pattern), pattern="%Y/%m/%d")
        path_folder = os.path.join(ROOT_DIR, "s3data", root_folder)
        filename = "rawData.pickle"
        # path_write = f"{ROOT_DIR}/s3data/{root_folder}/rawData.pickle"
        self.PickleDump(data_list_filter, path_folder, filename)
        return data_list_filter

    def dumpDateHourDataFilter(self, date, hour, dict_criteria={'event_type': None,'web_id': None}, pattern="%Y-%m-%d"):
        data_list_filter = self.getDateHourDataFilter(date, hour, dict_criteria)
        root_folder = datetime_to_str(to_datetime(date, pattern), pattern="%Y/%m/%d")
        sub_folder = datetime_to_str(to_datetime(f'{date}-{hour}', "%Y-%m-%d-%H"), '%H')
        path_folder = os.path.join(ROOT_DIR, "s3data", root_folder, sub_folder)
        filename = "rawData.pickle"
        # path_write = f"{ROOT_DIR}/s3data/{root_folder}/{sub_folder}/rawHourData.pickle"
        self.PickleDump(data_list_filter, path_folder, filename)
        return data_list_filter

    def getDateDataFilter(self, date, dict_criteria={'event_type': None,'web_id': None}):
        data_list_filter = []
        objects = self.getDateObjects(date)
        for i,object in enumerate(objects):
            path_object = object.key
            # print(path_object)  ##path of each Object
            data_list = json.loads(self.Read(path_object))
            if i%100==0:
                print(f"finish loading number of objects, {i}")
            data_list_filter += filterListofDictByDict(data_list, dict_criteria=dict_criteria)
        return data_list_filter

    def getDateHourDataFilter(self, date, hour, dict_criteria={'event_type': None,'web_id': None}):
        data_list_filter = []
        objects = self.getDateHourObjects(date, hour)
        for object in objects:
            path_object = object.key
            print(path_object)  ##path of each Object
            data_list = json.loads(self.Read(path_object))
            print(f"finish loading {path_object}")
            data_list_filter += filterListofDictByDict(data_list, dict_criteria=dict_criteria)
        return data_list_filter

    def getDateObjects(self, date):
        ## precision to hour
        date = datetime.datetime.strptime(date, '%Y-%m-%d')
        date = datetime_to_str(date, pattern="%Y/%m/%d")
        path_bucket = f"{date}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    def getDateHourObjects(self, date, hour):
        ## precision to hour
        date_hour = datetime.datetime.strptime(f"{date} {hour}", '%Y-%m-%d %H')
        datetime_hour = datetime_to_str(date_hour, pattern="%Y/%m/%d/%H")
        path_bucket = f"{datetime_hour}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    def getLatestHourObjects(self):
        ## precision to hour
        datetime_latest = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y/%m/%d/%H")
        path_bucket = f"{datetime_latest}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    ## quick test for unit test
    def getLatestMinObjects(self):
        ## precision to minute
        datetime_latest = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y/%m/%d/%H")
        prefix = "elephanthorse-2"
        datetime_min_latest = datetime_to_str(datetime.datetime.utcnow()-datetime.timedelta(minutes=1), pattern="%Y-%m-%d-%H-%M")
        path_bucket = f"{datetime_latest}/{prefix}-{datetime_min_latest}"
        return self._bucket.objects.filter(Prefix=path_bucket)

    @staticmethod
    def _CountObejects(Objects):
        counter = 0
        for object in Objects:
            if object.size != 0:
                counter += 1
        return counter

    def ListObjects(self):
        """ A function that listing all files in bucket """
        return self._bucket.objects.all()

    def ListSpecificObjects(self,path):
        """ A function that listing specific files in bucket """
        return self._bucket.objects.filter(Prefix=path)

    def PickleDump(self, data_list, path_foler, filename):

        Path(path_foler).mkdir(parents=True, exist_ok=True)
        path_write = os.path.join(path_foler, filename)
        with open(path_write, 'wb') as f:
            pickle.dump(data_list, f)

    def PickleLoad(self, path_read):
        with open(path_read, 'rb') as f:
            data_list = pickle.load(f)
        return data_list

    def JsonLoad(self, key):
        """ Reading the S3 object from bucket after json decode """
        try :
            return json.load(self._bucket.Object(key=key).get()["Body"])
        except :
            return False

    def JsonDump(self, key, obj):
        """ Writing the S3 object from bucket after json encode """
        try :
            return self._bucket.Object(key=key).put(Body=json.dumps(obj))
        except :
            return False

    def Read(self, key):
        """ Reading the S3 object from bucket """
        try :
            return '['+self._bucket.Object(key=key).get()["Body"].read().decode().replace('}{','},{')+']'
        except :
            return False

    def Dump(self, key, obj):
        """ Writing the S3 object from bucket """
        try :
            return self._bucket.Object(key=key).put(Body=obj)
        except :
            return False

    def _LoadConfig(self, settings_filename):
        self.settings_path = os.path.join(ROOT_DIR, 's3_parser', settings_filename)
        with open(self.settings_path) as settings_file:
            settings = json.load(settings_file)
            return settings

## unit test
if __name__ == "__main__":

    # with open('data_list.pickle', 'wb') as f:
    #     pickle.dump(data_list, f)



    with open('data_list.pickle', 'rb') as f:
        data_list = pickle.load(f)

    key_filter = 'event_type'
    values_filter = 'load'
    data_list_filter = filterListofDict(data_list, key='event_type', value='load')
    data_list_filter2 = filterListofDict(data_list_filter, key='web_id', value='nineyi11')
    data_list_filter3 = filterListofDictByList(data_list_filter, key_list=['web_id'], value_list=['nineyi11'])
    data_list_filter4 = filterListofDictByDict(data_list, dict_criteria={'event_type': 'load','web_id': 'nineyi11'})
    data_list_filter5 = filterListofDictByDict(data_list, dict_criteria={'event_type': None,'web_id': None})






    #
    #
    # elephants3 = AmazonS3('elephants3')
    # key_filter = 'event_type'
    # values_filter = 'load'
    # # objects = elephants3.getLatestObjects()
    # objects = elephants3.getLatestMinObjects()
    # n = AmazonS3._CountObejects(objects)
    # print(f"number of objects: {n}")
    # data_dict, i = {}, 0
    # data_list_filter = []
    # for object in objects:
    #     path_object = object.key
    #     print(path_object) ##path of each Object
    #     data_list = json.loads(elephants3.Read(path_object))
    #     print(f"finish loading {path_object}")
    #     data_list_filter += list(filter(lambda x: key_filter in x.keys(), data_list))
    #     data_list_filter = list(filter(lambda x: values_filter in x[key_filter], filter(lambda x: key_filter in x.keys(), data_list)))
    #     data_list_filter = filterListofDict(data_list, key=key_filter, value=values_filter)
        # for data in json.loads(elephants3.Read(path_object)):
        #     # content = json.loads(data)
        #     data_filter = list(filter(lambda x: x))
        #     if key_filter in data.keys():
        #
        #         data_dict[i] = data
        #         i += 1
                # print(data)
    # datetime_latest = datetime_to_str(datetime.datetime.utcnow(), pattern="%Y/%m/%d/%H")
