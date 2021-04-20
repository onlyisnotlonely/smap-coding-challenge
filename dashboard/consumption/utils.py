from django_pandas.io import read_frame
from consumption.models import User, Consumption

import numpy as np
import pandas as pd

import os
import sys
import configparser
import csv
import glob
import logging
from multiprocessing import Pool
from typing import List

import datetime
from datetime import datetime as dt

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self, file_paths=None):
        self.user_data_path = file_paths[0]
        self.consumption_data_path = file_paths[1]

    def load_user_data(self) -> pd.DataFrame:
        print("start loading user_data.")
        df = pd.read_csv(self.user_data_path)
        return df

    def load_consumption_data(self) -> pd.DataFrame:
        print("start loading consumption_data.")
        file_names = sorted(glob.glob(self.consumption_data_path + "/*csv"))

        # リスト内包表記より分散処理が早い
        # cpu_count:8の場合、ユーザー数10000件（8160行/件）で150〜190s
        p = Pool(os.cpu_count())
        df = pd.concat(p.map(self.add_user_id_to_df, file_names))
        p.close()
        return df

    def register_user(self):
        print("start register_user.")
        df = self.load_user_data()
        user_ids = df["id"].values
        areas = df["area"].values
        tariffs = df["tariff"].values

        p = Pool(os.cpu_count())
        objs = p.map(self.create_user_obj, zip(user_ids, areas, tariffs))
        p.close()
        User.objects.bulk_create(objs)
        print("register_user completed.")

        return True

    def register_consumption(self):
        print("start register_consumption.")
        df = self.load_consumption_data()
        print("downloading consumption data was completed.")

        user_ids = df["id"].values
        datetimes = df["datetime"].values
        consumptions = df["consumption"].values
        
        print("start creating register_consumption objs.")
        p = Pool(os.cpu_count())
        objs = p.map(self.create_consumption_obj, zip(user_ids, datetimes, consumptions))
        p.close()
        Consumption.objects.bulk_create(objs)
        print("register_consumption completed.")
        
        return True

    def add_user_id_to_df(self, f_name: str):
        user_id = f_name.replace(self.consumption_data_path, "").replace(".csv", "")
        df = pd.read_csv(f_name)
        df["id"] = int(user_id)
        return df

    def create_user_obj(self, *args):
        return User(user_id=args[0][0], area=args[0][1], tariff=args[0][2])        

    def create_consumption_obj(self, *args):
        return Consumption(user=User.objects.get(user_id=args[0][0]),
                           datetime=args[0][1],
                           consumption=args[0][2])

class Validator:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = pd.read_csv(self.file_path)    
        self.fields = None
        self.dtypes = None
        self.invalids = {"invalid_field":[], "invalid_dtype":[]}
    
    def _is_valid_fields(self):
        headers = self.df.columns.values
        for header, field in zip(headers, self.fields):
            if header != field:
                self.invalids["invalid_field"].append(f"{header}")
        if len(self.invalids["invalid_field"]) == 0:
            return True
        else:
            return False
       
    def _is_valid_dtypes(self):
        for row in range(len(self.df)):
            for col in range(len(self.fields)):
                dtypes = self._convert_to_dtypes(self.df.iloc[row].values)
                if dtypes[col] != self.dtypes[col]:
                    self.invalids["invalid_dtype"].append(self.fields[col] + "_" + str(row))
                
        if len(self.invalids["invalid_dtype"]) == 0:
            return True
        else:
            return False

    def is_valid(self):
        if self._is_valid_fields() and self._is_valid_dtypes():
            return True
        else:
            print(f"{self.file_path} is invalid.")
            print(f"invalids:{self.invalids}")
            return False
        
    def _convert_to_dtypes(self, values: List):
        return [type(value) for value in values]

class UserValidator(Validator):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.fields = ["id", "area", "tariff"]
        self.dtypes = [np.int64, str, str]

class ConsumptionValidator(Validator):
    def __init__(self, file_path: str):
        super().__init__(file_path)
        self.fields = ["datetime", "consumption"]
        self.dtypes = [datetime.datetime, np.float64]
    
    def _convert_to_dtypes(self, values: List):
        if type(values[0]) == str:
            try:
                return [type(dt.strptime(values[0], '%Y-%m-%d %H:%M:%S')), type(values[1])]
            except ValueError:
                print(f"error.")
        else:
            return [type(value) for value in values]

class Aggregater:
    def __init__(self):
        self.consumption_data = Consumption.objects.all()
        self.df = read_frame(self.consumption_data)
        self._to_datetime()

    def _to_datetime(self):
        self.df["datetime"] = pd.to_datetime(self.df["datetime"])
        self.df.set_index("datetime", inplace=True)        

    def total(self, terms="30T"):
        total = self.df.resample(terms)["consumption"].sum()
        return pd.DataFrame(total)

    def mean(self, terms="30T"):        
        mean = self.df.resample(terms)["consumption"].mean()
        return pd.DataFrame(mean)
