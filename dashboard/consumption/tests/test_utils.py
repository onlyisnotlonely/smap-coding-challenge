from django.test import TestCase, TransactionTestCase
from consumption.utils import DataLoader, UserValidator, ConsumptionValidator, Aggregater

import os
import pandas as pd


class TestDataLoader(TestCase):
    def setUp(self):
        user_data_path = "consumption/tests/resource/user_data.csv"
        consumption_data_path = "consumption/tests/resource/consumption/"
        file_paths = (user_data_path, consumption_data_path)
        self.dataloader = DataLoader(file_paths=file_paths)
    
    def test_register_consumption(self):
        self.dataloader.register_user()
        self.dataloader.register_consumption()

    def test_load_user_data(self):
        df = self.dataloader.load_user_data()
        self.assertTrue(isinstance(df, pd.DataFrame))

    def test_load_consumption_data(self):
        df = self.dataloader.load_consumption_data()
        self.assertTrue(isinstance(df, pd.DataFrame))

    def test_register_user(self):
        self.assertTrue(self.dataloader.register_user())
    
    def test_register_consumption(self):
        self.dataloader.register_user()
        self.assertTrue(self.dataloader.register_consumption())


class TestUserValidator(TestCase):
    def setUp(self):
        self.valid_file_path = "consumption/tests/resource/validate/user/valid.csv"
        self.invalid_field_path = "consumption/tests/resource/validate/user/invalid_field.csv"
        self.invalid_dtype_path = "consumption/tests/resource/validate/user/invalid_dtype.csv"
    
    def test_invalid_fields(self):
        # 正常系
        validator = UserValidator(file_path=self.valid_file_path)
        self.assertTrue(validator._is_valid_fields())

        # 異常系
        validator = UserValidator(file_path=self.invalid_field_path)
        self.assertFalse(validator._is_valid_fields())
    
    def test_invalid_dtypes(self):
        # 正常系
        validator = UserValidator(file_path=self.valid_file_path)
        self.assertTrue(validator._is_valid_dtypes())

        # 異常系
        validator = UserValidator(file_path=self.invalid_dtype_path)
        self.assertFalse(validator._is_valid_dtypes())
    
    def test_is_valid(self):
        # 正常系
        validator = UserValidator(file_path=self.valid_file_path)
        self.assertTrue(validator.is_valid())

        # 異常系
        validator = UserValidator(file_path=self.invalid_field_path)
        self.assertFalse(validator.is_valid())


class TestConsumptionrValidator(TestCase):
    def setUp(self):
        self.valid_file_path = "consumption/tests/resource/validate/consumption/valid.csv"
        self.invalid_field_path = "consumption/tests/resource/validate/consumption/invalid_field.csv"
        self.invalid_dtype_path = "consumption/tests/resource/validate/consumption/invalid_dtype.csv"

    
    def test_invalid_fields(self):
        # 正常系
        validator = ConsumptionValidator(file_path=self.valid_file_path)
        self.assertTrue(validator._is_valid_fields())

        # 異常系
        validator = ConsumptionValidator(file_path=self.invalid_field_path)
        self.assertFalse(validator._is_valid_fields())
    
    
    def test_invalid_dtypes(self):
        # 正常系
        validator = ConsumptionValidator(file_path=self.valid_file_path)
        self.assertTrue(validator._is_valid_dtypes())        

        # 異常系
        validator = ConsumptionValidator(file_path=self.invalid_dtype_path)
        self.assertFalse(validator._is_valid_dtypes())
    
    def test_is_valid(self):
        # 正常系
        validator = ConsumptionValidator(file_path=self.valid_file_path)
        self.assertTrue(validator.is_valid())

        # 異常系
        validator = ConsumptionValidator(file_path=self.invalid_field_path)
        self.assertFalse(validator.is_valid())


class TestAggregator(TestCase):
    def setUp(self):
        user_data_path = "consumption/tests/resource/user_data.csv"
        consumption_data_path = "consumption/tests/resource/consumption/"
        file_paths = (user_data_path, consumption_data_path)
        self.dataloader = DataLoader(file_paths=file_paths)
        self.dataloader.register_user()
        self.dataloader.register_consumption()
        self.aggregator = Aggregater()
    
    def test_mean(self):
        mean_yearly = self.aggregator.mean("Y")
        mean_monthly = self.aggregator.mean("M")
        mean_weekly = self.aggregator.mean("W")
        mean_daily = self.aggregator.mean("D")

        self.assertTrue(isinstance(mean_yearly, pd.DataFrame))
        self.assertTrue(isinstance(mean_monthly, pd.DataFrame))
        self.assertTrue(isinstance(mean_weekly, pd.DataFrame))
        self.assertTrue(isinstance(mean_daily, pd.DataFrame))

    def test_total(self):
        total_yearly = self.aggregator.total("Y")
        total_monthly = self.aggregator.total("M")
        total_weekly = self.aggregator.total("W")
        total_daily = self.aggregator.total("D")

        self.assertTrue(isinstance(total_yearly, pd.DataFrame))
        self.assertTrue(isinstance(total_monthly, pd.DataFrame))
        self.assertTrue(isinstance(total_weekly, pd.DataFrame))
        self.assertTrue(isinstance(total_daily, pd.DataFrame))
