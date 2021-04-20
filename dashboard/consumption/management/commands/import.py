from django.core.management.base import BaseCommand
from consumption.utils import DataLoader, UserValidator, ConsumptionValidator

import os
import glob
import configparser

config_ini = configparser.ConfigParser()
config_ini.read("config.ini", encoding="utf-8")
user_data_path = config_ini.get("PATH_SETTINGS", "USER_DATA")
consumption_data_path = config_ini.get("PATH_SETTINGS", "CONSUMPTION_DATA")
file_paths = (user_data_path, consumption_data_path)

class Command(BaseCommand):
    help = 'import data'

    def handle(self, *args, **options):
        is_valid = True
        user_validator = UserValidator(user_data_path)
        consumption_files = glob.glob(consumption_data_path + "/*.csv")
        for consumption_file in consumption_files:
            consumption_validator = ConsumptionValidator(consumption_file)
            if not consumption_validator.is_valid():
                is_valid = False
        
        if is_valid:
            print(f"All data is valid.")
            dataloader = DataLoader(file_paths=file_paths)
            dataloader.register_user()
            dataloader.register_consumption()
