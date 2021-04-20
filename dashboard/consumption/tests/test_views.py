from django.test import TestCase, TransactionTestCase
from django.test.client import RequestFactory
from consumption.views import summary, detail
from consumption.utils import DataLoader

class TestViews(TestCase):
    def setUp(self):
        user_data_path = "consumption/tests/resource/user_data.csv"
        consumption_data_path = "consumption/tests/resource/consumption/"
        self.file_paths = (user_data_path, consumption_data_path)
        self.dataloader = DataLoader(file_paths=self.file_paths)
    
    
    def test_summary(self):
        self.dataloader.register_user()
        self.dataloader.register_consumption()
        rf = RequestFactory()
        request = rf.get("/summary/")
        respnose = summary(request)
        