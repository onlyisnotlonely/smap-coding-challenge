# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import models

class User(models.Model):
    user_id = models.IntegerField(unique=True)
    area = models.TextField(max_length=100)
    tariff = models.TextField(max_length=100)

    def __str__(self):
        return f"{self.user_id}, {self.area}, {self.tariff}"

class Consumption(models.Model):
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    datetime = models.DateTimeField()
    consumption = models.FloatField()

    def __str__(self):
        return f"{self.user}, {self.datetime}, {self.consumption}"
