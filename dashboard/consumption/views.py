# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.shortcuts import render
from consumption.models import User, Consumption
from consumption.utils import Aggregater

import plotly.graph_objects as go
import pandas as pd

def summary(request):
    aggregater = Aggregater()

    graphs = {"Y": None, "M": None, "W": None, "D": None}
    graph_titles = ["Annual Trends", "Montly Trends", "Weekely Trends", "Daily Trends"]
    dfs = {"Y": None, "M": None, "W": None, "D": None}
    
    for key, graph_title in zip(graphs.keys(), graph_titles):
        df = pd.DataFrame(aggregater.total(key))
        dfs[key] = df
        layout = go.Layout(title=graph_title, xaxis={'title': 'datetime'}, yaxis={'title': 'consumption'})
        fig = go.Figure(layout=layout)
        fig.add_trace(go.Scatter(
            x=list(df.index),
            y=df["consumption"].values,
            mode="lines",
            name="consumption",
        ))
        graph = fig.to_html(fig, include_plotlyjs=False)
        graphs[key] = graph

    context = {
        'message': 'Summary of consumption.',
        'graph_monthly': graphs["M"],
        'graph_weekly': graphs["W"],
        'graph_daily': graphs["D"],
        'table_title': "Weekly trends",
        'table': dfs["W"].to_html(),
    }
    
    return render(request, 'consumption/summary.html', context)
    
def detail(request):
    context = {
    }
    return render(request, 'consumption/detail.html', context)
