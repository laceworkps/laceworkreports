#!/usr/bin/env python3

import datetime

import datemath
from jinja2 import Environment, FileSystemLoader

loader = FileSystemLoader(searchpath="templates")
env = Environment(loader=loader)
t = env.get_template("template.j2")
print(
    t.render(
        datetime=datetime.datetime, timedelta=datetime.timedelta, datemath=datemath.dm
    )
)
