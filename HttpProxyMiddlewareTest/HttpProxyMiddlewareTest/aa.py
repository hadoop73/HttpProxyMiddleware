# coding:utf-8


import re
import json

import logging

from datetime import datetime, date, timedelta

a = datetime(2012, 9, 23)
z = (a + timedelta(days=1))
nice_z = datetime.strftime(z, '%Y-%m-%d')
print nice_z


import random

print random.random()

import time
a = int(round(time.time()))
s = str(a)+ str(random.randint(100,900))
print s

a = '{"a":123}'
b = json.loads(a)
print "a" in b
