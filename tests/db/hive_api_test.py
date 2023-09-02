#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import mysql.connector
import time
db = mysql.connector.connect(host='222.20.95.42',user='root', password='123qweASD!@#', database='mooc_test', charset='utf8', port=3307)


db.close()
db.cursor()

