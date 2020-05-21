# Word2vec
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
import pyspark
import itertools
import pyspark
import numpy as np
import pandas as pd
import re
from pyspark.sql import functions as sf
from flask import Flask, render_template,request,url_for, jsonify
from Adventure_final import match, sql_ouput

spark = SparkSession.builder.config("spark.driver.extraClassPath","/home/tushar/Desktop/Adventure_work/mssql-jdbc-6.4.0.jre8.jar").appName("Adventure_Works").getOrCreate()

# Flask App Name
flaskAppInstance = Flask(__name__)
 
@flaskAppInstance.route('/regexmatch',methods=['GET', 'POST'])
def regexmatch():
	zx=sql_ouput()
	final_dict=match(zx)
  	if request.method == "GET":
  		return jsonify({"response": "Get Request Called"})
  	elif request.method == "POST":
  		req_Json = request.json

	    # boolp = req_Json['boolp']
	return final_dict


if __name__ == '__main__':
    flaskAppInstance.run(host="0.0.0.0",port=5000,debug=True,use_reloader=True)
