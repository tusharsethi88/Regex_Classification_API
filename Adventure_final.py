#!/usr/bin/env python
# coding: utf-8

# In[64]:


#Importing the required libraries
import pyspark
import itertools
import pyspark
import numpy as np
import pandas as pd
import numpy as np
import re
from pyspark.sql import functions as sf
from pyspark.sql.session import SparkSession
from pyspark.sql.types import Row

#Creating Spark session
spark = SparkSession.builder.config("spark.driver.extraClassPath","mssql-jdbc-6.4.0.jre8.jar").appName("Adventure_Works").getOrCreate()

#Function to create multiple SQL statements and its output in list
def sql_ouput():
    url = "jdbc:sqlserver://localhost:1433;databaseName=AdventureWorks2017"
    user = "sa"
    password = "Rahsut@160288"
    schema = spark.read.format("jdbc").option("url", url).option("query", "select schema_name(tab.schema_id) as schema_name, tab.name as table_name, col.column_id, col.name as column_name, t.name as data_type from sys.tables as tab inner join sys.columns as col on tab.object_id = col.object_id left join sys.types as t on col.user_type_id = t.user_type_id  where t.name not in ('bigint', 'int', 'smallint', 'tinyint', 'decimal', 'numeric','smallmoney', 'money', 'bit', 'float', 'real', 'xml', 'hierarchyid')").option("user", user).option("password", password).load()
    final_clean_schema=schema.withColumn('schema_table', sf.concat(sf.col('schema_name'),sf.lit('.'), sf.col('table_name'))).distinct()
    final_clean_schema_req=final_clean_schema.select('schema_table', 'column_name')

    list_1 = final_clean_schema_req.rdd.map(lambda row: {row[0]: row[1]}).collect()
    list_2 = []
    for i in range(len(list_1)):
        for idx, (x,y) in enumerate(zip(list_1[i].keys(),list_1[i].values())):
            sql="SELECT top(100) {} from {} where {} is not null order by newid()".format(y,x,y)
            schema = spark.read.format("jdbc").option("url", url).option("query", sql).option("user", user).option("password", password).load()
            list_2.append(schema.collect())
    return list_2



def match(zx):
    final_list=sql_ouput()
    reg=pd.read_csv("reg.csv")
    r1=reg['Regex']#.select('Regex').collect()
    r2=reg['PI Subcategory(with specific references)']#.select('PI Subcategory(with specific references)').collect()
    r3=reg['Personal Information Category']#.select('Personal Information Category').collect()

    probs_dict=probs_dict1= {} #dictionary to have Key(column name) and values(probablity score)
    out=[]
    #matching regex
    for col_name in zx:

        g=[] 
        for i in range(len(col_name)):
            g.append(col_name[i])
        g.append('prob')  
        df=pd.DataFrame(index=g)

        for regex,subcat,cat in zip(enumerate(r1),enumerate(r2),enumerate(r3)):
            #print(subcat)

            for array1 in range(len(g)-1):
                #print(str(g[array1][0]))
                if re.match(regex[1],str(g[array1][0])):
                    out.append(1)
                else:
                    out.append(0)
            out.append(sum(out)/(len(g)-1))
            df[regex[1],subcat[1],cat[1]]=out
            if out[-1]>0:
                probs_dict.update({cat[1]:out[-1]})
                probs_dict1.update({subcat[1]:out[-1]})
            out=[]

        df.to_csv('out.csv')
        return(probs_dict)


# In[39]:





# In[ ]:




