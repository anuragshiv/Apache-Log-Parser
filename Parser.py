#Pyspark script to read and parse Apache Logs
# Code Developed by Anurag Shivaprasad  
# Last Modified: 07/29/2016

#---------------PATH TO HADOOP DATA -------->>>

ApacheLog = "<enter filepath>"

#---------------IMPORT MODULES ------------->>>

import re
from pyspark.sql import Row
from pyspark.sql import SQLContext
from pyspark.sql.types import *

sqlContext = SQLContext(sc) 
sqlContext.setConf("spark.sql.tungsten.enabled", "false")

#-----REGULAR EXPRESSION FOR BIRF LOGS------>>>
logRE = '^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+)?( \S+)?" (\S+) (\S+) "(.*?)" "(.*?) "(.*?)"?$'

#--------------HELPER FUNCTIONS------------->>>

#Matches line to the provided REGEX to filter lines that do not match the REGEX - To address logs that were either incomplete or broken.
def matchLog(line):
    match = re.search(logRE,line)

    if match is None:
        return False
   
	else:    
    	return True

#Parses each log into a Row Object, much like a JSON Object which together can be abstracted to a DataFrame

def parseLog(line):
    match = re.search(logRE,line)
    
    if match is None:
        raise ValueError("Invalid line: %s" % line)
    #To resolve spurious regex anomalies that caused additional content to be included as part of the cookie
    return Row(ip = match.group(1),cookie = match.group(12).split('"')[0],dateTime = match.group(4),method = match.group(5),endpoint = match.group(6),protocol = match.group(7),responseCode = int(match.group(8)),contentSize = match.group(9))

#--------------START OF EXECUTION------------->>>

logs = sc.textFile(ApacheLog)
logs = logs.filter(matchLog) #Call to matchLog : filter in only those rows that reach True condition in matchLog
logs = logs.map(parseLog) #Call to parseLog : Map each line in logs to a Row Object of its Content

logDF = sqlContext.createDataFrame(logs)
logDF = logDF.dropna()  #Drops any rows that contain NaN values, in case some rows were incorrectly parsed.

#-------ADDITIONAL INFORMATION---------------->>>
'''
Use the following documentation for help on methods available to query and manipulate dataframes:
https://spark.apache.org/docs/latest/api/python/pyspark.sql.html

The same code maybe refactored into Scala.
