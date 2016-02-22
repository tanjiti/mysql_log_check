# -*- coding: utf-8 -*-
import sys
import json
import re
import logging
import base64

#db log format configure
db_pattern = r"^(\d{2}\d{2}\d{2}\s+\d{1,2}:\d{2}:\d{2}|\t)\s+\d+\s+([A-Za-z]+)\s*(.*)$"
db_regex = re.compile(db_pattern)

sql_pattern = r"^(\S+)\s"
sql_regex = re.compile(sql_pattern)

#log configure
logging.basicConfig(level = logging.ERROR,
                    format = '%(message)s',                    
                    stream = sys.stderr)


#query blacklist configure
command_blacklist = [
"query"
]

query_blacklist = [
"select",
"update",
"insert",
"delete",
"replace"
]

def read_mapper_output(file):
    """
    read data from file using yield
    """
    for line in file:
        yield line.rstrip() 


def db_log_normailze():
    """
    normalize db log, extend timestamp and merge multi-line sql statement
    """
    #read data from stdin
    data = read_mapper_output(sys.stdin)

    #last time 
    last_time = "\t"
    #current time command and sql
    time = ""
    command = ""
    sql = ""
    line_number = 1
    

    for line in data:
        db_match = db_regex.search(line)

        if db_match:
            if command != "":
                if sql and command.lower() in command_blacklist:
                    sql_match = sql_regex.search(sql)
                    if sql_match:
                        sql_command = sql_match.group(1)
                        if sql_command.lower() in query_blacklist:
                            debug = "FINAL_RESULT %d: %s %s %s" %(line_number - 1, time, command, sql)
                            logging.debug(debug)
                            sql_base64 = base64.b64encode(sql)
                            time_base64 = base64.b64encode(time)
                            print "%s\t%s" %(sql_base64, time_base64)
                            
            else:
                info ="NULL_COMMAND %d: %s %s %s" %(line_number - 1, time, command, sql)
                logging.info(info)

            time, command, sql = db_match.groups()            
            #time extend
            if time == "\t":
                time = last_time
            else:
                last_time = time
        else:
            #for debug
            info = "MULTI_LINE %d: %s" %(line_number, line.strip())
            logging.info(info)

            if command != "":
                sql = sql + line
                
                
        line_number = line_number + 1
        
        
        
        

if __name__ == '__main__':
    db_log_normailze()    
