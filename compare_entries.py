# This script compares definitions from our two dictionary sources
# Use: python3 compare_entries.py PATH_TO_DDB

# Original code written for ALTLab
# University of Alberta
# Aida Radu

# Last Updated: 1 May 2020

import sqlite3
import pandas as pd
import numpy as np
import sys
import fuzzywuzzy
from fuzzywuzzy import fuzz

def ConnectToDb(db):
    # open a connection to the DDB
    # db ::str:: the path to the DDB
    
    return(sqlite3.connect(db))
 
def CWvsMD(conn):
    # create a table with info from both sources matches for normed data
    # conn ::sqlite3.Connection:: the connection to the DDB
    
    CW_df = pd.read_sql('SELECT * FROM CW', conn)
    MD_df = pd.read_sql('SELECT * FROM MD', conn)

    # a full (outer) join combines all entries from both sources;
    # matching the entries which appear in both dictionaries (based on heads)
    # the "indicator" parameter adds info about which dictionary the entry is found in
    # - i.e., MD, CW, or both

    full_join = pd.merge(CW_df,MD_df,how='outer',left_on = 'CW_normed_head',right_on = 'MD_head',indicator=True)
    full_join = full_join.rename(columns={"_merge": "Location"})
    full_join["Location"]=full_join["Location"].replace({"left_only":"CW","right_only":"MD"})

    # Adding a % match column and doing comparisons using fuzzy set ratio
    # This calculation compares words regardless of position, duplication, and punctuation
    
    full_join["Percent_match"] = np.nan

    for ind,row in full_join.iterrows():
        if full_join.loc[ind,"Location"] == "both":
            percent_match = fuzz.token_set_ratio(full_join.loc[ind,"CW_normed_definition"],full_join.loc[ind,"MD_normed_definition"])
            full_join.loc[ind,"Percent_match"] = percent_match

    return(full_join)

def Write(data,table,conn):
    # write data to the DDB
    # data  ::pandas.DataFrame:: contains the data to write
    # table ::str:: the name of the table in the DDB to write to
    # conn  ::sqlite3.Connection:: the connection to the DDB
    data.to_sql(table,conn, if_exists ='replace')
    return 

def main():
    conn = ConnectToDb(sys.argv[1])
    comp = CWvsMD(conn)
    Write(comp, 'CWMD_Aggregate',conn)
    conn.close()

main()

