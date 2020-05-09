# This script imports and norms our current two dictionary sources for comparison in the DDB
# Use: python3 prep_sources.py PATH_TO_CW.CSV PATH_TO_MD.CSV PATH_TO_DDB

# Original code written for ALTLab
# University of Alberta
# Aida Radu

# Last Updated: 26 April 2020


import sqlite3
import pandas as pd
import sys

def ImportSources(CW, MD):
  # transfer the given spreadsheets into dataframes
  # CW ::str:: the path to the CW csv being used
  # MD ::str:: the path to the MD csv being used

  CW_df = pd.read_csv(CW, usecols=['\sro','\ps','\def','\stm'])
  CW_df.columns = ['CW_normed_head','POS','CW_normed_definition','stem']
  CW_df['CW_source_head'] = CW_df['CW_normed_head']
  CW_df['CW_source_definition'] = CW_df['CW_normed_definition']

  MD_df = pd.read_csv(MD, usecols=['SRO','MeaningInEnglish'])
  MD_df.columns = ['MD_head','MD_normed_definition']
  MD_df['MD_source_definition'] = MD_df['MD_normed_definition']

  return (CW_df, MD_df)

def NormCW(CW_df):
  # standardize spelling in CW entries
  # CW_df ::Pandas.DataFrame:: 

  CW_head_replacements = {'c':'ch', 'â':'a','ê':'e','ô':'o','î':'i','-':'','ý':'y','š':'s'}
  CW_def_replacements = {'s/he': 'he', 's\.o\.':'him', 's\.t\.' : 'it',';':' ', ',': ' '}

  CW_df['CW_normed_head']=CW_df['CW_normed_head'].replace(CW_head_replacements, regex=True)
  CW_df['CW_normed_definition'] =  CW_df['CW_normed_definition'].replace(CW_def_replacements, regex = True)

  return CW_df

def NormMD(MD_df):
  # standardize spelling in MD entries
  # MD_df ::Pandas.DataFrame:: 

  MD_def_replacements = {'\.':' ', ',': ' '}

  MD_df['MD_normed_definition'] = MD_df['MD_normed_definition'].str.lower()
  MD_df['MD_normed_definition'] =  MD_df['MD_normed_definition'].replace(MD_def_replacements, regex = True)

  return MD_df

def CreateTables(CW_df, MD_df, db):
  # create and populate tables with normed sources
  # CW_df ::Pandas.DataFrame:: 
  # MD_df ::Pandas.DataFrame:: 
  # db    ::str:: the path to the database

  conn = sqlite3.connect(db)
  cur = conn.cursor()
  
  cur.execute('CREATE TABLE IF NOT EXISTS CW (CW_normed_head text, POS text, CW_normed_definition text, stem text, CW_source_head text, CW_source_definition text, PRIMARY KEY(CW_normed_head,CW_normed_definition))')
  conn.commit()
  CW_df.to_sql('CW', conn, if_exists ='replace', index = False)

  cur.execute('CREATE TABLE IF NOT EXISTS MD (MD_head text, MD_normed_definition text, MD_source_definition text, PRIMARY KEY(MD_head,MD_normed_definition))')
  conn.commit()
  MD_df.to_sql('MD', conn, if_exists ='replace', index = False)


  return

def main():
  (CW_df,MD_df) = ImportSources(sys.argv[1], sys.argv[2])
  CW_df = NormCW(CW_df)
  MD_df = NormMD(MD_df)
  CreateTables(CW_df,MD_df,sys.argv[3])

main()
