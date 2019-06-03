#!/usr/bin/env python3

import psycopg2
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
import os
import logging
from db_engine import give_conn_info, execute_raw_sql, engine

@give_conn_info
def fast_copy(conn_info, filename = 'ethanol fuel prices.csv', tablename = 'fuel_prices'):
	csv_file_path = Path.cwd().parent / filename
	with open(csv_file_path, 'r') as f:    
		conn = psycopg2.connect("host=localhost dbname=postgres user=postgres password=password")
		cursor = conn.cursor()
		cmd = "COPY public.master_{} FROM STDIN DELIMITER ',' CSV HEADER ENCODING 'UTF8'".format(tablename)
		try:
			cursor.copy_expert(cmd, f)
			conn.commit()
		except:
			conn.rollback()
		finally:
			conn.close()
	print("Data load complete")


@execute_raw_sql
def make_table(raw_conn, tablename = 'fuel_prices'): # invoke Mohsen's tablename style thing
	sql_drop_table_cmd = "DROP TABLE IF EXISTS public.master_{};".format(tablename)
	sql_create_table_cmd = """
	    CREATE TABLE public.master_{} (
	        \"Month\" text NULL,
	        \"2017\" double precision NULL,
	        \"2018\" double precision NULL,
	        \"2019\" double precision NULL
	    );
	""".format(tablename)
	raw_conn.execute(sql_drop_table_cmd)
	raw_conn.execute(sql_create_table_cmd)
	print('Table created')


def full_table_import():
	make_table()
	fast_copy()


# def make_individual_csvs(inpath = None, outpath = None):
# 	if inpath is None:
# 		inpath = Path.cwd().parent / 'Downloads' / 'Normalized Worksheet Global Sugar - EDITED.xlsx'
# 	xl = pd.ExcelFile(inpath)
# 	xl.sheet_names  # see all sheet names
# 	for name in list(xl.sheet_names):
# 		sheet = xl.parse(name)  # read a specific sheet to DataFrame
# 		filename = '{}.csv'.format(name)
# 		if outpath is None:
# 			outpath = Path.cwd() / 'MDLZ Procurement RFP Test App' / 'Inputs'
# 		curr_path = outpath / filename
# 		sheet.to_csv(curr_path, index = False)


	'''
	learn to automatically generate platform jsons from input files
	'''

def guess_file_types():
	csv_file_path = Path.cwd().parent / 'ethanol fuel prices.csv'
	df = pd.read_csv(csv_file_path, nrows = 10)
	print(df.dtypes	)


@engine
def write_output(eng, frame, tablename = 'forecast_output'):
	frame.to_sql(tablename, eng, schema = 'public', if_exists='replace', index = False) # change last two args once on platform

@engine
def read_table(eng, table = 'books'):
	df = pd.read_sql('select * from public.{}'.format(table), eng)
	print(df.head())

if __name__ == "__main__":
	# make_table()
	# guess_file_types()

	# read_table('books')
	# fast_copy()

	full_table_import()