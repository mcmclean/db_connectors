#!/usr/bin/env python3

'''
fooling around w/ new database connectors
'''

# https://auth0.com/blog/sqlalchemy-orm-tutorial-for-python-developers/
# https://www.learndatasci.com/tutorials/using-databases-python-postgres-sqlalchemy-and-alembic/#Workingwithsessions
# https://www.compose.com/articles/using-postgresql-through-sqlalchemy/

# https://packaging.python.org/guides/installing-using-pip-and-virtual-environments/
# https://packaging.python.org/overview/
# https://setuptools.readthedocs.io/en/latest/setuptools.html#automatic-script-creation

# https://docs.sqlalchemy.org/en/13/orm/tutorial.html # <---- this is a good one, good querying stuff and other basics

from sqlalchemy import create_engine, Column, String, Integer, Date
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text


import pandas as pd
from datetime import datetime
import logging
import os

from db_config import get_config

##############################################################################

##################
#### Classes #####
##################


class Creds(object): # private, protected, etc.?
	def __init__(self):
		self.username = os.environ['USERNAME']
		self.password = os.environ['PASSWORD']
		self.string_segment = self.username + ':' + self.password


class ConnectionInfo(object):
	def __init__(self, db = get_config()['db'], host = get_config()['host'], creds = Creds()):
		self.db = db
		self.host = host
		self.creds_obj = creds
		self.params_string = creds.string_segment + '@' + self.host + '/' + self.db


class DBEngine(object):
	def __init__(self, flavor, conn):
		self.db_flavor = flavor
		self.connection = self.db_flavor + '://' + conn.params_string
		self.engine = None

	def construct_engine(self):
		self.engine = create_engine(self.connection, paramstyle = 'format')

	def shut_down(self):
		self.engine.dispose()


class SQLSession(object):
	def __init__(self, engine):
		self.engine = engine
		Session = sessionmaker(bind=self.engine)
		self.session = Session()


##############################################################################

##################
### Decorators ###
##################

def connection_info(func):
	def conn_info_inner(*args,**kwargs):
		conn_info = ConnectionInfo() # should grab from wherever
		rv = func(conn_info, *args,**kwargs)
		return rv
	return conn_info_inner


@connection_info
def raw_sql_conn(conn_info, func):
	def raw_sql_inner(*args, **kwargs):
		eng = DBEngine('postgres', conn_info)
		eng.construct_engine()
		conn = eng.engine.connect()
		trans = conn.begin()
		try:
			rv = func(conn, *args,**kwargs)
			trans.commit()
		except Exception:
			trans.rollback()
			logging.error("Database connection error")
			raise
		finally:
			conn.close() # close_all?
			eng.shut_down()
		return rv
	return raw_sql_inner


@connection_info
def engine(conn_info, func):
	def engine_inner(*args,**kwargs):
		engine_ = DBEngine('postgres', conn_info)
		engine_.construct_engine()
		rv = func(engine_.engine, *args,**kwargs)
		engine_.shut_down()
		return rv
	return engine_inner


@engine
def sql_session(engine, func):
	def sql_session_inner(*args,**kwargs):
		sess = SQLSession(engine)
		cnn = sess.session
		try:
			rv = func(cnn, *args,**kwargs)
		except Exception:
			cnn.rollback()
			logging.error("Database connection error")
			raise
		else:
			cnn.commit()
		finally:
			cnn.close()
		return rv
	return sql_session_inner


##############################################################################

##################
## Misc/Testing ##
##################

Base = declarative_base()

@engine
def recreate_database(engine):
	Base.metadata.drop_all(engine)
	Base.metadata.create_all(engine)


class Book(Base):
	__tablename__ = 'books'
	id = Column(Integer, primary_key=True)
	title = Column(String)
	author = Column(String)
	pages = Column(Integer)
	published = Column(Date)

	def __repr__(self):
		return "<Book(title='{}', author='{}', pages={}, published={})>".format(self.title, self.author, self.pages, self.published)


@sql_session
def add_book_test(sess):
	recreate_database()
	print("\nDatabase refreshed.\n")
	book = Book(
		title='Deep Learning',
		author='Ian Goodfellow',
		pages=775,
		published=datetime(2016, 11, 18)
	)
	sess.add(book)
	print("Book \"{}\" added to database.\n".format(book.title))


def create_frame_from_sqlalchemy_class(item):
	attributes = [attr for attr in dir(item) if not attr.startswith('__')]
	attributes = [attr for attr in attributes if attr not in ['_decl_class_registry', '_sa_class_manager', 'metadata', '_sa_instance_state']]
	# print(attributes)
	return pd.DataFrame(columns = attributes)


@engine
def read_book(engine):
	data = pd.read_sql("select * from public.books", engine)
	# print(data)
	return data


@sql_session
def get_first_book(conn):
	# result = conn.query(Book).all() # faster than raw_sql or pandas + engine
	# result = conn.query(Book).from_statement(text("SELECT * FROM public.books")).all() # even faster
	result = pd.read_sql(conn.query(Book).from_statement(text("SELECT * FROM public.books")).statement, conn.bind) 

	# print(result)
	return result


@raw_sql_conn
def get_book(conn):
	# result = conn.execute("select * from public.books")
	# for line in result:
	# 	print(line)

	# df = DataFrame(resoverall.fetchall())
	# df.columns = resoverall.keys()

	df = pd.read_sql("select * from public.books", conn)
	return df


if __name__ == "__main__":
	### Create book table, refresh DB if necessary, add book	
	# add_book_test() # add a book via session

	import time
	start_time = time.time()

	data = get_first_book()
	# data = get_book()
	# data = read_book()


	# for i in range(500):
	# 	### Three ways to read the db
	# 	data = get_first_book() # via Session >>> 2-2.5s
		# data = read_book() # via pandas + engine >>> 20s
		# data = get_book() # via raw SQL >>> 18s
	print(data)

	# time out the above??? ^^^ remove print statements as well for test
	print("--- %s seconds ---" % (time.time() - start_time))

	# all_books = get_first_book()
	# print(all_books)
	# df = create_frame_from_sqlalchemy_class(Book)
	# book = pd.DataFrame([{k:v for k, v in vars(f).items()} for f in all_books])
	# print(book)
	# df = df.append(book)
	# print(df.head())



### TODO:  convert all engine calls (and maybe connections?) from session to .bind 
### also binds – multiple engines/connections per session ---> solution to encoding problem!

# https://docs.sqlalchemy.org/en/13/orm/basic_relationships.html#relationship-patterns