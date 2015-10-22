import psycopg2

from .local_settings import settings


def conn():
	return psycopg2.connect(user=settings['db_user'],password=settings['db_pw'],database=settings['db_name'],host=settings['db_host'])

def reconnect():
	conn = psycopg2.connect(user=settings['db_user'],password=settings['db_pw'],database=settings['db_name'],host=settings['db_host'])

def curs():
	return conn().cursor()
	