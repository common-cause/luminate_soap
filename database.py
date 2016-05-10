import psycopg2
from psycopg2 import OperationalError, DatabaseError, DataError, ProgrammingError
from .local_settings import settings, debug
from .soap_message import SOAPResponse
from threading import Thread
from io import StringIO

def conn():
	return psycopg2.connect(user=settings['db_user'],password=settings['db_pw'],database=settings['db_name'],host=settings['db_host'])

def reconnect():
	conn = psycopg2.connect(user=settings['db_user'],password=settings['db_pw'],database=settings['db_name'],host=settings['db_host'])

def curs():
	return conn().cursor()


			
class DBThread(Thread):
	def __init__(self,db,db_queue,start_date,end_date,group=None,target=None,name=None):
		super().__init__(group=group,target=target,name=name)
		self.daemon = True
		self.db = db
		self.db_queue = db_queue
		self.start_date = start_date
		self.end_date = end_date
		self.headers = {}
		print('DBThread Initiated')
		
	def run(self):
		print('dbthread running')
		while True:
			(soap,opn,el,op,page,response) = self.db_queue.get()
			if debug:
				print('dbthread working on page %s of %s %s' % (str(page),opn, op))
			try:
				assert type(response) == SOAPResponse
				try:
					header = self.headers[opn]
				except KeyError:
					self.db.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '%s_loader' ORDER BY ordinal_position" % opn.lower())
					header = [col[0] for col in self.db.fetchall()]
					self.db.execute('commit;')
					self.headers[opn] = header
				data = response.list_results(header=header)
				#in all cases except constituent group relationships the data that's coming across is ready to be written to the db.
				#For cons/group relationships we need to split each row into multiple rows of consid - groupid
				if debug:
					print('dbthread working on %s results' % str(len(data)))
				if opn == 'ConsGroupRel':
					olddata = data
					data = []
					for consrecord in olddata:
						for grpid in consrecord[1]:
							data.append([consrecord[0],grpid])
				results = StringIO('\n'.join(['\t'.join(row) for row in data]))
				try:
					self.db.copy_from(results,opn + '_loader',null='')
					self.progress_insert((opn,op,page),'C')
				except DatabaseError as d:
					self.db = curs()
					print('database error %s' % str(d))
					results = StringIO('\n'.join(['\t'.join(row) for row in data]))
					self.db.copy_from(results,opn + '_loader',null='')
					self.progress_insert((opn,op,page),'C')
				except OperationalError:
					self.db = curs()
					self.db_queue.put((opn, el, op, page, data))
					self.progress_insert((opn,op,page),'C')
				except:
					self.db.execute('rollback;')
					self.progress_insert((opn,op,page),'D')
					raise
			except AssertionError:
				try:
					self.db.execute("INSERT INTO sync_errors (opname, operation, start_date, end_Date, page, error_message, event_time) VALUES ('%s','%s','%s','%s',%s,'%s',current_timestamp);" % (opn, op, self.start_date, self.end_date, str(page), response))
					self.db.execute('COMMIT;')
					if response == 'HTTP ERROR':
						errcode = 'E'
					else:
						errcode = 'U'
					self.progress_insert((opn,op,page),errcode)
				except OperationalError:
					self.db_queue.put((opn, el, op, page, data))
					self.db = curs()
				except ProgrammingError:
					print('actual response type was %s' % str(type(response)))
			self.db_queue.task_done()
			if debug:
				print('dbthread task done')
					
	def progress_insert(self,records,code):
		(opn, op, page) = (records[0], records[1], str(records[2]))
		self.db.execute("SELECT sync_progress_update('%s','%s','%s','%s',%s,'%s');" % (opn, op, self.start_date, self.end_date, page, code))
		self.db.execute('COMMIT;')