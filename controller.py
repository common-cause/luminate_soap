#purpose of this module is to offer up a high level of abstraction for managing bulk download operations
#conceptually this could be part of the session object, but I think it's better not to clutter that further

from .session import SOAPSession
from os import chdir
from csv import reader
from .utilities import log
from .exceptions import SOAPError
from .database import curs
import pickle
from threading import Thread
from requests.exceptions import RequestException
from queue import Queue
from .local_settings import settings, debug
from psycopg2 import IntegrityError, DatabaseError
from io import StringIO
from math import floor

workerthreads = settings['workerthreads']

class DownloadThread(Thread):
	def __init__(self,session,task_queue,db_queue,group=None,target=None,name=None):
		super().__init__(group=group,target=target,name=name)
		self.daemon = True
		self.session = session
		self.task_queue = task_queue
		self.db_queue = db_queue
		self.name = name
		if debug:
			print(name + ' initiated')
	
	def run(self):
		while True:
			(el, op,fields, page) = self.task_queue.get()
			if debug:
				print('%s beginning work on %s %s page %s' % (self.name,el,op,str(page)))
			try:
				response = self.session.download(el,fields,op,page=page)
				self.db_queue.put((el,op,page,response.list_results()))
				if debug:
					print('downloaded page %s of %s %s results; success!' % (str(page), el, op))
			except RequestException:
				self.db_queue.put((el,op,page,'HTTP ERROR'))
			except Exception as e:
				self.db_queue.put((el,op,page,'UNHANDLED EXCEPTION %s' % str(e).replace("'","''")))
			self.task_queue.task_done()


class DBThread(Thread):
	def __init__(self,db,db_queue,start_date,end_date,group=None,target=None,name=None):
		super().__init__(group=group,target=target,name=name)
		self.daemon = True
		self.db = db
		self.db_queue = db_queue
		self.start_date = start_date
		self.end_date = end_date
		print('DBThread Initiated')
		
	def run(self):
		while True:
			records = self.db_queue.get()
			if debug:
				print('dbthread working on page %s of %s %s' % (str(records[2]), records[0], records[1]))
			try:
				assert type(records[3]) == list
				results = StringIO('\n'.join(['\t'.join(row) for row in records[3]]))
				try:
					self.db.copy_from(results,records[0] + '_loader',null='')
					self.progress_insert(records,'C')
				except DatabaseError:
					self.db = curs()
					self.db.copy_from(results,records[0] + '_loader',null='')
					self.progress_insert(records,'C')
				except:
					self.db.execute('rollback;')
					self.progress_insert(records,'D')
					raise
			except AssertionError:
				self.db.execute("INSERT INTO sync_errors (element, operation, start_date, end_Date, page, error_message, event_time) VALUES ('%s','%s','%s','%s',%s,'%s',current_timestamp);" % (records[0], records[1], self.start_date, self.end_date, str(records[2]), records[3]))
				self.db.execute('COMMIT;')
				if records[3] == 'HTTP ERROR':
					errcode = 'E'
				else:
					errcode = 'U'
				self.progress_insert(records,errcode)
			self.db_queue.task_done()
			if debug:
				print('dbthread task done')
					
	def progress_insert(self,records,code):
		(el, op, page) = (records[0], records[1], str(records[2]))
		self.db.execute("SELECT sync_progress_update('%s','%s','%s','%s',%s,'%s');" % (el, op, self.start_date, self.end_date, page, code))
		self.db.execute('COMMIT;')


class Controller():
	def __init__(self,):
		self.session = SOAPSession()
		self.db = None
		self.sync = None
		
	def db_connect(self):
		if self.db is None:
			self.db = curs()
		
	def db_sync(self,syncstart,syncend,ops=None):
		self.db_connect()
		if ops is None:
			self.db.execute('SELECT element, operation FROM sync_ops')
			ops = self.db.fetchall()
		self.db.execute('COMMIT;')
		self.task_queue = Queue()
		self.db_queue = Queue()
		self.dbthread = DBThread(curs(),self.db_queue,syncstart,syncend,name='db')
		threads = {}
		for i in range(workerthreads):
			sessions = {1:self.session}
			sessnum = floor(i/2) + 1
			try:
				session = sessions[sessnum]
			except KeyError:
				session = SOAPSession(username=settings['account' + str(sessnum)],pw=settings['pw' + str(sessnum)])
				sessions[sessnum] = session
				try:
					session.start_sync(syncstart,syncend)
				except SOAPError as e:
					if e.faultcode == 'CLIENT':
						session.end_sync()
						session.start_sync(syncstart,syncend)
					else:
						raise
			threads[i] = DownloadThread(session,self.task_queue,self.db_queue,name='worker'+str(i))
			threads[i].start()
		self.dbthread.start()
		for (el, op) in ops:
			(pages, complete) = self.sync_status(el,op,syncstart,syncend)
			if complete == 'N':
				self.db.execute("SELECT page FROM sync_progress WHERE element = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'C'" % (el,op,syncstart,syncend))
				completed = self.db.fetchall()
				self.db.execute('COMMIT;')
				self.db.execute("SELECT * FROM luminate_fields WHERE element = '%s';" % (el,))
				fields = [res[1] for res in self.db.fetchall()]
				for i in range(1,pages+1):
					if i not in [int(rec[0]) for rec in completed]:
						self.task_queue.put((el,op,fields,i))
				self.task_queue.join()
				self.db_queue.join()
				self.db.execute("SELECT page FROM sync_progress WHERE element = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'H'" % (el, op, syncstart, syncend))
				for i in self.db.fetchall():
					self.task_queue.put((el,op,fields,i))
				self.task_queue.join()
				self.db_queue.join()
				self.db.execute("SELECT db_load('%s','%s')" % (el,op))
				self.db.execute('COMMIT;')
				syncvals = (el, op, syncstart, syncend)
				self.db.execute("SELECT is_complete('%s','%s','%s','%s')" % syncvals )
				complete = self.db.fetchone()[0]
				self.db.execute('COMMIT;')
				if complete:
					self.db.execute("UPDATE sync_event e SET completed = 'Y' WHERE e.element = '%s' AND e.operation = '%s' AND e.start_date = '%s' AND e.end_date = '%s'" % syncvals)
			
	def sync_status(self, el, op, start_date, end_date):
		self.start_sync(start_date,end_date)
		self.db.execute("SELECT * FROM sync_event WHERE element = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s'" % (el,op,start_date,end_date))
		status = self.db.fetchone()
		self.db.execute('COMMIT;')
		if status is None:
			recordcount = self.session.getcount(el,op)
			pages = 1 + (recordcount - recordcount % 100) / 100 
			self.db.execute("INSERT INTO sync_event (element, operation, start_date, end_date, pages) VALUES ('%s','%s','%s','%s',%s)" % (el, op, start_date, end_date, str(pages)))
			status = (el, op, start_date, end_date, pages, 'N')
		return (int(status[4]), status[5])
		
	def start_sync(self,start_date,end_date):
		if self.sync == (start_date,end_date):
			return
		try:
			self.session.start_sync(start_date,end_date)
		except SOAPError as e:
			if e.faultcode == 'CLIENT':
				self.session.end_sync()
				self.session.start_sync(start_date,end_date)
			else:
				raise
		self.sync = (start_date,end_date)
		
	def sync_ops(self,start_date,end_date,fprefix,optuples,progressdata=None):
		"""Carry out a full set of sync operations.  Can be a new set of instructions or resuming a previous set.
		Takes the arguments:
		start_date - isoformatted start date of the sync window
		end_date - isoformatted end date of the sync window
		fprefix - prefix which will be prepended to the name of all files generated by this operation
		optuples - a list of tuples, of the form (data_element, op, [list, of, fields]) specifying what to download
		progressdata - if applicable, an dictionary object indicating how much of each download has already been completed, of the form {(data_element, op) : pages, ...}"""
		log('Initiating a sync session from %s to %s' % (start_date,end_date))
		self.progress = {}
		try:
			assert progressdata is None
		except AssertionError:
			self.progress=progressdata
		
		self.start_sync(start_date,end_date)

		for (data_element,op,fields) in optuples:
			records = self.session.getcount(data_element,op)
			pages = (records - records % 100) / 100 + 1
			try:
				page = self.progress.get((data_element,op)) + 1
			except TypeError:
				page = 1
			while page <= pages:
				try:
					self.download_pages(data_element,fields,op,page,min(page+49,pages),fprefix + '-' + data_element + '-' + op + '.csv',newfile=page==1)
					page += 50
				except:
					self.progress[(data_element,op)]+= -1
					with open(fprefix + '_progress.pk3','wb') as progressdump:
						pickle.dump(self.progress,progressdump,protocol=3)
					raise
	
	def sync_from_folder(self,folder):
		"""Designate a folder containing a guidefile containing instructions for download ops.
		Format of the guidefile should be as follows:
		first row: start date (isoformat), end date (isoformat), prefix
		succeeding rows: data_element (Constituent,ActionAlertResponse,etc), operation (insert, update, delete), list of all fields to download"""
		chdir(folder)
		instructions = []
		with open('guidefile.csv','rt') as guidefile:
			guidedata = reader(guidefile)
			[start_date, end_date, prefix] = next(guidedata)
			for instruct in guidedata:
				instructions.append([instruct[0],instruct[1],instruct[2:]])
		try:
			with open(prefix + '_progress.pk3','rb') as progressfile:
				progress = pickle.load(progressfile)
		except FileNotFoundError:
			progress = None
		self.sync_ops(start_date, end_date, prefix, instructions,progressdata=progress)
		
		
	def download_pages(self,data_element,fields,op,startpage,endpage,destfile,newfile=True):
		if newfile:
			self.session._prep_writefile(destfile)
		else:
			self.session._append_writefile(destfile)
		
		while startpage <= endpage:
			self.session.dl_write(data_element,fields,op,page=startpage)
			startpage += 1
			if startpage % 10 == 0:
				self.session.writefile.flush()
			self.progress[(data_element,op)] = startpage
		log('Downloaded pages %s to %s of %s records from the %s set to %s.' % (str(startpage), str(endpage), data_element, op, destfile))
		