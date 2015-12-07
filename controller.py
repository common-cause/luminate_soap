#purpose of this module is to offer up a high level of abstraction for managing bulk download operations
#conceptually this could be part of the session object, but I think it's better not to clutter that further

from .session import SOAPSession, recordtypes
from os import chdir
from csv import reader
from .utilities import log
from .exceptions import SOAPError
from .database import curs
import pickle
from threading import Thread, Lock
from requests.exceptions import RequestException
from queue import Queue
from .local_settings import settings, debug
from psycopg2 import IntegrityError, DatabaseError, OperationalError
from io import StringIO
from math import floor

workerthreads = settings['workerthreads']

#these represent the practical limits to how many records the interface will return; documentation says it will support 200 at a go but this is not true for all record types
pagelimits = {'EmailRecipient' : 200, 'Constituent' : 100, 'ActionAlertResponse' : 100} 
#use this for sorting queried records
timefields = {('Constituent','insert') : 'CreationDate', ('Constituent','update') : 'ModifyDate', ('ActionAlertResponse','insert') : 'SubmitDate'}
pks = {'Constituent' : 'ConsId', 'ActionAlertResponse' : 'AlertResponseId'}



class DownloadThread(Thread):
	def __init__(self,parent,session,task_queue,db_queue,group=None,target=None,name=None):
		super().__init__(group=group,target=target,name=name)
		self.parent = parent
		self.daemon = True
		self.session = session
		self.task_queue = task_queue
		self.db_queue = db_queue
		self.name = name
		if debug:
			print(name + ' initiated')
	
	def run(self):
		while True:
			(soap, opn, el, op,fields,pageparams) = self.task_queue.get()
			
			try:
				if soap == 'dl':
					page = pageparams
					if debug:
						print('%s beginning work on %s %s page %s' % (self.name,opn,op,str(page)))
					response = self.session.download(el,fields,op,pagesize=pagelimits[el],page=page)

				elif soap == 'qu':					
					#for a query we need to explicitly load the date range because it's not embedded in the sync
					(sdate, edate, page) = pageparams
					if debug:
						print('%s beginning work on %s %s page %s' % (self.name,opn,op,str(page)))
					# now we assemble the query text
					qstring = "SELECT "
					fieldsstring = ', '.join(fields)
					qstring += fieldsstring
					timefield = timefields[(el,op)]
					sortfield = timefields[(el,'insert')]
					qstring += " WHERE %s >= '%sT00:00:00+0000' AND %s <= '%sT23:59:59+0000' ORDER BY %s" % (timefield, sdate, timefield, edate, sortfield)
					response = self.session.query(qstring,pagesize=pagelimits[el],page=str(page))
					if len(response.list_results()) == 0:
						self.parent.blankpage = page
				self.db_queue.put((soap,opn,el,op,page,response.list_results()))
				if debug:
					print('downloaded page %s of %s %s results; success!' % (str(page), el, op))
			except RequestException:
				self.db_queue.put((soap,opn,el,op,page,'HTTP ERROR'))
			except Exception as e:
				self.db_queue.put((soap,opn,el,op,page,'UNHANDLED EXCEPTION %s' % str(e).replace("'","''")))
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
			(soap,opn,el,op,page,data) = self.db_queue.get()
			if debug:
				print('dbthread working on page %s of %s %s' % (str(page),opn, op))
			try:
				assert type(data) == list
				#in all cases except constituent group relationships the data that's coming across is ready to be written to the db.
				#For cons/group relationships we need to split each row into multiple rows of consid - groupid
				if opn == 'ConsGroupRel':
					olddata = data
					data = []
					for consrecord in olddata:
						for grpid in consrecord[1:]:
							data.append([consrecord[0],grpid])
				results = StringIO('\n'.join(['\t'.join(row) for row in data]))
				try:
					self.db.copy_from(results,opn + '_loader',null='')
					self.progress_insert((opn,op,page),'C')
				except DatabaseError:
					self.db = curs()
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
					self.db.execute("INSERT INTO sync_errors (opname, operation, start_date, end_Date, page, error_message, event_time) VALUES ('%s','%s','%s','%s',%s,'%s',current_timestamp);" % (opn, op, self.start_date, self.end_date, str(page), data))
					self.db.execute('COMMIT;')
					if data == 'HTTP ERROR':
						errcode = 'E'
					else:
						errcode = 'U'
					self.progress_insert((opn,op,page),errcode)
				except:
					self.db_queue.put(opn, el, op, page, data)
					self.db = curs()
			self.db_queue.task_done()
			if debug:
				print('dbthread task done')
					
	def progress_insert(self,records,code):
		(opn, op, page) = (records[0], records[1], str(records[2]))
		self.db.execute("SELECT sync_progress_update('%s','%s','%s','%s',%s,'%s');" % (opn, op, self.start_date, self.end_date, page, code))
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
		#start independently threaded processes for managing the downloads from the database and uploads to the database
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
			threads[i] = DownloadThread(self,session,self.task_queue,self.db_queue,name='worker'+str(i))
			threads[i].start()
		self.dbthread.start()
		for (opn, op) in ops:
			#if we can use download ops we'll use those; we can get a page count and run the operation to completion
			self.db.execute("SELECT element FROM sync_ops WHERE opname = '%s' AND operation = '%s'" % (opn, op))
			el = self.db.fetchone()[0]
			if recordtypes[el].ops['GetIncremental' + op.capitalize() + 's'] == 'true':
				(pages, complete) = self.sync_status(opn,el,op,syncstart,syncend)
				if complete == 'N':
					self.db.execute("SELECT page FROM sync_progress WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'C'" % (opn,op,syncstart,syncend))
					completed = self.db.fetchall()
					self.db.execute('COMMIT;')
					self.db.execute("SELECT field FROM luminate_fields WHERE opname = '%s';" % (opn,))
					fields = [res[0] for res in self.db.fetchall()]
					for i in range(1,pages+1):
						if i not in [int(rec[0]) for rec in completed]:
							self.task_queue.put(('dl',opn,el,op,fields,i))
					self.task_queue.join()
					self.db_queue.join()
					self.db.execute("SELECT page FROM sync_progress WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'H'" % (opn, op, syncstart, syncend))
					for i in self.db.fetchall():
						self.task_queue.put(('dl',opn,el,op,fields,i))
					self.task_queue.join()
					self.db_queue.join()

			#need a different routine for when we're querying, because we won't be able to get a count
			elif recordtypes[el].ops['Query'] == 'true':
				self.blankpage = None
				(pages, complete) = self.query_status(opn, el, op, syncstart, syncend)
				if complete == 'N':
					if pages is not None:
						self.db.execute("SELECT page FROM sync_progress WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'C'" % (opn,op,syncstart,syncend))
						completed = self.db.fetchall()
						self.db.execute('COMMIT;')
					self.db.execute("SELECT field FROM luminate_fields WHERE opname = '%s';" % (opn,))
					fields = [res[0] for res in self.db.fetchall()]
					page = 1
					while True:
						if page not in [int(rec[0]) for rec in completed]:
							#if we pass 
							self.task_queue.put('qu',opn,el,op,fields,(syncstart,syncend,page))
							self.task_queue.join()
							if self.blankpage is None:
								page += 1
							else:
								self.db.execute("UPDATE sync_event SET pages = %s WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s'" % (str(self.blankpage-1),opn, op, sdate, edate))
								self.db.execute("COMMIT;")
								break
					self.db.execute("SELECT page FROM sync_progress WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'H'" % (opn, op, syncstart, syncend))
					for i in self.db.fetchall():
						self.task_queue.put('qu',opn,el,op,fields,(syncstart,syncend,i))
					self.task_queue.join()
					self.db_queue.join()
			else:
				raise SOAPError('attempted operation with no compatible option on the SOAP interface')
					
			self.db.execute("SELECT db_load('%s','%s')" % (opn,op))
			self.db.execute('COMMIT;')
			syncvals = (opn, op, syncstart, syncend)
			self.db.execute("SELECT is_complete('%s','%s','%s','%s')" % syncvals )
			complete = self.db.fetchone()[0]
			self.db.execute('COMMIT;')
			if complete:
				self.db.execute("UPDATE sync_event e SET completed = 'Y' WHERE e.opname = '%s' AND e.operation = '%s' AND e.start_date = '%s' AND e.end_date = '%s'" % syncvals)
				
	def patch(self,opn):
		self.db_connect()
		self.db.execute("SELECT element FROM sync_ops WHERE opname = '%s' AND operation = '%s'" % (opn, 'insert'))
		el = self.db.fetchone()[0]
		self.db.execute("COMMIT;")
		self.db.execute("SELECT field FROM luminate_fields WHERE opname = '%s'" % (opn,))
		fields = [rec[0] for rec in self.db.fetchall()]
		fieldstring = ', '.join(fields)
		self.db.execute("COMMIT")
		pk = pks[el]
		while True:
			self.db.execute("SELECT %s FROM %s_gaps WHERE resolved = 'N' LIMIT 100" % (pk, el))
			ids = [rec[0] for rec in self.db.fetchall()]
			if len(ids) == 0:
				break
			idjoin = 'OR %s = ' % (pk,)
			idstring = idjoin.join([str(id) for id in ids])
			self.db.execute("COMMIT;")

			qstring = "SELECT " + fieldstring + ' FROM ' + el + ' WHERE ' + pk + ' = ' + idstring
			r = self.session.query(qstring)
			data = r.list_results()
			if opn == 'ConsGroupRel':
				olddata = data
				data = []
				for consrecord in olddata:
					for grpid in consrecord[1:]:
						data.append([consrecord[0],grpid])
			results = StringIO('\n'.join(['\t'.join(row) for row in data]))
			self.db.copy_from(results,opn + '_loader',null='')
			self.db.execute("UPDATE %s_gaps g SET resolved = 'Y' WHERE %s = %s" % (el, pk, idstring))
			self.db.execute("SELECT db_load('%s','insert')" % (el,))
			self.db.execute("COMMIT;")

				
				
	def query_status(self, opn, el, op, start_date, end_date):
		self.db.execute("SELECT * FROM sync_event WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s'" % (opn,op,start_date,end_date))	
		status = self.db.fetchone()
		self.db.execute('COMMIT;')
		if status is None:
			self.db.execute("INSERT INTO sync_event (opname, operation, start_date, end_date) VALUES ('%s','%s','%s','%s')" % (opn,op,start_date,end_date))
			self.db.execute("COMMIT;")
			return(None, 'N')
		else:
			return(int(status[4]), status[5])
			
	def sync_status(self, opn, el,op, start_date, end_date):
		self.start_sync(start_date,end_date)
		self.db.execute("SELECT * FROM sync_event WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s'" % (opn,op,start_date,end_date))
		status = self.db.fetchone()
		self.db.execute('COMMIT;')
		if status is None:
			recordcount = self.session.getcount(el,op)
			pages = 1 + (recordcount - recordcount % pagelimits[el]) / pagelimits[el]
			self.db.execute("INSERT INTO sync_event (opname, operation, start_date, end_date, pages) VALUES ('%s','%s','%s','%s',%s)" % (opn, op, start_date, end_date, str(pages)))
			status = (opn, op, start_date, end_date, pages, 'N')
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
			pages = (records - records % 200) / 200 + 1
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
		