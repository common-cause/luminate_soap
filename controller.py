#purpose of this module is to offer up a high level of abstraction for managing bulk download operations
#conceptually this could be part of the session object, but I think it's better not to clutter that further

from .session import SOAPSession, recordtypes
from os import chdir
from csv import reader
from .utilities import  isodate_to_jsdate
from .exceptions import SOAPError
from .database import curs, DBThread
import pickle
from threading import Thread, Lock
from requests.exceptions import RequestException
from queue import Queue
from .local_settings import settings, debug, pagelimits, timefields, pks,longdates
from psycopg2 import IntegrityError, DatabaseError, OperationalError, ProgrammingError
from io import StringIO
from math import floor
from datetime import date
from time import mktime

workerthreads = settings['workerthreads']

#these represent the practical limits to how many records the interface will return; documentation says it will support 200 at a go but this is not true for all record types

dontquery = ['Donation']


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
			inst = self.task_queue.get()
			
			try:
				if inst.soap == 'dl':
					if debug:
						print('%s beginning work on %s %s page %s' % (self.name,inst.opn,inst.op,str(inst.page)))
					response = self.session.download(inst.el,inst.fields,inst.op,pagesize=pagelimits[inst.el],page=inst.page)
					results = response.list_results()
					self.db_queue.put((inst.soap,inst.opn,inst.el,inst.op,inst.page,response))
				elif inst.soap == 'qu':					
					#for a query we need to explicitly load the date range or other criteria because it's not embedded in the sync
					if debug:
						print('%s beginning work on %s %s page %s' % (self.name,inst.opn,inst.op,str(inst.page)))

					response = self.session.query_fields(inst.el,inst.fields,inst.op,start_date=inst.startdate,end_date=inst.enddate,page=inst.page,querytype=inst.querytype,)
					results = response.list_results()
					if len(results) == 0:
						self.parent.blankpage = inst.page
					else:
						self.db_queue.put((inst.soap,inst.opn,inst.el,inst.op,inst.page,response))
				if debug:
					print('downloaded page %s of %s %s results; success!' % (str(inst.page), inst.el, inst.op))
			except RequestException:
				self.db_queue.put((inst.soap,inst.opn,inst.el,inst.op,inst.page,'HTTP ERROR'))
			except Exception as e:
				self.db_queue.put((inst.soap,inst.opn,inst.el,inst.op,inst.page,'UNHANDLED EXCEPTION %s, %s' % (e.__class__.__name__, str(e).replace("'","''"))))
				raise
			self.task_queue.task_done()
			


class Controller():
	def __init__(self,):
		self.session = SOAPSession()
		self.db = None
		self.sync = None
		self.threads = {}
		self.task_queue = Queue()
		self.db_queue = Queue()
		self.db_connect()
		self.dbthread = DBThread(curs(),self.db_queue,start_date=None,end_date=None,name='db')
		self.dbthread.start()
		print('controller not totally shitting the bed')
		self.threads = {}
		for i in range(workerthreads):
			print('working on workerthread %s' % str(i))
			sessions = {1:self.session}
			sessnum = floor(i/2) + 1
			try:
				session = sessions[sessnum]
			except KeyError:
				session = SOAPSession(username=settings['account' + str(sessnum)],pw=settings['pw' + str(sessnum)])
				sessions[sessnum] = session

			self.threads[i] = DownloadThread(self,session,self.task_queue,self.db_queue,name='worker'+str(i))
			self.threads[i].start()
		
	def db_connect(self):
		if self.db is None:
			self.db = curs()
		
	def db_sync_by_days(self,syncstart,syncend,ops):
		self.db.execute('SELECT populate_days();')  #populate the days table up to the present date
		self.db.execute('COMMIT;')
		for (opn, op) in ops:
			self.db.execute("SELECT cd.past_date FROM convio_days cd WHERE cd.past_date BETWEEN '%s' AND '%s' AND NOT EXISTS (SELECT 'X' FROM sync_event e WHERE cd.past_date BETWEEN e.start_date AND e.end_date AND e.opname = '%s' AND e.operation = '%s' AND e.completed = 'Y')" % (syncstart, syncend, opn, op))
			days_to_sync = [data_row[0] for data_row in self.db.fetchall()]
			for sync_day in days_to_sync:
				print('trying to sync %s %s for %s' %(opn, op, sync_day.isoformat()))
				self.db_sync_one(opn,op,sync_day.isoformat(),sync_day.isoformat())
	
	def __get_fields__(self,opname, el):
		self.db.execute("SELECT field, parent FROM luminate_fields WHERE opname = '%s';" % (opname,))
		dlfields = [(res[0], res[1]) for res in self.db.fetchall()]	
		self.db.execute('COMMIT;')
		#this is commented out because I moved the sorting task into the data_structures.DataElement.prepsort function
		#need to get the syntax right for fields where we have a parent record type to access them through.
		#el_obj = recordtypes[el]
		#dlfields.sort(key=lambda x: get_fieldsortkey(el_obj,x))
		fields = []
		i = 0
		while i < len(dlfields):
			(fname, parent) = dlfields[i]
			if parent is None:
				fields.append((parent,fname))
			else:
				fields.append((parent,fname))
			i += 1				
		return fields		
		
	def __sync__(self,opn, el, op, syncstart, syncend):
		(pages, complete) = self.sync_status(opn,el,op,syncstart,syncend)
		if complete == 'N':
			fields= self.__get_fields__(opn,el)
				
			for i in range(1,pages+1):
				inst = Download_Instructions('dl',opn,el,op,fields,i)
				self.task_queue.put(inst)
			self.task_queue.join()
			self.db_queue.join()
			self.db.execute("SELECT page FROM sync_progress WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'H'" % (opn, op, syncstart, syncend))
			for i in self.db.fetchall():
				inst = Download_Instructions('dl',opn,el,op,fields,i)
				self.task_queue.put(inst)
			self.task_queue.join()
			self.db_queue.join()
			
	def __query__(self,opn, el, op, syncstart = None, syncend = None, altwhere = None):
		self.blankpage = None
		if syncstart is None:
			(syncstart, syncend) = ('2014-06-01', date.today().isoformat())
		
		(pages, complete) = self.query_status(opn, el, op, syncstart, syncend)

		if complete == 'N':
			completed = []
			if pages is not None:
				self.db.execute("SELECT page FROM sync_progress WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'C'" % (opn,op,syncstart,syncend))
				completed = self.db.fetchall()
				self.db.execute('COMMIT;')
			self.db.execute("SELECT field, parent FROM luminate_fields WHERE opname = '%s';" % (opn,))
			fields= self.__get_fields__(opn,el)
			if syncstart is not None:
				type = 'time'
			else:
				type = 'other'
			page = 1
			while True:
				if page not in [int(rec[0]) for rec in completed]:
					#if we pass 
					inst = Download_Instructions('qu',opn,el,op,fields,page,querytype=type,startdate=syncstart,enddate=syncend)
					self.task_queue.put(inst)
					self.task_queue.join()
					if self.blankpage is None:
						page += 1
					else:
						self.db.execute("UPDATE sync_event SET pages = %s WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s'" % (str(self.blankpage-1),opn, op, syncstart, syncend))
						self.db.execute("COMMIT;")
						break
			self.db.execute("SELECT page FROM sync_progress WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s' AND status = 'H'" % (opn, op, syncstart, syncend))
			try:
				for i in self.db.fetchall():
					self.task_queue.put('qu',opn,el,op,fields,('time',syncstart,syncend,i))
			except ProgrammingError:
				pass
			self.task_queue.join()
			self.db_queue.join()
		
		
	def db_sync_one(self,opn,op,syncstart,syncend):
		self.db.execute("SELECT element FROM sync_ops WHERE opname = '%s' AND operation = '%s'" % (opn, op))
		el = self.db.fetchone()[0]
		if debug:
			print("syncing one, el is %s" % el)
		syncvals = (opn, op, syncstart, syncend)
		self.db.execute('DELETE FROM %s_loader;' % opn)
		self.db.execute("DELETE FROM sync_event e WHERE e.opname = '%s' AND e.operation = '%s' AND e.start_date = '%s' AND e.end_date = '%s' AND e.completed = 'N'" % syncvals)
		self.dbthread.start_date = syncstart
		self.dbthread.end_date = syncend
		#find out what operations the SOAP interface supports for this element
		validops = recordtypes[el].ops
		#querying is faster, so try that first
		if validops['Query'] == 'true' and opn not in dontquery:
			self.__query__(opn,el,op,syncstart=syncstart,syncend=syncend)
		elif validops['GetIncremental' + op.capitalize() + 's'] == 'true':
			self.__sync__(opn, el, op, syncstart, syncend)

		else:
			raise SOAPError('attempted operation with no compatible option on the SOAP interface')
								
		self.db.execute("SELECT is_complete('%s','%s','%s','%s')" % syncvals )
		complete = self.db.fetchone()[0]
		self.db.execute('COMMIT;')
		if complete:
			print('the sync op succeeded')
			self.db.execute("SELECT db_load('%s','%s')" % (opn,op))
			self.db.execute("UPDATE sync_event e SET completed = 'Y' WHERE e.opname = '%s' AND e.operation = '%s' AND e.start_date = '%s' AND e.end_date = '%s'" % syncvals)
			self.db.execute('COMMIT;')
			self.db.execute('COMMIT;')
		else:
			print ('the sync op failed')
			self.db.execute('DELETE FROM %s_loader;' % opn)
			self.db.execute("DELETE FROM sync_event e WHERE e.opname = '%s' AND e.operation = '%s' AND e.start_date = '%s' AND e.end_date = '%s'" % syncvals)
			self.db.execute('COMMIT;')
				
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
			if debug:
				print('running a patch job')
			self.db.execute("SELECT %s FROM %s_gaps WHERE resolved = 'N' LIMIT 100" % (pk, opn))
			ids = [rec[0] for rec in self.db.fetchall()]
			if len(ids) == 0:
				break
			idjoin = 'OR %s = ' % (pk,)
			idstring = idjoin.join([str(id) for id in ids])
			self.db.execute("COMMIT;")

			qstring = "SELECT " + fieldstring + ' FROM ' + el + ' WHERE ' + pk + ' = ' + idstring
			r = self.session.query(qstring)
			self.db.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '%s_loader' ORDER BY ordinal_position" % opn.lower())
			header = [col[0] for col in self.db.fetchall()]
			data = r.list_results(header=header)
			self.db.execute('COMMIT;')
			if opn[-3:] == 'Rel':
				print('doing relation table fix')
				olddata = data
				data = []
				for consrecord in olddata:
					for grpid in consrecord[1]:
						print('appending a row')
						data.append([consrecord[0],grpid])
			results = StringIO('\n'.join(['\t'.join(row) for row in data]))
			print('uploading')
			self.db.copy_from(results,opn + '_loader',null='')
			self.db.execute("UPDATE %s_gaps g SET resolved = 'Y' WHERE %s = %s" % (el, pk, idstring))
			print('marking completed')
			self.db.execute("SELECT db_load('%s','insert')" % (el,))
			self.db.execute("COMMIT;")
			
#this was a concept that I toyed with but eventually abandoned for getting group memberships without creating a giant table.
#	def dl_group(self,groupid):
#		self.db.execute('SELECT wipe_group(%s);' % str(groupid))
#		self.db.execute('COMMIT');
#		(start_date, end_date) = ('2014-06-01', date.today().isoformat())
#		dump = self.query_status('ConsGroupRel','Constituent','update',start_date, end_date)
#		self.dbthread.start_date = start_date
#		self.dbthread.end_date = end_date
#		self.__query__('ConsGroupRel','Constituent','update',altwhere = 'WHERE groupid = %s' % str(groupid))
#		self.db.execute("SELECT db_load('ConsGroupRel','update')")
#		self.db.execute("DELETE FROM sync_ops WHERE opname = 'ConsGroupRel' AND start_date = '%s' AND end_date = '%s'" % (start_date, end_date))
#		self.db.execute("COMMIT;")
		
				
	def query_status(self, opn, el, op, start_date, end_date):
		print("fetching query status")
		print ("SELECT * FROM sync_event WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s'" % (opn,op,start_date,end_date))
		self.db.execute("SELECT * FROM sync_event WHERE opname = '%s' AND operation = '%s' AND start_date = '%s' AND end_date = '%s'" % (opn,op,start_date,end_date))	
		status = self.db.fetchone()
		self.db.execute('COMMIT;')
		if status is None:
			self.db.execute("INSERT INTO sync_event (opname, operation, start_date, end_date) VALUES ('%s','%s','%s','%s')" % (opn,op,start_date,end_date))
			self.db.execute("COMMIT;")
			return(None, 'N')
		else:
			try:
				return(int(status[4]), status[5])
			except TypeError:
				return(None, status[5])
			
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



class Download_Instructions():
	def __init__(self,soap, opn, el, op,fields,page,startdate=None,enddate=None,querytype=None,whereclause=None):
		self.soap = soap
		self.opn = opn
		self.el = el
		self.op = op
		self.fields = fields
		self.page = page
		self.querytype = querytype
		self.whereclause = whereclause
		self.startdate=startdate
		self.enddate=enddate
		