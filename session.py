
from .local_settings import *
from .exceptions import *
from .soap_message import SOAPLogin, SOAPRequest, SOAPQuery
from .utilities import element
from .interface_data import recordtypes as ifdrec
from .data_structures import Data_Element, DataField
import csv
import pickle
from os.path import isfile



class SOAPSession():
	write_open = False
	write_initialized = False
	"""Class representing a SOAP session, with functions for carrying on most interaction with the SOAP interface."""
	def __init__(self,username=soap_uname,pw=soap_pw):
		self.username = username
		self.pw = pw
		self.login()
		
	def login(self,username=None,pw=None):
		"""Login with a username and password.  In general the class will instantiate with the username and password
		from the local settings file, but this can be overridden using this function."""
		if username is None:
			username = self.username
		if pw is None:
			pw = self.pw
		sr = SOAPLogin(username,pw)
		self.session = sr.session
	
	def query(self,querytext,records=100,querypage=1):
		"""Deliver a SQL query to Luminate and return the SOAP Response object returned."""
		qr = SOAPQuery(self.session,querytext,records=records,querypage=querypage)
		return qr.response
		
	def request(self):
		"""Create a new SOAP Request object as part of this session."""
		return SOAPRequest(session=self.session)
		
	def start_sync(self,startdate,enddate):
		"""Starts a synchronization session with the SOAP API.
		Date parameters should be provided as iso-8601 formatted strings, e.g. 2015-12-31"""
		sr = self.request()
		sync = element(urn,'StartSynchronization',parent=sr.body)
		p = element(urn,'PartitionId',parent=sync,text=soap_partition)
		start = element(urn,'Start',parent=sync,text = startdate + 'T00:00:00+0000')
		end = element(urn,'End',parent=sync,text = enddate + 'T23:59:59+0000')
		sr.submit()
		
		self.syncid = sr.response.tree.find('.//SyncId').text
	
	def end_sync(self):
		"""Terminates a synchronization session"""
		sr = self.request()
		sync = element(urn,'EndSynchronization',parent=sr.body) 
		p = element(urn,'PartitionId',parent=sync,text=soap_partition)
		
		sr.submit()
		
		SyncID = None
		
	def _sync_op_checks(self,data_element,operation):
		"""Checks that a sync is active and that the operation requested is valid for the Record type in question before performing an operation."""
		try:
			assert self.syncid is not None
		except (AttributeError, AssertionError):
			raise SOAPClientError('Attempted sync operations without a sync session open')
		check_op_validity(data_element,operation)
		
	def getcount(self,data_element,optype):
		"""Requests a count of records changed during the time window of the current synchronization session.
		data_element may be any valid Record type from Luminate.
		optype must be 'insert', 'update', or 'delete'"""
		operation = syncsessiontags[optype] + 'Count'
		self._sync_op_checks(data_element,operation)
		sr = self.request()
		countreq = element(urn,operation,parent=sr.body)
		p = element(urn,'PartitionId',parent=countreq,text=soap_partition)
		rt = element(urn,'RecordType',parent=countreq,text=data_element)
		pg = element(urn,'Page',parent=countreq,text='1')
		ps = element(urn,'PageSize',parent=countreq,text='100')
		sr.submit()
		
		return int(sr.response.tree.find('.//RecordCount').text)
		
	def _prep_writefile(self,destfilename):
		"""Opens a file and csv writer object for write operations.
		Takes the argument destfilename, the path to the file."""
		if self.write_open:
			self._close_writefile()
		self.writefile = open(destfilename,'wt',newline='')
		self.writer = csv.writer(self.writefile,delimiter=',')
		self.write_open = True
		self.write_initialized = False
		
	def _append_writefile(self,reopenfilename):
		if self.write_open:
			self._close_writefile()
		try: 
			assert isfile(reopenfilename)
			self.writefile = open(reopenfilename,'at',newline='')
			self.writer = csv.writer(self.writefile,delimiter=',')
			self.write_open = True
			self.write_initialized =True
		except AssertionError:
			raise SOAPClientError('Writefile %s does not exist' % reopenfilename)
			
	def _close_writefile(self):
		self.writefile.close()
		del self.writer
		self.write_open = False
	
	def download_all(self,data_element,fields,dltype,destfilename):
		recordcount = self.getcount(data_element,dltype)
		dlpage = 1
		self._prep_writefile(destfilename)
		while (dlpage - 1) * 100 < recordcount:
			self.dl_write(data_element,fields,dltype,pagesize=100,page=dlpage)
			dlpage += 1
		self._close_writefile()
		
	def dl_write(self,data_element,fields,dltype,pagesize=100,page=1):
		dl = self.download(data_element,fields,dltype,pagesize=pagesize,page=page)
		if not self.write_initialized:
			self.writer.writerow(dl.results_header())
			self.write_initialized = True
		self.writer.writerows(dl.list_results())
			
	def download(self,data_element,fields,dltype,pagesize=200,page=1):
		"""Download records that were inserted/updated/deleted within the parameters of an active sync session.
		Because of pagination limits this will need to be iterated through to capture the full set of records available, if the number is greater than 200.
		data_element may be any valid Record type from Luminate.
		optype must be 'insert', 'update', or 'delete'"""
		operation = syncsessiontags[dltype]
		self._sync_op_checks(data_element,operation)
		sr = self.request()
		fields.sort(key=lambda f: recordtypes[data_element].fields[f].num)
		req = element(urn,operation,parent=sr.body)
		p = element(urn,'PartitionId',parent=req,text=soap_partition)
		rt = element(urn,'RecordType',parent=req,text=data_element)
		pg = element(urn,'Page',parent=req,text=str(page))
		ps = element(urn,'PageSize',parent=req,text=str(pagesize))
		for field in fields:
			fel = element(urn,'Field',parent=req,text=field)
		sr.submit()	
		if debug == True:
			print('Downloaded %s %s records, page %s of this record set.' % (str(pagesize),data_element,str(page)))
		
		return sr.response
		
				
	def gettypedescription(self,data_element):
		"""Requests the type description of a Record type from Luminate.
		Returns a Data_Element object of that type."""
		sr = self.request()
		request = element(urn,'DescribeRecordType',parent=sr.body)
		rt = element(urn,'RecordType',parent=request,text=data_element)
		sr.submit()
		return Data_Element(sr.response.tree)


def purge_descriptions():
	"""Updates the module-level saved dictionary of Luminate data elements and fields with a new call to the SOAP API."""
	ss = SOAPSession()
	global recordtypes
	recordtypes = {}
	for data_el in ifdrec:
		recordtypes[data_el] = ss.gettypedescription(data_el)
	with open('record_descriptions.pk3','wb') as descr_file:
		pickle.dump(recordtypes,descr_file,protocol=3)
	del ss
	

try:
	with open(soap_path + 'record_descriptions.pk3','rb') as descr_file:
		recordtypes = pickle.load(descr_file)
except (FileNotFoundError, AttributeError):
	recordtypes = purge_descriptions()


def check_op_validity(data_element,operation):
	"""Function that checks the module-level dictionary of Luminate record types to confirm whether the operation named by the operation argument
	is valid for the data element named by the data_element argument. 
	Raises a SOAPClientError if invalid, otherwise acts silently."""
	try:
		record_desc = recordtypes[data_element]
		assert record_desc.ops[operation.replace('Count','')] == 'true'
	except AssertionError:
		raise SOAPClientError('Attempted invalid operation %s on record type %s' % (operation, data_element))