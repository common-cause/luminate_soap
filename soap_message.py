

import requests
from .utilities import element
from .exceptions import SOAPError
from .local_settings import *
import re
import lxml.etree as ET		

class SOAPRequest():
	"""Generalized class for constructing and submitting SOAP Requests.
	Optional arguments:
	session - the ID of the session
	parent - the SOAPSession object initialized with that session id. 
	
	Both optional arguments are provided automatically if the object is created by an existing, logged in SOAP session."""
	def __init__(self,session='',parent=None):
		self.envelope = element(soap,'Envelope')
		self.parent = parent
		self.tree = ET.ElementTree(self.envelope)
		self.header = element(soap,'Header',parent=self.envelope)
		if parent is not None:
			self.parent = parent
		if session != '':
			s = element(soap,'Session',parent=self.header)
			self.sid = element(soap,'SessionId',parent=s,text=session)
		self.body = element(soap,'Body',parent=self.envelope)
		
	def submit(self):
		"""Submit the SOAP Request.  The response received is a SOAPResponse object stored as the response attribute of the SOAPRequest object."""
		self.parent.lock.acquire()
		try:
			result = requests.post(soap_endpoint,ET.tostring(self.tree.getroot()))
			self.parent.lock.release()
		except:
			self.parent.lock.release()
			raise
		stripns1 = re.sub(' xmlns(?:\:[^"]+)?="[^"]+"','',result.text)
		stripns2 = re.sub('\<\w+\:','<',stripns1)
		stripns3 = stripns2.replace('xsi:','')
		stripns4 = stripns3.replace('ens:','')
		stripns5 = stripns4.replace('fns:','')
		for unicode in ['\x81','\x82','\x83','\x84','\x85','\x86','\x87','\x88','\x89','\x80','\x8a','\x8b','\x8c','\x8d','\x8e','\x8f','\x91','\x92','\x93','\x94','\x95','\x96','\x97','\x98','\x99','\x90','\x9a','\x9b','\x9c','\x9d','\x9e','\x9f']:
			stripns5 = stripns5.replace(unicode,'')
		stripns = re.sub('\</\w+\:','</',stripns5)
		
		self.xmltext = stripns
		try:
			self.response = SOAPResponse(stripns.encode('utf-8'))
		except ET.XMLSyntaxError:
			print(self.xmltext)
			
		#this is where we're going to catch errors fed back to us by the SOAP API
		
		try:
			assert self.response.tree.find('.//Fault') is None
		except AssertionError:
			faultcode = self.response.tree.find('.//faultcode').text
			faultstring = self.response.tree.find('.//faultstring').text
			if faultcode == 'SESSION':
				try:
					assert self.parent.loginfail
				except (AssertionError, AttributeError):
					self.parent.loginfail = True
					self.parent.login()
					self.sid.text = self.parent.session
					self.submit()
			else:
				raise SOAPError(faultcode + ' fault during request submission',faultcode,faultstring)
		try:
			self.parent.loginfail = False
		except AttributeError:
			pass
		
		
class SOAPQuery(SOAPRequest):	
	"""Specialized class of SOAP request for queries."""
	def __init__(self,session,querytext,records=100,querypage=1):
		super().__init__(session=session)
		self.query = element(urn,'Query',parent=self.body)
		qt = element(urn,'QueryString',parent=self.query,text=querytext)
		qp = element(urn,'Page',parent=self.query,text=str(querypage))
		qs = element(urn,'PageSize',parent=self.query,text=str(records))
		self.submit()

class SOAPLogin(SOAPRequest):
	"""Specialized class of SOAP Request for processing logins."""
	def __init__(self,username,pw,parent=None):
		super().__init__(parent=parent)
		login = element(soap,'Login',parent=self.body)
		u = element(urn,'UserName',parent=login,text=username)
		p = element(urn,'Password',parent=login,text=pw)
		self.submit()
		self.session = self.response.tree.find('.//SessionId').text
		

class SOAPResponse():
	"""General purpose class for parsing returned xml from Luminate SOAP.
	Contains functions designed specifically for parsing results returns into field headers, a list of data rows,
	and a list of parsed data rows where we need to decode Luminate value codes."""
	def __init__(self,soapxml):
		self.tree = ET.fromstring(soapxml)
	
	def results_header(self):
		"""Returns the list of field names downloaded in this SOAP Response"""
		return [el.tag for el in self.tree.find('.//Record').iter() if (el.text is not None and not re.match('\n\s+',el.text)) or el.get('nil') == 'true']
	
	def list_results(self):
		"""Returns a list of the records in the xml document, with each record as a list of the values in that record.  
		Values are presented in the same order as fields in the field header.
		Values are not decoded; integer codes in Luminate are presented as integers."""
		results = []
		for rec in self.tree.iterfind('.//Record'):
			row = []
			for el in rec.iter():
				if el.text is not None and not re.match('\n\s+',el.text):
					row.append(el.text)
				elif el.get('nil') == 'true':
					row.append('')
				else:
					pass
			results.append(row)
		return results
	
	