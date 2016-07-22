
import lxml.etree as ET			
from .local_settings import ns, soap_path
from .interface_data import recordtypes as ifdrec
from datetime import date
import dateutil.parser as dp
from time import mktime

def element(namespace, tag, parent=None, text=None):
	"""Quick constructor function, shorthand for the various components of the lxml element constructors.
	ns = the namespace of the element to be constructed
	tag = the tag for the element
	parent = the parent element object, if any
	text = any interior text for this element"""
	elstr = '{' + namespace + '}' + tag
	if parent is None:
		el = ET.Element(elstr,nsmap=ns)
	else:
		el = ET.SubElement(parent,elstr, nsmap = ns)
	if text is not None:
		el.text = text
	return el
		
def log(message):
	with open(soap_path + 'logfile_' + date.today().isoformat() + '.txt','at',newline='') as logfile:
		logfile.write(message+'\n')

def isodate_to_jsdate(isodate):
	pydate = dp.parse(isodate)
	unixdate = mktime(pydate.timetuple())
	jsdate = unixdate * 1000
	return jsdate
	