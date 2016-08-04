from .local_settings import soap_path
import pickle

field_params_std = ['Name','Writable','Custom','Nillable','Multiple','Type','MaxLength','IsCriterion','IsWildcard']



def getname(field_obj):
	if type(field_obj) == str:
		return field_obj
	elif type(field_obj) == tuple:
		return field_obj[0]
	else:
		raise TypeError('Not string or tuple')

class Data_Element():
	"""Class for holding the descriptions of Luminate internal data elements.
	Presents two dictionaries, .ops for indicating what operations are and are not allowable on this Record type, and .fields, listing out the fields present in this data type."""
	def __init__(self,tree):
		self.name = tree.find('.//Result').find('Name').text
		self.ops = {}
		for op in tree.find('.//SupportedOperations'):
			self.ops[op.tag] = op.text
		self.fields = {}
		fieldnum = 1
		self.fieldsbynum = {}
		for field in tree.find('.//Result').iterfind('Field'):
			self.fields[field.find('Name').text] = DataField(field,fieldnum)
			self.fieldsbynum[fieldnum] = self.fields[field.find('Name').text]
			fieldnum +=1
	
	def prepsort(self,fields):
		fields.sort(key = lambda f: get_fieldsortkey(self,f))
		ret = []
		for (parent,field) in fields:
			if parent is None:
				ret.append(field)
			else:
				ret.append(parent + '.' + field)
		return ret

		
class DataField():
	"""Class for holding the description of Luminate data fields.
	Each object has a .characteristics dictionary indicating the name, datatype, and other field descriptors."""
	def __init__(self,field_elem,fieldnum):
		self.characteristics = {}
		self.num = fieldnum
		for fieldchar in field_params_std:
			try:
				assert field_elem.find(fieldchar) is not None
				self.characteristics[fieldchar] = field_elem.find(fieldchar).text
			except AssertionError:
				pass
		if field_elem.find('.//Option') is None:
			self.is_coded = False
		else:
			self.is_coded = True
			self.codes = {}
			for option in field_elem.findall('.//Option'):
				self.codes[option.find('Value').text] = option.find('Name').text
				
	def __getitem__(self,x):
		return self.characteristics[x]
	
	def parse(self,val):
		"""For luminate fields that use integers to encode string values, return the string value given the integer as an argument."""
		if self.is_coded:
			return self.codes[val]
		else:
			return val
		
def get_fieldsortkey(el_obj,fieldtuple):
	"""function generates a sortkey that places fields in an order that the Luminate interface will permit, using 
	information downloaded from Luminate about data structures."""
	(level1, level2) = fieldtuple
	if level1 is None:
		sortkey =  'c.0000' + str(el_obj.fields[level2].num)
		sortkey = sortkey[-4:]

	else:
		sortkey = '0000' + str(el_obj.fields[level1].num)
		sortkey = sortkey[-4:] + '.'
		secondel = recordtypes[el_obj.fields[level1]['Type']]
		sortkey2 = '0000' + str(secondel.fields[level2].num)
		sortkey += sortkey2[-4:]
		
	return sortkey		
		

			
try:
	with open(soap_path + 'record_descriptions.pk3','rb') as descr_file:
		recordtypes = pickle.load(descr_file)
except FileNotFoundError:
	recordtypes = {}