
class SOAPError(Exception):
	"""Class only used for passing error messages received from the SOAP interface.
	Exclusively raised from the constructor of the SOAPResponse object.
	Contains as attributes the fault code and the fault string reported by the interface."""
	def __init__(self,msg,faultcode,faultstring):
		self.msg = msg
		self.faultcode = faultcode
		self.faultstring = faultstring
		

class SOAPClientError(Exception):
	"""Class used for passing errors related to incorrect requests on the client side of the API.
	This is the error passed when a request is detected to be incompatible with the SOAP API before submission."""
	def __init__(self,msg):
		self.msg = msg