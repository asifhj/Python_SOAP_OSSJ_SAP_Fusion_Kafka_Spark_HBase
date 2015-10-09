from SOAPpy import WSDL
wsdlFile = 'JVTTroubleTicketSession.wsdl'
server = WSDL.Proxy(wsdlFile)
server.methods.keys()