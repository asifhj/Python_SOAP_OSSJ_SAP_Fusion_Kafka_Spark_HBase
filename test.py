__author__ = 'asifj'
import json
d = {"srId":"2015-1016-T-0017","createDate":None,"lastModifiedDate":None,"closedDate":None,"processType":"ZTEC","processTypeDesc":"Technical Service Request","desc":"testing SM, AM  stamp","betaType":None,"escalationKey":"0","escalationDesc":None,"courtesy":None,"courtesyKey":None,"secVulnerability":None,"productId":"ACX4000BASE-AC","serialNumber":"JR0213040116","status":"Dispatch","statusKey":"E0004","reason":"Newly Created","priority":"P3 - Medium","priorityKey":"3","severity":None,"severityKey":"00","criticalOutage":None,"productSeries":"ACX","platform":"ACX4000","release":"JUNOS12.3X51","version":"D11.3","software":"Select","specialRelease":None,"srCat1":None,"srCat2":None,"srCat3":None,"srCat4":None,"prodSeriesTech":"ACX","techCat1":"Synchronization","techCat2":None,"techCat3":None,"jsaAdvisoryBoard":None,"cve":None,"cvss":None,"smeContact":None,"jtac":None,"sirtBundle":None,"reporterDetails":None,"externallyReported":None,"urgency":None,"urgencyKey":None,"entitlement":{"entitlementChecked":"X","entitledSerialNumber":"JR0213040116","serviceProduct":"SRV001","entitlementServiceLevel":"Unlimited JTAC 24*7","entitlementSource":"Service Contract","sku":"PAR-AR5-ACX4000","startDate":"1969-12-31","endDate":"1969-12-31","contractStatus":"Active","contractId":"C1-2848829959","warrantyEndDate":"1969-12-31"},"outage":{"outageKey":None,"outage":None,"outageInfoAvailable":None,"outageCausekey":None,"ouatgeCause":None,"totalOutageTime":"00000000","outageTypekey":None,"outageType":None,"overideOutage":None,"outageImpactKey":None,"outageImpact":None,"numOfSystemsAffected":None,"numOfUsersAffected":None,"zzq1":None,"zzq2":None,"zzq3":None,"zzq4":None,"zzq5":None,"zzq6":None,"zzq7":None,"zzq8":None,"zzq9":None,"zzq10":None,"criticalIssue":None,"escalationLevelkey":"0","escalationLevel":None,"internalUse":None,"previousTeam":None,"previousOwnerSkill":None,"support24X7":None,"knowledgeArticle":None,"raFa":None,"ccCustomer":None,"ccEngineer":None,"routerName":None,"top5":None,"build":None,"custCaseNo":None,"viaKey":"ZWE","via":"Web Portal","followUpMethod":"Email Full Text Update","followUpMethodkey":"EFUL","theaterkey":"2","theater":"AMER","temperature":None,"country":"MX","outsourcer":None},"partnerFunctions":[{"partnerFunctionName":"Service Manager","partnerFunctionKey":"ZSRVMGR","partnerID":"0000003259","partnerName":"Jonathan Pflaum"},{"partnerFunctionName":"Account Manager","partnerFunctionKey":"ZACCTMGR","partnerID":"0000003328","partnerName":"Adam Rypinski"},{"partnerFunctionName":"Account Manager","partnerFunctionKey":"ZACCTMGR","partnerID":"0000026304","partnerName":"Darlene Cruz"},{"partnerFunctionName":"Reporter (Person)","partnerFunctionKey":"00000151","partnerID":"200341147","partnerName":"Daniel Schatte"},{"partnerFunctionName":"Responsible Group","partnerFunctionKey":"00000099","partnerID":"0089512573","partnerName":"PSACXL1 JTAC-ACX-JTAC L1"},{"partnerFunctionName":"Sold-To Party","partnerFunctionKey":"00000001","partnerID":"100076977","partnerName":"TELMEX"}],"dates":[],"attachments":[],"caseNotes":[],"kbLinks":[]}

print json.dumps(d, indent=4, sort_keys=True)