from lxml import etree
from time import time
import datetime
import xml.etree.ElementTree as ET
from infi.clickhouse_orm import models as md
from infi.clickhouse_orm import fields as fd
from infi.clickhouse_orm import engines as en
from infi.clickhouse_orm.database import Database

class Visits(md.Model):
# describes datatypes and fields
	idSite=fd.UInt64Field()
	idVisit = fd.UInt64Field()
	visitIp=fd.StringField(default='none')
	visitorId=fd.StringField()
	goalConversions=fd.UInt64Field()
	siteCurrency=fd.StringField()
	siteCurrencySymbol=fd.StringField()
	serverDate=fd.DateField()
	visitServerHour=fd.UInt64Field()
	lastActionTimestamp=fd.UInt64Field()
	lastActionDateTime=fd.StringField()
	userId=fd.StringField()
	visitorType=fd.StringField()
	visitorTypeIcon=fd.StringField()
	visitConverted=fd.UInt64Field()
	visitConvertedIcon=fd.StringField()
	visitCount=fd.UInt64Field()
	firstActionTimestamp=fd.UInt64Field()
	visitEcommerceStatus=fd.StringField()
	visitEcommerceStatusIcon=fd.StringField()
	daysSinceFirstVisit=fd.UInt64Field()
	daysSinceLastEcommerceOrder=fd.UInt64Field()
	visitDuration=fd.UInt64Field()
	visitDurationPretty=fd.StringField()
	searches=fd.UInt64Field()
	actions=fd.UInt64Field()
	interactions=fd.UInt64Field()
	referrerType=fd.StringField()
	referrerTypeName=fd.StringField()
	referrerName=fd.StringField()
	referrerKeyword=fd.StringField()
	referrerKeywordPosition=fd.UInt64Field()
	referrerUrl=fd.StringField()
	referrerSearchEngineUrl=fd.StringField()
	referrerSearchEngineIcon=fd.StringField()
	languageCode=fd.StringField()
	language=fd.StringField()
	deviceType=fd.StringField()
	deviceTypeIcon=fd.StringField()
	deviceBrand=fd.StringField()
	deviceModel=fd.StringField()
	operatingSystem=fd.StringField()
	operatingSystemName=fd.StringField()
	operatingSystemIcon=fd.StringField()
	operatingSystemCode=fd.StringField()
	operatingSystemVersion=fd.StringField()
	browserFamily=fd.StringField()
	browserFamilyDescription=fd.StringField()
	browser=fd.StringField()
	browserName=fd.StringField()
	browserIcon=fd.StringField()
	browserCode=fd.StringField()
	browserVersion=fd.StringField()
	events=fd.UInt64Field()
	continent=fd.StringField()
	continentCode=fd.StringField()
	country=fd.StringField()
	countryCode=fd.StringField()
	countryFlag=fd.StringField()
	region=fd.StringField()
	regionCode=fd.StringField()
	city=fd.StringField()	
	location=fd.StringField()
	latitude=fd.Float64Field()
	longitude=fd.Float64Field()
	visitLocalTime=fd.StringField()
	visitLocalHour=fd.UInt64Field()
	daysSinceLastVisit=fd.UInt64Field()
	customVariables=fd.StringField()
	resolution=fd.StringField()
	plugins=fd.StringField()
	pluginsIcons=fd.StringField()
	provider=fd.StringField()
	providerName=fd.StringField()
	providerUrl=fd.StringField()
	dimension1=fd.StringField()
	campaignId=fd.StringField()
	campaignContent=fd.StringField()
	campaignKeyword=fd.StringField()
	campaignMedium=fd.StringField()
	campaignName=fd.StringField()
	campaignSource=fd.StringField()
	serverTimestamp=fd.UInt64Field()
	serverTimePretty=fd.StringField()
	serverDatePretty=fd.StringField()
	serverDatePrettyFirstAction=fd.StringField()
	serverTimePrettyFirstAction=fd.StringField()
	totalEcommerceRevenue=fd.Float64Field()
	totalEcommerceConversions=fd.UInt64Field()
	totalEcommerceItems=fd.UInt64Field()
	totalAbandonedCartsRevenue=fd.Float64Field()
	totalAbandonedCarts=fd.UInt64Field()
	totalAbandonedCartsItems=fd.UInt64Field()
# creating an sampled MergeTree
	engine = en.MergeTree('serverDate', ('idSite','idVisit','visitIp','visitorId','goalConversions','siteCurrency','siteCurrencySymbol','serverDate','visitServerHour',
'lastActionTimestamp','lastActionDateTime','userId','visitorType','visitorTypeIcon','visitConverted','visitConvertedIcon',
'visitCount','firstActionTimestamp','visitEcommerceStatus','visitEcommerceStatusIcon','daysSinceFirstVisit','daysSinceLastEcommerceOrder',
'visitDuration','visitDurationPretty','searches','actions','interactions','referrerType','referrerTypeName','referrerName','referrerKeyword',
'referrerKeywordPosition','referrerUrl','referrerSearchEngineUrl','referrerSearchEngineIcon','languageCode','language','deviceType','deviceTypeIcon','deviceBrand','deviceModel',
'operatingSystem','operatingSystemName','operatingSystemIcon','operatingSystemCode','operatingSystemVersion','browserFamily','browserFamilyDescription',
'browser','browserName','browserIcon','browserCode','browserVersion','events','continent','continentCode',
'country','countryCode','countryFlag','region','regionCode','city','location','latitude','longitude','visitLocalTime','visitLocalHour',
'daysSinceLastVisit','customVariables','resolution','plugins','pluginsIcons','provider','providerName','providerUrl','dimension1','campaignId',
'campaignContent','campaignKeyword','campaignMedium','campaignName','campaignSource','serverTimestamp','serverTimePretty','serverDatePretty',
'serverDatePrettyFirstAction','serverTimePrettyFirstAction','totalEcommerceRevenue','totalEcommerceConversions','totalEcommerceItems','totalAbandonedCartsRevenue',
'totalAbandonedCarts','totalAbandonedCartsItems'))
class Hits(md.Model):
	idVisit = fd.UInt64Field()
	Type=fd.StringField()
	goalName=fd.StringField(default='none')
	goalId=fd.UInt64Field()
	revenue=fd.UInt64Field()
	goalPageId=fd.StringField(default='none')
	url=fd.StringField(default='none')
	pageTitle=fd.StringField(default='none')
	pageIdAction=fd.UInt64Field(default=0)
	serverTimePretty=fd.StringField()
	pageId=fd.UInt64Field(default=0)
	generationTimeMilliseconds=fd.UInt64Field(default=0)
	generationTime=fd.StringField()
	interactionPosition=fd.UInt64Field(default=0)
	icon=fd.StringField(default='none')
	timestamp=fd.UInt64Field(default=0)
	Date=fd.DateField(default=datetime.datetime(2017,2,2))
	engine = en.MergeTree('Date', ('idVisit','Type','goalName','goalId','revenue','goalPageId','url','pageTitle','pageIdAction','serverTimePretty',
'pageId','generationTimeMilliseconds','generationTime','interactionPosition','icon','timestamp',))	
def safely_get_data(element, key):
	try:
		for child in element:
			if child.tag == key:
				return child.text
				
	except:
		return "not found"

def parse_clickhouse_xml(filename, db_name, db_host):
	visits_buffer =[]
	hits_buffer =[]
	tree = ET.parse('log2.xml')
	root = tree.getroot()
	#for i in root:
		#print(i.tag)
	for result in root:
		
	
		
		idSite=safely_get_data(result, 'idSite') 
		idVisit=safely_get_data(result, 'idVisit') 
		visitIp=safely_get_data(result, 'visitIp')  
		visitorId=safely_get_data(result, 'visitorId') 
		goalConversions =safely_get_data(result, 'goalConversions') 
		siteCurrency=safely_get_data(result, 'siteCurrency') 
		siteCurrencySymbol=safely_get_data(result, 'siteCurrencySymbol') 
		serverDate=safely_get_data(result, 'serverDate') 
		visitServerHour=safely_get_data(result, 'visitServerHour') 
		lastActionTimestamp =safely_get_data(result, 'lastActionTimestamp') 
		lastActionDateTime =safely_get_data(result, 'lastActionDateTime') 
		userId=safely_get_data(result, 'userId')		
		visitorType= safely_get_data(result, 'visitorType')		
		visitorTypeIcon=safely_get_data(result, 'visitorTypeIcon') 		
		visitConverted =safely_get_data(result, 'visitConverted') 		
		visitConvertedIcon =safely_get_data(result, 'visitConvertedIcon')		
		visitCount =safely_get_data(result, 'visitCount') 
		firstActionTimestamp =safely_get_data(result, 'firstActionTimestamp') 
		visitEcommerceStatus =safely_get_data(result, 'visitEcommerceStatus') 
		visitEcommerceStatusIcon =safely_get_data(result, 'visitEcommerceStatusIcon') 
		daysSinceFirstVisit =safely_get_data(result, 'daysSinceFirstVisit') 
		daysSinceLastEcommerceOrder =safely_get_data(result, 'daysSinceLastEcommerceOrder') 
		visitDuration =safely_get_data(result, 'visitDuration') 
		visitDurationPretty =safely_get_data(result, 'visitDurationPretty') 
		searches =safely_get_data(result, 'searches') 
		actions =safely_get_data(result, 'actions') 
		interactions =safely_get_data(result, 'interactions') 
		referrerType =safely_get_data(result, 'referrerType') 
		referrerTypeName =safely_get_data(result, 'referrerTypeName') 
		referrerName =safely_get_data(result, 'referrerName') 
		referrerKeyword =safely_get_data(result, 'referrerKeyword') 
		referrerKeywordPosition =safely_get_data(result, 'referrerKeywordPosition') 
		referrerUrl =safely_get_data(result, 'referrerUrl')
		referrerSearchEngineUrl=safely_get_data(result, 'referrerSearchEngineUrl')		
		referrerSearchEngineIcon =safely_get_data(result, 'referrerSearchEngineIcon')
		languageCode =safely_get_data(result, 'languageCode') 	
		language =safely_get_data(result, 'language') 
		deviceType =safely_get_data(result, 'deviceType') 
		deviceTypeIcon =safely_get_data(result, 'deviceTypeIcon') 
		deviceBrand =safely_get_data(result, 'deviceBrand') 
		deviceModel =safely_get_data(result, 'deviceModel') 
		operatingSystem =safely_get_data(result, 'operatingSystem') 
		operatingSystemName =safely_get_data(result, 'operatingSystemName') 
		operatingSystemIcon =safely_get_data(result, 'operatingSystemIcon') 
		operatingSystemCode =safely_get_data(result, 'operatingSystemCode')
		operatingSystemVersion =safely_get_data(result, 'operatingSystemVersion') 
		browserFamily =safely_get_data(result, 'browserFamily') 
		browserFamilyDescription =safely_get_data(result, 'browserFamilyDescription') 
		browser =safely_get_data(result, 'browser') 
		browserName =safely_get_data(result, 'browserName') 
		browserIcon =safely_get_data(result, 'browserIcon') 
		browserCode =safely_get_data(result, 'browserCode')
		browserVersion =safely_get_data(result, 'browserVersion') 
		events =safely_get_data(result, 'events') 
		continent =safely_get_data(result, 'continent') 
		continentCode=safely_get_data(result, 'continentCode') 
		country =safely_get_data(result, 'country') 
		countryCode =safely_get_data(result, 'countryCode') 
		countryFlag =safely_get_data(result, 'countryFlag') 
		region =safely_get_data(result, 'region') 
		regionCode =safely_get_data(result, 'regionCode') 
		city =safely_get_data(result, 'city')
		location =safely_get_data(result, 'location') 
		latitude =safely_get_data(result, 'latitude') 
		longitude =safely_get_data(result, 'longitude') 
		visitLocalTime =safely_get_data(result, 'visitLocalTime') 
		visitLocalHour =safely_get_data(result, 'visitLocalHour') 
		daysSinceLastVisit =safely_get_data(result, 'daysSinceLastVisit') 
		customVariables =safely_get_data(result, 'customVariables') 
		resolution =safely_get_data(result, 'resolution') 
		plugins =safely_get_data(result, 'plugins') 
		pluginsIcons =safely_get_data(result, 'pluginsIcons')
		provider =safely_get_data(result, 'provider') 
		providerName =safely_get_data(result, 'providerName') 
		providerUrl =safely_get_data(result, 'providerUrl')
		dimension1 =safely_get_data(result, 'dimension1') 
		campaignId =safely_get_data(result, 'campaignId') 
		campaignContent =safely_get_data(result, 'campaignContent')
		campaignKeyword =safely_get_data(result, 'campaignKeyword') 
		campaignMedium =safely_get_data(result, 'campaignMedium')
		campaignName =safely_get_data(result, 'campaignName')
		campaignSource =safely_get_data(result, 'campaignSource') 
		serverTimestamp=safely_get_data(result, 'serverTimestamp') 
		serverTimePretty=safely_get_data(result, 'serverTimePretty') 
		serverDatePretty=safely_get_data(result, 'serverDatePretty') 
		serverDatePrettyFirstAction =safely_get_data(result, 'serverDatePrettyFirstAction') 
		serverTimePrettyFirstAction =safely_get_data(result, 'serverTimePrettyFirstAction') 
		totalEcommerceRevenue =safely_get_data(result, 'totalEcommerceRevenue') 
		totalEcommerceConversions =safely_get_data(result, 'totalEcommerceConversions') 
		totalEcommerceItems =safely_get_data(result, 'totalEcommerceItems') 
		totalAbandonedCartsRevenue=safely_get_data(result, 'totalAbandonedCartsRevenue') 
		totalAbandonedCarts =safely_get_data(result, 'totalAbandonedCarts') 
		totalAbandonedCartsItems=safely_get_data(result, 'totalAbandonedCartsItems')
		strings=[visitIp,visitorId,siteCurrency,siteCurrencySymbol,lastActionDateTime,userId,visitorType,visitorTypeIcon,visitConvertedIcon,visitEcommerceStatus,visitEcommerceStatusIcon,
visitDurationPretty,referrerType,referrerTypeName,referrerName,referrerKeyword,referrerUrl,referrerSearchEngineUrl,referrerSearchEngineIcon,languageCode,language,deviceType,deviceTypeIcon,deviceBrand,deviceModel,
operatingSystem,operatingSystemName,operatingSystemIcon,operatingSystemCode,operatingSystemVersion,browserFamily,browserFamilyDescription,
browser,browserName,browserIcon,browserCode,browserVersion,continent,continentCode,
country,countryCode,countryFlag,region,regionCode,city,location,visitLocalTime,customVariables,resolution,plugins,pluginsIcons,provider,providerName,providerUrl,dimension1,campaignId,
campaignContent,campaignKeyword,campaignMedium,campaignName,campaignSource,serverTimePretty,serverDatePretty,
serverDatePrettyFirstAction,serverTimePrettyFirstAction]
		ints=[idSite,idVisit,goalConversions,visitServerHour,lastActionTimestamp,visitConverted,visitCount,firstActionTimestamp,daysSinceFirstVisit,daysSinceLastEcommerceOrder,visitDuration,searches,actions,
interactions,referrerKeywordPosition,events,latitude,longitude,visitLocalHour,daysSinceLastVisit,serverTimestamp,totalEcommerceRevenue,totalEcommerceConversions,totalEcommerceItems,totalAbandonedCartsRevenue,
totalAbandonedCarts,totalAbandonedCartsItems,]
		for i in range(len(strings)):
			if strings[i] == None:
				strings[i]='none'
		for i in range(len(ints)):
			if ints[i] == None:
				ints[i]=0
		for  res in result.find('actionDetails'):
			Type=safely_get_data(res, 'type')
			goalName=safely_get_data(res, 'goalName')	
			goalId=safely_get_data(res, 'goalId')	
			revenue=safely_get_data(res, 'revenue')	
			goalPageId=safely_get_data(res, 'goalPageId')			
			url=safely_get_data(res, 'url')		
			pageTitle=safely_get_data(res, 'pageTitle')		
			pageIdAction=safely_get_data(res, 'pageIdAction')		
			serverTimePretty=safely_get_data(res, 'serverTimePretty')		
			pageId=safely_get_data(res, 'pageId')		
			generationTimeMilliseconds=safely_get_data(res, 'generationTimeMilliseconds')		
			generationTime=safely_get_data(res, 'generationTime')		
			interactionPosition=safely_get_data(res, 'interactionPosition')		
			icon=safely_get_data(res, 'icon')		
			timestamp=safely_get_data(res, 'timestamp')
			Astrings=[Type,goalName,goalPageId,url,pageTitle,serverTimePretty,generationTime,icon]
			Aints=[goalId,revenue,pageIdAction,pageId,generationTimeMilliseconds,interactionPosition,timestamp]
			for i in range(len(Astrings)):
				if Astrings[i] == None:
					Astrings[i]='none'
			for i in range(len(Aints)):
				if Aints[i] == None:
					Aints[i]=0
			insert_hits = Hits(
				idVisit=ints[1],
				Type=Astrings[0],
				goalName=strings[1],
				goalId=Aints[0],
				revenue=Aints[1],
				goalPageId=strings[2],
				url=Astrings[3],
				pageTitle=Astrings[4],
				pageIdAction=Aints[2],
				serverTimePretty=Astrings[5],
				pageId=Aints[3],
				generationTimeMilliseconds=Aints[4],
				generationTime=Astrings[6],
				interactionPosition=Aints[5],
				icon=Astrings[7],
				timestamp=Aints[6],
				)
		
	# appends data into couple
			hits_buffer.append(insert_hits)
			res.clear()
# inserting data into clickhouse model representation
		insert_visits = Visits(
			idSite=ints[0],
			idVisit=ints[1],
			visitIp=strings[0],
			visitorId=strings[1],
			goalConversions=ints[2],
			siteCurrency=strings[2],
			siteCurrencySymbol=strings[3],
			serverDate=serverDate,
			visitServerHour=ints[3],
			lastActionTimestamp=ints[4],
			lastActionDateTime=strings[4],
			userId=strings[5],
			visitorType=strings[6],
			visitorTypeIcon=strings[7],
			visitConverted=ints[5],
			visitConvertedIcon=strings[8],
			visitCount=ints[6],
			firstActionTimestamp=ints[7],
			visitEcommerceStatus=strings[9],
			visitEcommerceStatusIcon=strings[10],
			daysSinceFirstVisit=ints[8],
			daysSinceLastEcommerceOrder=ints[9],
			visitDuration=ints[10],
			visitDurationPretty=strings[11],
			searches=ints[11],
			actions=ints[12],
			interactions=ints[13],
			referrerType=strings[12],
			referrerTypeName=strings[13],
			referrerName=strings[14],
			referrerKeyword=strings[15],
			referrerKeywordPosition=ints[14],
			referrerUrl=strings[16],
			referrerSearchEngineUrl=strings[17],
			referrerSearchEngineIcon=strings[18],
			languageCode=strings[19],
			language=strings[20],
			deviceType=strings[21],
			deviceTypeIcon=strings[22],
			deviceBrand=strings[23],
			deviceModel=strings[24],
			operatingSystem=strings[25],
			operatingSystemName=strings[26],
			operatingSystemIcon=strings[27],
			operatingSystemCode=strings[28],
			operatingSystemVersion=strings[29],
			browserFamily=strings[30],
			browserFamilyDescription=strings[31],
			browser=strings[32],
			browserName=strings[33],
			browserIcon=strings[34],
			browserCode=strings[35],
			browserVersion=strings[36],
			events=ints[15],
			continent=strings[37],
			continentCode=strings[38],
			country=strings[39],
			countryCode=strings[40],
			countryFlag=strings[41],
			region=strings[42],
			regionCode=strings[43],
			city=strings[44],
			location=strings[45],
			latitude=ints[16],
			longitude=ints[17],
			visitLocalTime=strings[46],
			visitLocalHour=ints[18],
			daysSinceLastVisit=ints[19],
			customVariables=strings[1],
			resolution=strings[47],
			plugins=strings[48],
			pluginsIcons=strings[49],
			provider=strings[50],
			providerName=strings[51],
			providerUrl=strings[52],
			dimension1=strings[53],
			campaignId=strings[54],
			campaignContent=strings[55],
			campaignKeyword=strings[56],
			campaignMedium=strings[57],
			campaignName=strings[58],
			campaignSource=strings[59],
			serverTimestamp=ints[20],
			serverTimePretty=strings[60],
			serverDatePretty=strings[61],
			serverDatePrettyFirstAction=strings[62],
			serverTimePrettyFirstAction=strings[63],
			totalEcommerceRevenue=ints[21],
			totalEcommerceConversions=ints[22],
			totalEcommerceItems=ints[23],
			totalAbandonedCartsRevenue=ints[24],
			totalAbandonedCarts=ints[25],
			totalAbandonedCartsItems=ints[26],
		
				)
		visits_buffer.append(insert_visits)
		result.clear()
	

# open database with database name and database host values
	db = Database(db_name, db_url=db_host)
# create table to insert prepared data
	db.create_table(Visits)
# insert prepared data into database
	db.insert(visits_buffer)
	db.create_table(Hits)
	db.insert(hits_buffer)
	#for visits in db.select("SELECT * FROM test2.visits", model_class=Visits):
		#print(visits.idVisit,visits.dimension1,visits.city,visits.region,visits.operatingSystem)
if __name__ == '__main__':
	parse_clickhouse_xml(
		'log.xml',
		'CHdatabase',
		'http://85.143.172.199:8123')
    
    
    
    
    



