
import datetime
import mysql.connector
from pymongo import MongoClient
from bson import ObjectId
import bson

configMeta = {
'user': 'capillary',
'password': '123',
'host': '192.168.10.33',
'database':'extended_fields_custom_data'
}

configMaster = {
'user': 'capillary',
'password': '123',
'host': '192.168.10.33',
'database':'masters'
}

ipMongo = '192.168.10.33'

mongoCollection = 'customer_extended_fields'

def getNewEfValueId(oldEfValueId, efId):

	cnx = mysql.connector.connect(**configMeta)
	cursor = cnx.cursor()

	query = ("select value from  ef_"+str(efId)+"_extended_fields_values where id = "+str(oldEfValueId))


	cursor.execute(query)

	nameIdDict={}
	value = []
	for value in cursor:
		print ("id from old ef value table is ")
	  	print(value)
	  	nameIdDict[value]=oldEfValueId
	  	break

	if(len(value)>0):
		query = ('select id from  extended_fields_values where value = "'+str(value[0])+'"')

		cursor.execute(query)

	id = []
	for id in cursor:
		print ("id from new  ef value table is ")
		print id
		break



	cursor.close()
	cnx.close()
	return id



if __name__ == "__main__":


	cnx = mysql.connector.connect(**configMaster)
	cursor = cnx.cursor()

	query = ("select name,id from extended_fields where datatype = 'standard_string'")

	cursor.execute(query)

	nameIdDict={}

	for (name, id) in cursor:
	  print(name )
	  print(id)
	  nameIdDict[name]=id

	print("list of all standerd string extended fields are below\n:")
	print(nameIdDict)

	mongoclient = MongoClient(ipMongo+':27017')
	db = mongoclient.multi_profile


	f = open("efupdate.txt","a")
	i=0
	try:
		empCol = db[mongoCollection].find()
		print "\n All data from %s mongo \n"%(mongoCollection)
		for emp in empCol:
			print emp["orgId"]
			orgId = emp["orgId"]
			for ef in emp["extendedFields"]:
				print "debug   name :"+ef["name"]
				if ef["name"] in nameIdDict:
					# print  ef["value"]
					print "calling for  efid :"+str(nameIdDict[ef["name"]])+" value id :"+str(ef["value"])
					id = getNewEfValueId(ef["value"], nameIdDict[ef["name"]])
					print "id is "+ str(id)
					if( len(id) > 0):
						## update the doc here 
						update = db[mongoCollection].update({"_id":ObjectId(emp["_id"]),"extendedFields.name":ef["name"]},{"$set":{"extendedFields.$.value":bson.Int64(id[0])}})
						print(update)
						i=i+1
						f.write("updated doc %d : %s  new %s old %s\r\n" %((i),emp["_id"], str(id),ef["value"]))

						# print ameIdDict[ef["name"]]

	except Exception, e:
		print str(e)
		f.write("Exception is %s\r\n" %str(e))



	cursor.close()
	cnx.close()


	

