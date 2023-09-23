#connecting with mongoDB in localhost

#pymongo module for making connection wth mongoDB

#import first
from pymongo import MongoClient
#from bson.son import SON

# import compago
# app = compago.Application()
		
# @app.command
def insertbyparse():
	db, coll = startit()
	df = first.readings_df
	coll.insert(df,check_keys=False)


def startit():
	client = MongoClient("mongodb://localhost:27017/")
	db = client['laliga']
	# stats is the collection we have created.
	
	collections = db['stats']
	#using insert method to finally insert the data into our collection-stats of laliga database
	return db, collections
# @app.command
def createNew(club1 :str,club2:str):
	db, coll = startit()
	coll.insert_one({ 
	    "Div" : "SP1",
	    "Date" : "01/02/09",
	    "HomeTeam" : club1,
	    "AwayTeam" : club2,
	    "FTHG" : 3,
	    "FTAG" : 2,
	    "FTR" : "H",
	    "HTHG" : 2,
	    "HTAG" : 1,
	    "HTR" : "H",
	    "HS" : 28,
	    "AS" : 9,
	    "HST" : 11,
	    "AST" : 3,
	    "HF" : 18,
	    "AF" : 12,
	    "HC" : 10,
	    "AC" : 3,
	    "HY" : 2,
	    "AY" : 2,
	    "HR" : 0,
	    "AR" : 0,
	    "B365H" : 1.25,
	    "B365D" : 5.5,
	    "B365A" : 13.0,
	    "BWH" : 1.2,
	    "BWD" : 5.75,
	    "BWA" : 10.5,
	    "GBH" : 1.25,
	    "GBD" : 5.5,
	    "GBA" : 11.0,
	    "IWH" : 1.25,
	    "IWD" : 5.0,
	    "IWA" : 12.0,
	    "LBH" : 1.25,
	    "LBD" : 4.5,
	    "LBA" : 10.0,
	    "SBH" : 1.22,
	    "SBD" : 5.5,
	    "SBA" : 10.0,
	    "WHH" : 1.2,
	    "WHD" : 5.0,
	    "WHA" : 11.0,
	    "SJH" : 1.2,
	    "SJD" : 6.0,
	    "SJA" : 15.0,
	    "VCH" : 1.25,
	    "VCD" : 5.0,
	    "VCA" : 10.0,
	    "BSH" : 1.25,
	    "BSD" : 5.0,
	    "BSA" : 11.0,
	    "Bb1X2" : 31,
	    "BbMxH" : 1.26,
	    "BbAvH" : 1.24,
	    "BbMxD" : 6.6,
	    "BbAvD" : 5.65,
	    "BbMxA" : 15.0,
	    "BbAvA" : 11.75,
	    "BbOU" : 28,
	    "BbMxTwo" : 1.52,
	    "BbAvTwo" : 1.48,
	    "BbMxLess" : 2.9,
	    "BbAvLess" : 2.53,
	    "BbAH" : 19,
	    "BbAHh" : 0.0,
	    "BbMxAHH" : 9.0,
	    "BbAvAHH" : 1.92,
	    "BbMxAHA" : 11.0,
	    "BbAvAHA" : 8.69
	})
	pipeline = [{"$match":{"HomeTeam":"old12"}},{"$group":{"_id":{"_id":"$_id._id","home":"$HomeTeam","away":"$AwayTeam"}}}]
    # {"$sort": SON([("count", -1), ("_id", -1)])}
	cursor = coll.aggregate(pipeline)
	return print(list(cursor))

def crudf():
	db, coll = startit()
	requestDel = coll.delete_many({"HomeTeam":"old12"})
	print(requestDel)
	# requestUpd = coll.update_one({"HomeTeam":"Zaragoza"}, {"AwayTeam":"new12"}, upsert=True)

def adminka():
	print("im here")
	db,coll = startit()
	filter = {"filter":{"mechanisms":"SCRAM-SHA-256"}}
	result = db.getUsers(filter)
	for i in result:
		print("here is" + i )
#print(createNew("old12","new324"))
# if __name__ == "__main__":
#     app.run()

def usefilter(my_filter):
	db, coll = startit()
	db.list_collection_names(filter=my_filter)

	res = coll.find(my_filter)
	result = []
	for doc in res:
		del doc['_id']
		result.append(doc)
	return result

my_filter = {"HomeTeam": {"$in": ["Real Madrid", "Barcelona"]}}

#print(usefilter(	my_filter))
# crudf()
# adminka()
def indexing():
	db, coll = startit()
	resp = coll.create_index("HomeTeam")
	print("index response:", resp)

indexing()