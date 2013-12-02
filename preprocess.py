import os, json, MySQLdb
from bs4 import BeautifulSoup
from lib.mysql import connection
from lib.datacleaner import DataClean

DATA_FOLDER = "data/"
PROCESSED_DATA_FOLDER = "processed-data/"
TRAIN_DATA = "data/train/"

cursor = connection.cursor()

dirlist = os.listdir(DATA_FOLDER)
for d in dirlist:
	f = open(DATA_FOLDER+d, "r")
	soup = BeautifulSoup(f.read().lower().replace('<body>', '<no-body>').replace('</body>', '</no-body>'))
	f.close()
	docs = soup.findAll("reuters")
	
	for doc in docs:
		# f = open(TRAIN_DATA+d, "w")
		text = doc.find("text")
		cats = []
		if doc.topics.d:
			cats.append('TOPICS')
		if doc.places.d:
			cats.append('PLACES')
		if doc.orgs.d:
			cats.append('ORGS')
		if doc.exchanges.d:
			cats.append('EXCHANGES')
		if doc.companies.d:
			cats.append('COMPANIES')
		if doc.people.d:
			cats.append('PEOPLE')
		print cats
		try:
			data = str(text.find('no-body')).replace('<no-body>', '').replace('</no-body>','').split('.\n')
			print len(data)
			sents = []
			for d in data:
				cleand = DataClean(d).getData()
				# print cleand
				sents.append(cleand)
			query = "insert into `r21578`(did, newid, type, cats, data) values(%s, %s, '%s', '%s', '%s')" \
					%(doc['oldid'], doc['newid'], doc['lewissplit'], json.dumps(cats), json.dumps(sents).replace('\'', '\\\''))
			cursor.execute(query)
			connection.commit()
		except Exception,e:
			print str(e)