import json, MySQLdb
from lib.dpattern import DP
from lib.mysql import connection

cursor = connection.cursor()
cursor2 = connection.cursor()
query = "select id, did, data from r21578 where type='test' and dp = ''"
cursor.execute(query)
cursor.scroll(0, 'absolute')
dp = DP(0.3)
while True:
	c = cursor.fetchone()
	if not c:
		break
	print json.loads(c[-1]), c[1]
	# break
	dps = dp.d_patterns(json.loads(c[-1]))
	print dps
	query = "update data set dp = '%s' where id = %s" %(json.dumps(dps).replace('\'', '\\\''), str(c[0]))
	cursor2.execute(query)
	connection.commit()