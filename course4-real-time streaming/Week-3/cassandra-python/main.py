from datetime import datetime
from cassandra.cluster import Cluster

cluster = Cluster(['localhost'])
session = cluster.connect()
session.set_keyspace('forex')

get_currency_statement = session.prepare("SELECT * FROM currency_pairs WHERE pair = ?")
save_currency_statement = session.prepare("INSERT INTO currency_pairs (pair, description) VALUES (?, ?)")
get_prices_state = session.prepare("SELECT * FROM exchange_rates WHERE pair = ? AND time < ? LIMIT ?")

def get_currency_pair(pair):
	rows = session.execute(get_currency_statement, [pair])
	if not rows:
		return None

	return rows[0]

def save_currency_pair(pair, description):
	session.execute(save_currency_statement, [pair, description])

def get_prices(pair, end_timestamp, limit):
	return list(session.execute(get_prices_state, [pair, end_timestamp, limit]))

print(get_currency_pair('EUR/USD'))
print(get_currency_pair('???'))

save_currency_pair('CHF/PLN', 'Frank to Zloty')
print(get_currency_pair('CHF/PLN'))

print(get_prices('USD/EUR', datetime(2017, 2, 3, 4, 7), 4))