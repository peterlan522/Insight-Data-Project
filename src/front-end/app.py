from flask import Flask, render_template
from flask_redis import Redis
import json, pickle
import time

app = Flask(__name__)
app.config['REDIS_HOST'] = '35.170.1.59'
app.config['REDIS_PORT'] = 6379
redis = Redis(app)

@app.route('/', methods=['GET'])
def homepage():
	return render_template('home.html', lines=enumerate(fill_table(), 1))

def fill_table():
	lines = []
	for line in redis.zrange('tweets', 0, 9):
		# line = line.strip('[]')
		one_row = list(line.split(","))
		tag = one_row[0]
		pos = one_row[1]
		neg = one_row[2]
		neu = one_row[3]
		row = [tag[3:-1], pos, neg, neu[:-1]]
		# one_row = one_row[0].strip('[\'')
		# one_row = one_row[3].strip(']')
		# lines.append(one_row)
		lines.append(row)
	return lines

if __name__ == '__main__':
	app.run(debug=True)
