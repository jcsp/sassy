import logging
from common import DEFAULT_PERIODS
from flask import Flask, render_template
import pymongo

app = Flask(__name__)
app.logger.addHandler(logging.StreamHandler())

@app.route('/ui/')
def view():
    stat_id = 0
    conn = pymongo.Connection()
    db = conn.journaldb
    charts = []
    for period in DEFAULT_PERIODS:
        collection_name = "rollup_%s" % period
        chart = {
            'name': collection_name,
            'data': []
            }
        for result in db[collection_name].find({'s_id': stat_id}):
            chart['data'].append((result['t'] * 1000, result['v']))
        charts.append(chart)

    return render_template('sassy.html', charts = charts)

if __name__ == '__main__':
    app.run()