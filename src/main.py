# -*- coding:utf-8 -*-
__author__ = 'yx'
import sys
sys.path.append('/usr/softwares/gears/')
from flask import Flask, request, jsonify
from flask.json import JSONEncoder
from datetime import datetime, timedelta
import pytz
from tasks import submit_task
from src.logger import logger
from src.config import config
import uuid
from src.database import db_conns
import json
import sys
reload(sys)

'''
implicit call method
'''
class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            loger = logger.Logger("CustomJSONEncoder.default")
            if isinstance(obj,datetime):
                return obj.astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        except TypeError as e:
            loger.print_error(str(e))
        return JSONEncoder.default(self, obj)

app = Flask(__name__)
app.json_encoder = CustomJSONEncoder
app.secret_key = 'PS#yio`%_!((f_or(%)))s'
app.permanent_session_lifetime = timedelta(minutes=30)

@app.route("/v1/import/submit", methods=['POST'])
def submit():
    try:
        if not request.json:
            return jsonify({'code':-1, 'message': "request is not json"})
        option = request.json
        loger.print_info(option)
        jobid = uuid.uuid1()
        option['jobid'] = str(jobid)
        importjob = db_conns.importjob_conn.add(jobid, "queued")
        submit_task.delay(json.dumps(option))
        return jsonify({'code':0, 'status':'queued', 'job_id': str(jobid)})
    except Exception as e:
        #the statement is to traceback exception info
        loger.print_error(str(e))
        return jsonify({'code': -1, 'messgae':e})

@app.route("/v1/import/status/<job_id>", methods=['GET'])
def describe(job_id):
    try:
        importjob = db_conns.importjob_conn.get(job_id)
        return jsonify({'code': 0, 'status':importjob.get("status"), 'finish_time':importjob.get('finish_time')})
    except Exception as e:
        return jsonify({'code': -1, 'message':e})


if __name__ == "__main__":
    loger = logger.Logger("MAIN")
    loger.print_info("[INFO]:DataCanvas Gears Server Started, Listen On Localhost:7395")
    app.run(host = '0.0.0.0', port=7395)
