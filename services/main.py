import json
import os
import sys
import traceback

import urllib3
from bottle import route, request, run, response, HTTPResponse
import logging
import atexit
import time
from apscheduler.schedulers.background import BackgroundScheduler

LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)


def get_record_by_id(id):
    id = str(id)
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r') as db:
            data = json.load(db)
        if id in data.keys():
            return data[id]
    return None


def create_or_update_record(id, body):
    if not os.path.exists(DB_FILE):
        data = {id: body}
        with open(DB_FILE, 'w+') as db:
            json.dump(data, db)
        return
    else:
        # data = {'{}~'.format(id): body}
        # with open(TEMP_DB_FILE, 'a') as db:
        #     db.write(data)
        # json.dump(data, db)
        with open(DB_FILE, 'r') as db:
            data = json.load(db)
        data[id] = body
        with open(DB_FILE, 'w') as db:
            json.dump(data, db)
        # if '{}~'.format(id) in data.keys():
        #     del data['{}~'.format(id)]


def delete_record_by_id(id):
    id = str(id)
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r') as db:
            data = json.load(db)
        if id in data.keys():
            del data[id]
            with open(DB_FILE, 'w') as db:
                json.dump(data, db)
            return True
        else:
            return False
    else:
        return False


@route('/', method='GET')
@route('/', method='OPTIONS')
def index():
    """index"""
    return '<h1>In-memory DB is running!</h1>'


@route('/<id>', method='GET')
def get_record(id):
    record = get_record_by_id(id)
    if record:
        return record
    else:
        return HTTPResponse(
            status=404,
            body=json.dumps('Record {} not found'.format(id)))


@route('/<id>', method='PUT')
def put_record(id):
    try:
        body = request.json
        create_or_update_record(id, body)
    except:
        LOGGER.error(traceback.format_exc())
        return HTTPResponse(
            status=400,
            body=json.dumps('Error in adding or updating record {}'.format(id))
        )


@route('/<id>', method='DELETE')
def delete_record(id):
    is_deleted = delete_record_by_id(id)
    if is_deleted:
        return '<h1>{} successfully deleted!</h1>'.format(id)
    else:
        return HTTPResponse(
            status=404,
            body=json.dumps('Record {} not found'.format(id)))


def ping_proxy():
    port = 8084
    data = {
        'url': f'http://127.0.0.1:{port}',
        'checked_at': time.time()
    }
    send_data = json.dumps(data)
    try:
        r = http.request('POST', f'http://127.0.0.1:8080/_check_node', body=send_data,
                         headers={'Content-Type': 'application/json'})
        node_number = json.loads(r.data)
        if node_number is not None:
            return node_number
    except Exception as err:
        LOGGER.error('Something wrong with Proxy')
        LOGGER.error(err)
    return None


if __name__ == '__main__' and len(sys.argv) > 2:
    port = sys.argv[1]
    is_master = sys.argv[2]
    if port != 8080:
        if is_master:
            http = urllib3.PoolManager()
            node_number = ping_proxy()
            print(node_number)
            if node_number is None:
                exit(-1)
            DB_FILE = os.path.join('.', 'data', f'node_{node_number}')

            LOGGER.info(f'Service started at port {port}')
            scheduler = BackgroundScheduler()
            scheduler.add_job(func=ping_proxy, trigger="interval", seconds=5)
            scheduler.start()

        run(host='127.0.0.1', port=port, server='gunicorn', reload=True, debug=True)

        atexit.register(lambda: scheduler.shutdown())
    else:
        pass
        LOGGER.error(f'Port {port} is running with proxy-server')
