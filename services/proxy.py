import glob
import json
import logging
import os
import time
import asyncio

import urllib3
from apscheduler.schedulers.background import BackgroundScheduler
from bottle import route, run, HTTPError, request

http = urllib3.PoolManager()
HEADERS = {'Content-Type': 'application/json'}

LOGGER = logging.getLogger(__name__)
handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
LOGGER.addHandler(handler)
LOGGER.setLevel(logging.INFO)


@route('/', method='GET')
@route('/', method='OPTIONS')
def index():
    """index"""
    return '<h1>In-memory DB is running!</h1>'


@route('/<id>', method='GET')
def get_record(id):
    try:
        node_url = get_node_url(id)
        r = http.request('GET', node_url + f'/{id}', headers=HEADERS)
    except HTTPError as er:
        print(er)


@route("/<id>", method='PUT')
def put_record(id):
    try:
        body = request.json
        node_url = get_node_url(id)
        r = http.request('PUT', node_url + f'/{id}', body=json.dumps(body), headers=HEADERS)
    except HTTPError as er:
        print(er)


def get_node_url(id):
    with open(os.path.join('..', 'data', 'nodes_list'), 'r') as nodes_list:
        data = json.load(nodes_list)
    nodes_count = len(data)
    node_number = int(id) % nodes_count
    return data[str(node_number)]['url']


@route('/<id>', method='DELETE')
def delete_record(id):
    try:
        current_nodes = get_nodes_list()
        if OLD_NODES_NUMBER == current_nodes:
            node_url = get_node_url(id)
            r = http.request('DELETE', node_url + f'/{id}', headers=HEADERS)
    except HTTPError as er:
        print(er)


def check_nodes_list(body):
    nodes_file = os.path.join('..', 'data', 'nodes_list')
    data = {}
    print(body)
    node_number = -1
    if not os.path.exists(nodes_file):
        with open(nodes_file, 'w+') as db:
            data[0] = body
            json.dump(data, db)
        return
    else:
        with open(nodes_file, 'r') as db:
            data = json.load(db)
        now = time.time()
        delete_nodes = []
        is_node_checked = False
        for key, value in data.items():
            if data[key]['url'] == body['url']:
                data[key] = body
                node_number = key
                is_node_checked = True
            elif int(now - data[key]['checked_at']) > 9:
                delete_nodes.append(key)

        if not is_node_checked:
            new_key = int(list(data.keys())[-1]) + 1
            data[new_key] = body
            node_number = new_key

        for node in delete_nodes:
            del data[node]
        with open(nodes_file, 'w') as db:
            json.dump(data, db)
        return node_number


def do_replica(node):
    pass


@route('/_check_node', method='POST')
def check_nodes():
    body = request.json
    died_nodes = check_nodes_list(body)
    return died_nodes
    # if died_nodes:
    #     for node in died_nodes:
    #         do_replica(node)


def get_nodes_list():
    with open(os.path.join('..', 'data', 'nodes_list'), 'r') as nodes_file:
        data = json.loads(nodes_file)
    return len(data)


def resharding():
    new_nodes_count = get_nodes_list()
    new_data = {}
    if OLD_NODES_NUMBER != new_nodes_count:
        for i in range(OLD_NODES_NUMBER):
            node_data_path = os.path.join('..', 'data', f'node_{i}')
            with open(node_data_path, 'r') as node_data:
                nodes_data = json.loads(node_data)
            for key, value in nodes_data.items():
                new_node = key % new_nodes_count
                new_data[new_node] = {key: value}
        for key, value in new_data:
            with open(os.path.join('..', 'data', f'node_{key}'), 'w') as db:
                json.dump(new_data[key], db)


if __name__ == '__main__':
    OLD_NODES_NUMBER = get_nodes_list()

    LOGGER.info(f'Proxy started at port 8080')
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=resharding, trigger='interval', seconds=30)
    scheduler.start()
    run(host='127.0.0.1', port=8080, server='gunicorn', reload=True, debug=True)
