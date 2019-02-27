import json
import os

from bottle import HTTPError

from services.main import delete_record
import urllib3


DB_FILE = os.path.join('data', 'db')
PORT = 8080
CONNECTION_STRING = 'http://127.0.0.1:{}'.format(PORT)
HEADERS = {'Content-Type': 'application/json'}
http = urllib3.PoolManager()


def clear_data():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)


def save_data(data):
    try:
        for i in range(1, len(data) + 1):
            send_data = json.dumps(data[i])
            r = http.request('PUT', CONNECTION_STRING + f'/{i}', body=send_data, headers={'Content-Type': 'application/json'})
            if r.status == 404:
                print(r.message)
    except HTTPError as er:
        print(er)


def check_saved_data(data):
    try:
        for i in range(1, len(data) + 1):
            r = http.request('GET', CONNECTION_STRING + '/{}'.format(i), headers=HEADERS)
            if r.status == 404 or json.loads(r.data) != data[i]:
                return False
        return True
    except HTTPError as er:
        print(er)
        return False


def delete(id):
    try:
        r = http.request('DELETE', CONNECTION_STRING + '/{}'.format(id), headers=HEADERS)
    except HTTPError as er:
        print(er)


def check_deleted(id):
    try:
        r = http.request('GET', CONNECTION_STRING + '/{}'.format(id), headers=HEADERS)
    except HTTPError as er:
        print(er)
        return False
    if r.status == 404:
        return True
    return False


def run_test(port):
    # global PORT = port
    clear_data()
    # start_service(port)
    data = {}
    for i in range(1, 150):
        data[i] = {'first_field': 'record {}'.format(i),
                   'second_field': 'number {}'.format(i*100 - 10)}
    save_data(data)
    saved = check_saved_data(data)
    print(saved)
    for i in range(1, 150):
        data[i] = {'first_field': 'record {}'.format(i),
                   'second_field': 'number zero'}
    save_data(data)
    updated = check_saved_data(data)
    print(updated)
    delete_record(10)
    deleted = check_deleted(10)
    print(deleted)


if __name__ == '__main__':
    run_test(8080)

