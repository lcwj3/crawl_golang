import pymongo
import requests
import json
import concurrent.futures
import datetime
import time
from datetime import datetime, timedelta
from lib.database import get_mongo_connection
mongo = get_mongo_connection()
MONGO_HOST = mongo.host
MONGO_PORT = int(mongo.port)
MONGO_USER = mongo.user
MONGO_PWD = mongo.password
MONGO_AUTH = mongo.auth_source
MONGO_DB = mongo.db
c = pymongo.MongoClient(MONGO_HOST, username=MONGO_USER,
                        password=MONGO_PWD, port=MONGO_PORT, authSource=MONGO_AUTH)
crawl = c['library-crawler']
print(crawl.collection_names())
golang = crawl['golang']
# res = {}
# try:
#     res = requests.get('https://go-search.org/api?action=packages')
#     res.raise_for_status()
# except Exception as e:
#     print(e)
# ids = json.loads(res.content.decode())
# count = len(ids)
# print(count)
# types = {}
# libraries = []
# for id in ids:
#     type = id.split('/')[0]
#     if type not in types:
#         types[type] = 1
#     else:
#         types[type] += 1
#     libraries.append({'id': id, 'address_type': type})
# for type, length in types.items():
#     print(type, length)
# with open('golang_type.json', 'w') as f:
#     json.dump(types, f)





limit = 10000
def download_libvers(skip):
    class MyHeaders:
        def __iter__(self):
            self._tokens = ['8fa2523f044c80916f0b8c5bfeae38f02450f70a',
                            '227c1f769e3536ad1d6444c12e7074ec6fa3fb96',
                            '857edc0ff8c5ed3f68961be1368d684ba1754f7e']
            self._size = len(self._tokens)
            self._i = 0
            return self

        def __next__(self):
            self._i = (self._i + 1) % self._size
            return {'Authorization': f'token {self._tokens[self._i]}'}

    headerIter = iter(MyHeaders())

    headers = next(headerIter)

    def get_last_week():
        return (datetime.now() - timedelta(days=7)).isoformat()
    def get_requests(url, headers, headerIter):
        #global headers, headerIter

        while True:
            try:
                res = requests.get(url, headers=headers)
            except Exception as e:
                print(e)
                time.sleep(60)
                continue

            if 'X-RateLimit-Remaining' not in res.headers:
                time.sleep(60)
                headers = next(headerIter)
            else:

                rate_limit_remaining = int(res.headers["X-RateLimit-Remaining"])
                if rate_limit_remaining % 100 == 0:
                    print(f'{datetime.now()} requests left: {rate_limit_remaining}')
                if rate_limit_remaining < 10:
                    headers = next(headerIter)
                break

        return res

    c1 = pymongo.MongoClient(MONGO_HOST, username=MONGO_USER,
                            password=MONGO_PWD, port=MONGO_PORT, authSource=MONGO_AUTH)
    crawl1 = c1['library-crawler']
    print(crawl.collection_names())
    go = crawl1['golang']
    bad_url = crawl1['golang_bad_url']
    count = 0
    with requests.Session() as session:
        for lib in go.find(no_cursor_timeout=True).skip(skip).limit(limit):
            print(count + skip, lib['id'])
            count += 1
            if lib['address_type'] != 'github.com':
                continue
            id_info = lib['id'].split('/')
            type = id_info[0]
            owner = id_info[1]
            repo = id_info[2]
            libraryurl = f'https://api.github.com/repos/{owner}/{repo}'
            try:
                res = get_requests(libraryurl, headers, headerIter)
                res.raise_for_status()
            except Exception as e:
                print(e)
                bad_url.insert({'url': libraryurl, 'error': str(e)})
                continue
            lib_content = json.loads(res.content.decode())
            tagurl = f'https://api.github.com/repos/{owner}/{repo}/tags'
            try:
                res2 = get_requests(tagurl, headers, headerIter)
                res2.raise_for_status()
                lib_content['versions'] = json.loads(res2.content.decode())
                lib_content['id'] = lib['id']
                lib_content['address_type'] = lib['address_type']
                go.update({'id': lib['id']}, lib_content)
            except Exception as e:
                print(e)


#golang.insert(libraries, check_keys=False)


with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    # future: name
    insert_docs = []
    future_name = {executor.submit(download_libvers, file): file for file in range(0, golang.count(), limit)}
    for future in concurrent.futures.as_completed(future_name):
        print('Done!')
