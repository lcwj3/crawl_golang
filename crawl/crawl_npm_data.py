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
token_sum = ['8fa2523f044c80916f0b8c5bfeae38f02450f70a',
                        #'227c1f769e3536ad1d6444c12e7074ec6fa3fb96',
                        #'857edc0ff8c5ed3f68961be1368d684ba1754f7e',
                        'b7e5dfa43ff5f8c8155a5013d05a0ef77ca3333d',
                        '8fb74577e9b5b17b86874f027ce63e8495fe170b',
                        'e84fe24cfc248960ca1874e9b6ceb0ba87187810',
                        '346a1b2d9851acc3e48eaa4c0e301aea157067a9',
                        'a559ed22b2251c9610f76953ada82dbcec39046d',
                        '37b82ea6ea7b27bd4ed34110da4d9d98584780c0',
                        'a6c4463f6e6e6f3cadf4e73d857d8e1a7522f436',
                        '9106ba17407302eba34e988cfd91c7fdf8d2255f',
                        '6999ac33990ba6afd5ab84f76bef0f2a6b451b12',
                        '36603ea05df8db280790b573985d0c0893c626d2',
                        'ba9014de304ce71fd778a02d7a49b11aa77f8db2',
                        '8025a761f899045d223c5ab53ce8a7813b513d0a',
                        '78120bdeb7388d621a61d9b6ce5d1bee7513e358',
                        '21bc4c27aad36a658e1db566c393a060aa69c2db',
                        '154c5170f7dbde5f7ea9d3b2bc3169c6448b17c5',
                        '55864f5b0b790ac2aa6f339879ad0a63d1a8f92c',
                        'fbe59b545228db8c45dec190eb238526d51b5ab6',
                        'dfd77a3156bbda50db6359e5eb7b9f8a2ac19952',
                        'b35514da16461e474459d1a471aba946b87c7de3',
                        '78b5df5d68cf483201f3e27be9c048a4b5415617',
                        '956044832e65ad4e6805c95a2e86fee951d5a651',
                        '92d1d7124b19a9b5f61c88f0e61c1e26a4b8ea5c',
                        '4879df7378a057afa5f14d029d7750d9ccf8b7fb',
                        'f3c3c234f5a44ffbb1d6c55d4c671485236485ef',
                        '6a3bda63ddd5ae10de007d801afa93f5acdd4efa',
                        '8a092dbf1fdd06c4b5887b07f3d710b58436c563',
                        'aed2f4be3cc42ff498e9c3f81657eb2167a91336'
                        ]
class MyHeaders():
    def __init__(self, t):
        self._tokens = t
    def __iter__(self):
        self._size = len(self._tokens)
        self._i = 0
        return self

    def __next__(self):
        self._i = (self._i + 1) % self._size
        return {'Authorization': f'token {self._tokens[self._i]}'}





def get_requests(url, headers, headerIter):
    # global headers, headerIter

    while True:
        try:
            res = requests.get(url, headers=headers)
            res.raise_for_status()
        except Exception as e:
            raise Exception(e)

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

    return res, headers


partition = int(int(golang.count())/12 + 1)

def download_libvers(skip):
    headerIter = iter(MyHeaders(token_sum[skip * 2: (skip + 1) * 2]))

    headers = next(headerIter)
    c1 = pymongo.MongoClient(MONGO_HOST, username=MONGO_USER,
                            password=MONGO_PWD, port=MONGO_PORT, authSource=MONGO_AUTH)
    crawl1 = c1['library-crawler']
    print(crawl1.collection_names())
    go = crawl1['golang']
    bad_url = crawl1['golang_bad_url']
    count = 0
    for lib in go.find(no_cursor_timeout=True).skip(skip * partition).limit(partition):
        count += 1
        if 'is_updated' in lib and lib['is_updated']:
            continue
        if lib['address_type'] != 'github.com':
            go.update({'id': lib['id']}, {'$set':{'is_updated':True}})
            continue
        id_info = lib['id'].split('/')
        type = id_info[0]
        owner = id_info[1]
        repo = id_info[2]
        libraryurl = f'https://api.github.com/repos/{owner}/{repo}'
        try:
            res, headers = get_requests(libraryurl, headers, headerIter)
            res.raise_for_status()
        except Exception as e:
            print(e, '1234')
            bad_url.insert({'url': libraryurl, 'error': str(e)})
            go.update({'id': lib['id']}, {'$set': {'is_updated': True}})
            continue
        lib_content = json.loads(res.content.decode())
        tagurl = f'https://api.github.com/repos/{owner}/{repo}/releases'
        try:
            res2, headers = get_requests(tagurl, headers, headerIter)
            res2.raise_for_status()
            lib_content['versions'] = json.loads(res2.content.decode())
            lib_content['id'] = lib['id']
            lib_content['address_type'] = lib['address_type']
            lib_content['is_updated'] = True
            go.update({'id': lib['id']}, lib_content)
        except Exception as e:
            print(e)
            continue
        print(count + skip * partition, lib['id'])
#golang.insert(libraries, check_keys=False)


with concurrent.futures.ThreadPoolExecutor(max_workers=12) as executor:
    # future: name
    insert_docs = []
    future_name = {executor.submit(download_libvers, file): file for file in range(12)}
    for future in concurrent.futures.as_completed(future_name):
        print('Done!')
