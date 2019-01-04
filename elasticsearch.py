import requests

# def getNewElasticsearchURL():
#     tempP = os.popen('sh ./getElasticsearchURL.sh')
#     domain=tempP.read()
#     NEW_URL = 'http://%s/freebase/label/_search' % domain
#     print("my elascticsearch_url: =======================", NEW_URL)
#     tempP.close()
#     return NEW_URL

#search: get FreebaseID from elasticsearch
def search(domain, query):
    url = 'http://%s/freebase/label/_search' % domain
    try:
        response = requests.get(url, params={'q': query, 'size':10})
    except:
        # import sys
        # sys.exit(0)
        #url = getNewElasticsearchURL()
        return {}
    id_labels = {}
    # res = []
    if response:
        response = response.json()
        for hit in response.get('hits', {}).get('hits', []):
            freebase_label = hit.get('_source', {}).get('label')
            freebase_id = hit.get('_source', {}).get('resource')
            #score = hit.get('_score', 0)
            id_labels.setdefault(freebase_id, set()).add( freebase_label )
            #res.append((freebase_id, freebase_label ,score))
    return id_labels


if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN, QUERY = sys.argv
    except Exception as e:
        print('Usage: python kb.py DOMAIN QUERY')
        sys.exit(0)

    for entity, labels in search(DOMAIN, QUERY).items():
        print (entity,labels)
    # print(elasticsearch(DOMAIN, QUERY))

