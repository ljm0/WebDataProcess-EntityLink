import requests, json



QUERY="""
    select distinct ?abstract where {
    ?s <http://www.w3.org/2002/07/owl#sameAs> <http://rdf.freebase.com/ns/%s> .
    ?s <http://www.w3.org/2002/07/owl#sameAs> ?o .
    ?o <http://dbpedia.org/ontology/abstract> ?abstract .
}"""
# NOTWORK:filter langMatches( lang(?abstract), "EN"  )

def sparql(domain, query):
    url = 'http://%s/sparql' % domain
    response = requests.post(url, data={'print': True, 'query': query})
    if response:
        try:
            response = response.json()
            for binding in response.get('results', {}).get('bindings', []):
                abstract = binding.get('abstract',{}).get('value')
                # english version
                if abstract[-3:-1] == "en":
                    return abstract
        except Exception as e:
            print(reponse)
            raise e

def query_abstract(domain, freebaseId):
    key = freebaseId[1:].replace("/",".")
    q = QUERY % key
    try:
        abstract = sparql(domain,q)
        return abstract
    except Exception as e:
        # import sys
        # sys.exit(0)
        return None


if __name__ == '__main__':
    import sys
    try:
        _, DOMAIN = sys.argv
    except Exception as e:
        print('Usage: python sparql.py DOMAIN QUERY')
        sys.exit(0)
    key = "/m/0br5f6"
    k = key[1:].replace("/",".")
    q = QUERY % k
    print(sparql(DOMAIN,q))
