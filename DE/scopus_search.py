import os
import json
import traceback
from pybliometrics.scopus import ScopusSearch, AbstractRetrieval
from tqdm import tqdm

def search_scopus(year):
    search_query = "( PUBYEAR = {} ) AND ( not AFFILORG ( chulalongkorn AND university ) ) AND ( AFFILCOUNTRY ( thailand ) )".format(year)
    print("Start searching for year: ", year)
    search = ScopusSearch(search_query)

    results = []

    for doc in tqdm(search.results[:200], desc="Year: {}".format(year)):
        try:
            doc_dict = doc._asdict()
            eid = doc_dict["eid"]
            ab = AbstractRetrieval(eid, view="FULL")

            data = {}
            data["eid"] = eid

            subjabbr = []
            for e in ab.subject_areas:
                subjabbr.append(e[1])

            data["subject_areas"] = subjabbr
            data["publication_year"] = year
            if (ab.abstract):
                data["length_of_abstract"] = len(ab.abstract.split())
            else:
                data["length_of_abstract"] = 0
            if (ab.authkeywords):
                data["author_keywords"] = ab.authkeywords
            else:
                data["author_keywords"] = ""

            data["sub_type"] = ab.subtype
            data["citation_count"] = ab.citedby_count
            data["refcount"] = ab.refcount

            results.append(data)

        except Exception as e:
            print("Error: ", e)
            print("Traceback: ", traceback.format_exc())

    return json.dumps(results)

if __name__ == "__main__":
    cur_path = os.path.dirname(os.path.realpath(__file__))
    for year in range(2018, 2024):
        folder_path = os.path.join(cur_path, "data")
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)

        data = search_scopus(year)

        with open(os.path.join(folder_path, "{}.json".format(year)), "w") as f:
            f.write(data)

# Path: DE/scopus_search.py
