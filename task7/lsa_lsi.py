import numpy as np
from numpy.linalg import svd

from connnect.ConnectionDB import ConnectionDB
from task3.query_execution import preprocessing_query
from task5.query_with_relevant_docs import get_id_term, select_docs_term_count, get_idf, select_needed_docs, \
    cos_measure, get_url
from task6.okapi_bm25 import get_freq

db = ConnectionDB()
cursor = db.cursor


def get_terms():
    cursor.execute("SELECT term_id, term_text FROM public.term_list")
    return cursor.fetchall()


if __name__ == "__main__":
    text = "Создание кино интересными людьми"
    query = preprocessing_query(text)
    query = [q for q in query if q != ' ' and q != ' \n']

    terms = []

    for q in query:
        terms.append(get_id_term(q))

    # get documents in which there is at least one word
    cursor.execute(select_needed_docs % ",".join('\'{0}\''.format(s) for s in terms))

    # rank
    k = 5

    # build matrix of docs
    docs = cursor.fetchall()
    all_terms = get_terms()
    matrix = []

    for t in all_terms:
        new_arr = []
        for d in docs:
            new_arr.append(get_freq(d, t[1]))
        matrix.append(new_arr)

    # build vector of query
    q = []
    for t1 in all_terms:
        flag = True
        for t2 in terms:
            if t1[0] == t2:
                q.append(1)
                flag = False
                break
        if flag:
            q.append(0)

    # svd
    U, S, V = svd(matrix, full_matrices=False)
    S = np.diag(S)
    S = S[:k, :k]
    U = U[:, :k]
    V = np.transpose(V)[:, :k]
    q = np.dot(np.dot(q, U), np.linalg.inv(S))

    # calc cos measure
    result = []
    for i in range(V.shape[0]):
        result.append((docs[i], cos_measure(q, V[i])))

    result.sort(key=lambda x: x[1])
    result = list(reversed(result))[:10]

    # output
    print("По запросу '%s' было найдено : \n" % text)
    for i in range(len(result)):
        print("%s) url: %s , lsa_lsi - %s" % (i + 1, get_url(result[i][0]), result[i][1]))
    db.close()
