import math

import numpy as np

from connnect.ConnectionDB import ConnectionDB
from task3.query_execution import preprocessing_query

db = ConnectionDB()
cursor = db.cursor
count_docs = "SELECT id FROM articles"
select_needed_docs = "SELECT distinct(article_id) FROM public.article_term WHERE term_id in (%s)"
cursor.execute(count_docs)
documents = cursor.fetchall()
D = len(documents)


def cos_measure(a, b):
    sum = 0
    power_a = 0
    power_b = 0
    for i in range(len(a)):
        sum += a[i] * b[i]
        power_a += a[i] * a[i]
        power_b += b[i] * b[i]
    return sum / (math.sqrt(power_a) * math.sqrt(power_b))


def get_idf(term):
    select_docs_term_count = "SELECT count(distinct(article_id, term)) FROM public.words_mystem WHERE term=%s"
    cursor.execute(select_docs_term_count, [term])
    small_d = int(cursor.fetchall()[0][0])
    idf = np.log(D / small_d)
    return idf


def get_id_term(term):
    select_term = "SELECT term_id FROM public.term_list WHERE term_text=%s"
    cursor.execute(select_term, [term])
    return cursor.fetchall()[0][0]


def get_idf_term_doc(doc, term, i):
    select_doc = "SELECT article_id FROM public.article_term WHERE term_id=%s and article_id=%s"
    cursor.execute(select_doc, [term, doc])
    result = cursor.fetchall()
    if len(result) == 0:
        return 0
    return query_vector[i]


def get_url(id):
    select_url = "SELECT url FROM articles WHERE id=%s"
    cursor.execute(select_url, [id])
    return cursor.fetchall()[0][0]


if __name__ == "__main__":
    text = "Создание кино интересными людьми"
    query = preprocessing_query(text)
    query = [q for q in query if q != ' ' and q != ' \n']

    terms_id = []
    query_vector = []
    docs_dict = {}

    for q in query:
        query_vector.append(get_idf(q))
        terms_id.append(get_id_term(q))
    cursor.execute(select_needed_docs % ",".join('\'{0}\''.format(s) for s in terms_id))

    for d in cursor.fetchall():
        docs_dict[d[0]] = []

    for i in range(len(terms_id)):
        for key in docs_dict.keys():
            docs_dict.get(key).append(get_idf_term_doc(key, terms_id[i], i))

    result = []
    for key in docs_dict.keys():
        result.append((key, cos_measure(query_vector, docs_dict.get(key))))
    result.sort(key=lambda x: x[1])
    result = list(reversed(result))[:10]

    print("По запросу '%s' было найдено : \n" % text)
    for i in range(len(result)):
        print("%s) url: %s , cos_measure - %s" % (i + 1, get_url(result[i][0]), result[i][1]))
    db.close()
