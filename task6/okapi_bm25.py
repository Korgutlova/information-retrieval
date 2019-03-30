import numpy as np

from connnect.ConnectionDB import ConnectionDB
from task3.query_execution import preprocessing_query
from task5.query_with_relevant_docs import get_id_term, select_needed_docs, get_url, get_doc_term

db = ConnectionDB()
cursor = db.cursor
count_docs = "SELECT id FROM articles"
select_needed_docs = "SELECT distinct(article_id) FROM public.article_term WHERE term_id in (%s)"
select_docs_term_count = "SELECT count(distinct(article_id, term)) FROM public.words_mystem WHERE term=%s"
count_terms = "SELECT count(*) FROM public.words_mystem"
cursor.execute(count_docs)
documents = cursor.fetchall()
D = len(documents)
cursor.execute(count_terms)
avgdl = cursor.fetchall()[0][0] / D


def get_len(id):
    select_term = "SELECT term FROM public.words_mystem WHERE article_id=%s"
    cursor.execute(select_term, [id])
    return len(cursor.fetchall())


def get_idf5(word):
    cursor.execute(select_docs_term_count, [word])
    small_d = int(cursor.fetchall()[0][0])
    idf5 = np.log((D - small_d + 0.5) / (small_d + 0.5))
    return idf5


def get_freq(d, q):
    select_term = "SELECT term FROM public.words_mystem WHERE article_id=%s and term=%s"
    cursor.execute(select_term, [d, q])
    return len(cursor.fetchall())


if __name__ == "__main__":
    text = "Создание кино интересными людьми"
    query = preprocessing_query(text)
    query = [q for q in query if q != ' ' and q != ' \n']

    terms = []
    docs_dict = []

    for q in query:
        terms.append([get_id_term(q), get_idf5(q), q])
    cursor.execute(select_needed_docs % ",".join('\'{0}\''.format(s[0]) for s in terms))

    k_1 = 1.2
    b = 0.75

    for d in cursor.fetchall():
        score = 0
        for t in terms:
            f = get_freq(d, t[2])
            elem = t[1] * (k_1 + 1) * f / (f + k_1 * (1 - b + b * get_len(d) / avgdl))
            if elem > 0:
                score += elem
        docs_dict.append((d, score))
    docs_dict.sort(key=lambda x: x[1])
    docs_dict = list(reversed(docs_dict))[:10]

    print("По запросу '%s' было найдено : \n" % text)
    for i in range(len(docs_dict)):
        print("%s) url: %s , score(D, Q) - %s" % (i + 1, get_url(docs_dict[i][0]), docs_dict[i][1]))
    db.close()
