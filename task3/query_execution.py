from nltk import word_tokenize
from nltk.corpus import stopwords
from pymystem3 import Mystem

from connnect.ConnectionDB import ConnectionDB

db = ConnectionDB()
cursor = db.cursor


def preprocessing_query(sent):
    list = []
    m = Mystem()
    stop_words = stopwords.words('russian')
    stop_words.extend([',', '.', '—', '–', '-', '«', '»', '?', '!', '(', ')', ';', ':', '”', '“', '\''])
    words = word_tokenize(sent)
    for w in words:
        word = w.lower()
        if word not in stop_words:
            list.append(m.lemmatize(word)[0])
    return list


def execute(text):
    query = preprocessing_query(text)
    select_query = "SELECT a_t.article_id FROM public.article_term as a_t join public.term_list as t_l " \
                   "on a_t.term_id=t_l.term_id WHERE t_l.term_text=%s;"
    select_article = "SELECT url FROM public.articles WHERE id=%s"
    list_term_doc = []

    for q in query:
        cursor.execute(select_query, [q])
        docs = []
        for d in cursor.fetchall():
            docs.append(d[0])
        list_term_doc.append(docs)

    result = []
    for item in list_term_doc:
        if len(result) == 0:
            result = item
            continue
        if len(item) != 0:
            result = list(set(result) & set(item))

    if len(result) == 0:
        print("По запросу '%s' не нашлось статей " % text)
    else:
        print("По запросу '%s' нашлись статьи : " % text)
        i = 1
        for r in result:
            cursor.execute(select_article, [r])
            print("%s) %s " % (i, cursor.fetchall()[0][0]))
            i += 1


if __name__ == "__main__":
    simple_query = ["Создание кино", "Популярная фотография", "Знаменитый актер"]
    for q in simple_query:
        execute(q)
    db.close()
