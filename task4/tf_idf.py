import numpy as np

from connnect.ConnectionDB import ConnectionDB

db = ConnectionDB()
cursor = db.cursor

if __name__ == "__main__":
    select_terms = "SELECT term_id, term_text FROM public.term_list"
    count_docs = "SELECT id FROM articles"
    select_docs_term_count = "SELECT count(distinct(article_id, term)) FROM public.words_mystem WHERE term=%s"
    select_term_in_doc = "SELECT count(*) FROM public.words_mystem WHERE article_id=%s and term=%s"
    select_all_term_doc = "SELECT count(*) FROM public.words_mystem WHERE article_id=%s"
    update = "UPDATE public.article_term SET tf_idf=%s WHERE article_id=%s and term_id=%s"
    cursor.execute(count_docs)
    documents = cursor.fetchall()
    D = len(documents)
    cursor.execute(select_terms)
    terms = cursor.fetchall()
    for term_id, term in terms:
        cursor.execute(select_docs_term_count, [term])
        small_d = int(cursor.fetchall()[0][0])
        idf = np.log(D / small_d)
        for item in documents:
            id = item[0]
            cursor.execute(select_term_in_doc, [id, term])
            term_only = int(cursor.fetchall()[0][0])
            if term_only != 0:
                cursor.execute(select_all_term_doc, [id])
                size = int(cursor.fetchall()[0][0])
                tf_idf = term_only / size * idf
                cursor.execute(update, [tf_idf, id, term_id])
        db.commit()
    db.close()
