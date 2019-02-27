from connnect.ConnectionDB import ConnectionDB

if __name__ == "__main__":
    db = ConnectionDB()
    cursor = db.cursor
    query = "SELECT id, term, article_id FROM public.words_mystem"
    check_unique = "SELECT term_id FROM public.term_list WHERE term_text=%s"
    check_unique_pair = "SELECT article_id FROM public.article_term WHERE article_id=%s and term_id=%s"
    insert_query_one = "INSERT INTO public.term_list(term_id, term_text) VALUES (%s, %s);"
    insert_query_two = "INSERT INTO public.article_term(article_id, term_id) VALUES (%s, %s)"
    cursor.execute(query)
    result = cursor.fetchall()
    for r in result:
        id, term, article_id = r[0], r[1], r[2]
        cursor.execute(check_unique, [term])
        temp_id = cursor.fetchall()
        if not temp_id:
            cursor.execute(insert_query_one, [id, term])
        else:
            id = temp_id[0]
        cursor.execute(check_unique_pair, [article_id, id])
        res = cursor.fetchall()
        if not res:
            cursor.execute(insert_query_two, [article_id, id])
        db.commit()
    db.close()
