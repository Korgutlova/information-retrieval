from connnect.ConnectionDB import ConnectionDB
from task2.porter_stemmer import PorterStemmer
from nltk.corpus import stopwords
from pymystem3 import Mystem
from nltk.tokenize import sent_tokenize, word_tokenize

if __name__ == "__main__":
    db = ConnectionDB()
    cursor = db.cursor
    select_query = "SELECT title, content, keywords, id from public.articles"
    insert_one_table = "INSERT INTO public.words_mystem(term, article_id) VALUES (%s, %s);"
    insert_two_table = "INSERT INTO public.words_porter(term, article_id) VALUES (%s, %s);"
    cursor.execute(select_query)
    result = cursor.fetchall()
    stop_words = stopwords.words('russian')
    stop_words.extend([',', '.', '—', '–', '-', '«', '»', '?', '!', '(', ')', ';', ':', '”', '“', '\''])
    m = Mystem()
    p = PorterStemmer()
    i = 1
    for r in result:
        title, content, keywords, id = r[0], r[1], r[2], r[3]
        sent_tokenize_list = sent_tokenize(content)
        sent_tokenize_list.append(title)
        sent_tokenize_list.append(keywords)
        l = 1
        for sent in sent_tokenize_list:
            words = word_tokenize(sent)
            for w in words:
                word = w.lower()
                if word not in stop_words:
                    cursor.execute(insert_one_table, (m.lemmatize(word)[0], id))
                    cursor.execute(insert_two_table, (p.stem(word), id))
                    # print("old - %s new mystem - %s new porter - %s" % (word, m.lemmatize(word)[0], p.stem(word)))
            db.commit()
            print("Sentence %s" % l)
            l += 1
        print("Article %s" % i)
        i += 1
    db.close()
