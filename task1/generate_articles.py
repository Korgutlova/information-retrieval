import requests
from parsel import Selector
from connnect.ConnectionDB import ConnectionDB

if __name__ == "__main__":
    domen = "https://kulturologia.ru/%s"
    url = 'https://kulturologia.ru/news/page/%s/'
    i = 596
    n = 30
    m = 0
    db = ConnectionDB()
    cursor = db.cursor
    postgres_insert_query = "INSERT INTO public.students(name, surname, mygroup) VALUES (%s, %s, %s) RETURNING id"
    postgres_insert_query2 = "INSERT INTO public.articles(title, keywords, content, url, student) VALUES (%s, %s, %s, %s, %s)"
    student_to_insert = ("Наталья", "Коргутлова", "11-501")
    cursor.execute(postgres_insert_query, student_to_insert)
    uuid = cursor.fetchone()[0]
    print(uuid)
    db.commit()
    count = cursor.rowcount
    print(count, "Students created")

    while m < n:
        text = requests.get(url % i).text
        selector = Selector(text=text)
        for link in selector.xpath('//div[@class="post"]//h1/a/@href').getall():
            url2 = domen % link
            print("url " + url2)
            text = requests.get(url2).text
            selector = Selector(text=text)
            tags = selector.xpath('//div[@class="tags"]//a/text()').getall()
            if len(tags) != 0:
                m += 1
                tags = ';'.join("{0}".format(x) for x in tags)
                title = selector.xpath('//div[@class="title"]//h1/a/text()').get()
                print("%s title %s " % (m, title))
                print("tags : %s" % tags)
                article = ""
                print("------")
                for paragraph in selector.xpath('//div[@class="content"]/p/descendant-or-self::*/text()').getall()[:-3]:
                    article += " " + paragraph.strip()
                print(article)
                cursor.execute(postgres_insert_query2, (title, tags, article, url2, uuid))
                db.commit()
            if m == n:
                break
        i -= 1
    db.close()
