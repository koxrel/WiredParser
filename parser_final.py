import requests as r
import nltk.data
import time
import re
import psycopg2 as ps
from bs4 import BeautifulSoup as bs, Tag, Comment
from datetime import datetime
from string import punctuation


def content(page, article):
    """Gets the main content of a page"""
    page_text = []

    # Check whether the page is actually a sort of list/ranking, and parse it accordingly
    list_check = page.find(id='listicle-intro')
    if list_check:
        page_text.append(list_check.get_text().strip())
        list_page = page.main.article('div', class_=re.compile('listicle'), recursive=False)

        for l in list_page:
            page_text.append(l.div.p.get_text().strip())
    # The section parses general content
    else:
        for part in page.main.article.contents:
            if type(part) is Tag:
                part_text = part.get_text().strip()
                if (u'p' == part.name or u'h3' == part.name)and part_text:
                    # To avoid bizarre situations with headings having no punctuation
                    if part_text[-1] not in punctuation:
                        part_text = part_text + '.'
                    page_text.append(part_text)
            elif (not type(part) is Comment) and part.strip():
                page_text.append(part.strip())
    # Consolidated text for processing
    article['text'] = ' '.join(page_text)


def record_to_db(article, conn):
    """Write an article to the database"""
    # Standard procedure described in docs for the package
    with conn:
        with conn.cursor() as curs:
            # Insert data into articles
            curs.execute(
                'INSERT INTO articles (resource, link, parse_date, pubdate, author, title, tags, category, programmer) '
                'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING docid',
                (article['source'], article['url'], article['parse_date'], article['pub_date'], article['author'],
                 article['title'], article['tags'], article['category'], article['progr_name']))
            # Get the id of the inserted article and add all related sentences
            docid = curs.fetchone()[0]
            for i in range(len(article['text'])):
                curs.execute('INSERT INTO sents (docid, sent_id, sent) VALUES (%s, %s, %s)',
                             (docid, i+1, article['text'][i]))


def parse_video_page(page, article):
    """Parses the page containing a video with a description"""
    page_video = page.main.article.find('div', class_='row')
    article['author'] = None
    article['pub_date'] = datetime.strptime(page_video.find('span', string=re.compile('\d{2}.\d{2}.\d{2}')).get_text(),
                                            '%m.%d.%y').date()
    article['category'] = page_video.span.a.get("title")
    article['text'] = page_video.find('div', class_=re.compile('vid-exchange')).get_text().strip()


def parse(url, tok=None, conn=None):
    """Parses the specified page (according to the inferred type) and record it to a database (if connection is supplied)"""
    try:
        page = get_page(url)
        article = {'source': 'Wired.com', 'url': url, 'parse_date': datetime.now().date(), 'progr_name': 'Tresoumov',
                   'title': page.h1.get_text().strip()}

        if url.find('video') == -1:
            parse_blog_page(article, page)
        else:
            parse_video_page(page, article)

        tag(page, article)

        if tok:
            article['text'] = tok.tokenize(article['text'])

        if conn:
            record_to_db(article, conn)
    except:
        print("Not successful: " + str(url))


def parse_blog_page(article, page):
    """Parses the page containing textual information"""
    article['author'] = page.main.header.ul.li.span.string[8:-1]
    article['pub_date'] = datetime.strptime(page.main.header.ul.contents[3].span.string.split(': ')[1][:-1],
                                            '%m.%d.%y').date()
    article['category'] = page.main.header.ul.find('span', itemprop="articleSection").get_text().strip()

    content(page, article)


def get_page(url):
    """Gets a page and transforms it to a parse tree utilising BeautifulSoup"""
    req = r.get(url)
    req.raise_for_status()
    return bs(req.text, 'lxml')


def tag(page, article):
    """Gets tags for a page"""
    tags = page.find(id='article-tags')
    if tags:
        article['tags'] = tags.get_text(';', strip=True)
    else:
        article['tags'] = None


def main():
    # Get all the links from the main page and parse them if possible
    wired_page = get_page('http://www.wired.com/')
    links = set([l.get('href') for l in wired_page('a', href=re.compile('http://www\.wired\.com/'))])
    num = len(links)
    print('Will be processing ' + str(num) + ' links')
    counter = 0
    tok = nltk.data.load('tokenizers/punkt/english.pickle')
    conn = ps.connect(database='***', host='***', user='***',
                      password='***')
    for link in links:
        counter += 1
        time.sleep(1)
        parse(link, tok, conn)
        print('Current progress: ', int(counter / num * 100), '%')
    conn.close()

if __name__ == '__main__':
    main()