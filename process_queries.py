import re
import math
from collections import Counter

import psycopg2 as psg
from mrcc import CCJob
from bs4 import BeautifulSoup

from sql_queries import DOCUMENT_CANONICAL_TYPES
from sql_queries import FILTER_QUERIES
from sql_queries import DOCUMENT_ENTITY_CANONICAL_TYPES
from sql_queries import WORD_IDF


class ProcessQuery(CCJob):
    """
 	Returns IDF of words in a corpus of WARC documents
    """
    def mapper_init(self):
        self.conn = psg.connect('dbname=qa_app \
                                user=test_user \
                                password=Everest \
                                host=127.0.0.1 \
                                port=5432')
        self.cur = self.conn.cursor()
        from myconfig import WINDOW_SIZE
        self.WINDOW_SIZE = WINDOW_SIZE

    def get_doc_canonical_types(self, document_id):
        doc_entity_types_query = self.cur.execute(DOCUMENT_CANONICAL_TYPES.\
                                            format(document_id=document_id))
        doc_entity_types = doc_entity_types_query.fetchall()
        return tuple([result[1] for result in doc_entity_types])

    def get_doc_queries(self, doc_canonical_types):
        doc_queries_query = self.cur.execute(FILTER_QUERIES.\
                                        format(types=doc_canonical_types))
        return doc_queries_query.fetchall()

    def get_idf_score(self, words):
        idf_scores = self.cur.execute(WORD_IDF.format(words=tuple(words)))
        return sum([float(result[1]) for result in idf_scores])

    def get_text_snippet_query(self, query_id, S, target_type, document_id,
                                doc_entity_canonical_types, doc_html_text):
        S = S.split(',')
        for item in doc_entity_canonical_types:
            if item[8] == target_type:
                start_byte = item[3] - self.WINDOW_SIZE
                end_byte = item[4] + self.WINDOW_SIZE
                window_text =  doc_html_text[start_byte:end_byte]
                window_text = re.sub('[^A-Za-z0-9 ]+', ' ', window_text)
                window_words = window_text.lower().split()
                matching_words =  list(set(window_words) & set(S))
                total_score = get_idf_score(matching_words)
                yield query_id, (document_id, window_text, total_score)


    def clean_text(self, html):
        soup = BeautifulSoup(html) # create a new bs4 object from the html data loaded
        for script in soup(["script", "style"]): # remove all javascript and stylesheet code
            script.extract()
        # get text
        text = soup.get_text()
        # break into lines and remove leading and trailing space on each
        lines = (line.strip() for line in text.splitlines())
        # break multi-headlines into a line each
        chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
        # drop blank lines
        text = '\n'.join(chunk for chunk in chunks if chunk)
        return text

    def process_record(self, record):
        if record['WARC-Type'] != 'response':
            # we're only interested in the HTTP responses
            return
        try:
            document_id = record['WARC-TREC-ID']
            doc_canonical_types = get_doc_canonical_types(document_id)
            if not doc_canonical_types:
                return
            relevant_queries = get_doc_queries(doc_canonical_types)
            if not relevant_queries:
                return
            doc_entity_canonical_types = self.cur.execute(DOCUMENT_ENTITY_CANONICAL_TYPES.\
                                                    format(document_id=document_id))
            doc_encoding = doc_entity_canonical_types[0][1]
            doc_html_text = record.payload.decode(doc_encoding).encode('utf-8')

            for _, S, target_type in relevant_queries:
                for key, value in get_text_snippet_query(query_id, S, target_type, document_id,
                                                        doc_entity_canonical_types, doc_html_text):
                    yield key, value
        except:
        	return

    def combiner_init(self):
        from myconfig import MIN_SCORE
        self.MIN_SCORE = MIN_SCORE

    def combiner(self, key, values):
        """
        Sums up count for each mapper
        """
        yield key, [val for val in values if val[2] > self.MIN_SCORE]
    
    def reducer(self, key, values):
        """
        Ouputs IDF of each word using number of documents
        as a constant
        """
        values = sorted(values, key=lambda tup: tup[2], reverse=True)
        for val in values:
            yield (key, val[0]), (val[1], val[2])


if __name__ == '__main__':
    ProcessQuery.run()