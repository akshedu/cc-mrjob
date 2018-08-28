import re
from mrcc import CCJob
from bs4 import BeautifulSoup
from collections import Counter


class WordIDF(CCJob):
    """
 	Returns list of unique words in a document
    """

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
        	text = self.clean_text(re.compile('Content-Length: \d+').split(record.payload)[1])
        	alnum_text = re.sub('[^A-Za-z0-9 ]+', ' ', text)
        	for word, counter in Counter(alnum_text.encode('utf-8').lower().split()).iteritems():
        		yield (record['WARC-Trec-iD'], word), 1
        except:
        	yield '(an error occurred)', 1
        	return

#    def reducer(self, key, values):

if __name__ == '__main__':
    WordIDF.run()