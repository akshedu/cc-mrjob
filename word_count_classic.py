"""The classic MapReduce job: count the frequency of words.
"""
import warc
from mrcc import CCJob
from mrjob.job import MRJob
import re
from gzipstream import GzipStreamFile
import gzip

WORD_RE = re.compile(r"[\w']+")


class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
    	# with open(line) as f:
    	# 	for text_line in f:
    	# 		for word in text_line.split():
    	# 			yield (word.lower(), 1, 'hello')
        for word in WORD_RE.findall(line):
            yield (word.lower(), 1)

    # def combiner(self, word, counts):
    #     yield (word, sum(counts))

    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
     MRWordFreqCount.run()