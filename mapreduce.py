import pandas as pd
from mrjob.job import MRJob
import re

#birthday is 11/18/1998

words_only = re.compile(r"[^\W\d_]+(?:['’][^\W\d_]+)*", re.UNICODE)

class word_count(MRJob):

    def mapper(self, _, line):
        for word in words_only.findall(line):
            word = word.replace("\u2019", "'")
            yield (word.lower(), 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    word_count.run()