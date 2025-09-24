import pandas as pd
from mrjob.job import MRJob
import re

#birthday is 11/18/1998

words_only = re.compile(r"[^\W\d_]+(?:['’][^\W\d_]+)*", re.UNICODE)

class word_count(MRJob):

    def mapper_init(self):
        self.dictionary = []
        with open("C:/Users/david/Downloads/words.txt", "r", encoding = "utf8") as en_words:
            for line in en_words:
                self.dictionary.append(line.strip().lower())
    
    def mapper(self, _, line):
        for word in words_only.findall(line):
            word = word.replace("\u2019", "'")
            case_normalized_word = word.lower()
            if case_normalized_word in self.dictionary:
                continue
            else:
                yield (case_normalized_word, 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))


if __name__ == '__main__':
    word_count.run()