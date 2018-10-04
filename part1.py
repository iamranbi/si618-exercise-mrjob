import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[\w']+")

class MRMostUsedWord(MRJob):
    #mapper that splits a line of input into words
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1
            
    #combiner that sums up the count for each word
    def combiner(self, word, counts):
        yield word, sum(counts)

     #reducer that sums up the count for each word 
    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == "__main__":
    MRMostUsedWord.run()
