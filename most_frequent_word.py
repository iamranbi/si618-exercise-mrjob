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
        yield None, (sum(counts), word)
    
    #reducer that takes the data from the previous reducer step and outputs the most frequent word    
    def reducer_find_max_word(self, _, word_count_pairs):
        yield max(word_count_pairs)
     
    def steps(self):
        return[
            MRStep(mapper=self.mapper,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_max_word)
        ]


if __name__ == "__main__":
    MRMostUsedWord.run()
