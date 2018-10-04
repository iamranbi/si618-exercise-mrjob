from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import os
import inspect

module_path = inspect.getfile(inspect.currentframe())
module_dir = os.path.realpath(os.path.dirname(module_path))
os.chdir(module_dir)
WORD_RE = re.compile(r"[\w']+")


class MRMostUsedWord(MRJob):
    def mapper(self, _, line):
        for word in WORD_RE.findall(line):
            yield word.lower(), 1

    def combiner_count_words(self, word, counts):
    yield word, sum(counts)

    def reducer_count_words(self, word, counts):
    yield None, (sum(counts), word)

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
