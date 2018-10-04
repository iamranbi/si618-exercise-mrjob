import mrjob
from mrjob.job import MRJob
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

    def combiner(self, word, counts):
        yield word, sum(counts)

    def reducer(self, word, counts):
        yield word, sum(counts)


if __name__ == "__main__":
    MRMostUsedWord.run()
