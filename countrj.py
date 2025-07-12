from mrjob.job import MRJob
import string

class MRRomeoJulietCount(MRJob):
    def mapper(self, _, line):
        line = line.strip()
        # Remove punctuations
        for s in string.punctuation:
            line = line.replace(s, '')
        words = line.split()
        for word in words:
            word = word.lower()
            if word in ['romeo', 'iuliet']:
                yield (word, 1)

    def reducer(self, word, counts):
        yield (word, sum(counts))

if __name__ == '__main__':
    MRRomeoJulietCount.run()