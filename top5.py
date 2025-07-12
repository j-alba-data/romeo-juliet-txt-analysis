from mrjob.job import MRJob
from mrjob.step import MRStep
import string

class MRWordFreqCount(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_find_top_words)
        ]

    def mapper_get_words(self, _, line):
        # Updated set of words to exclude
        exclude_words = set(['the', 'a', 'an', 'and', 'or', 'i', 'to', 'of', 'my', 'that'])

        line = line.strip().lower()
        # Remove punctuations
        for s in string.punctuation:
            line = line.replace(s, '')
        words = line.split()
        for word in words:
            if word not in exclude_words:
                yield (word, 1)

    def reducer_count_words(self, word, counts):
        yield None, (sum(counts), word)

    def reducer_find_top_words(self, _, word_counts):
        # Sort the word_counts in descending order and take the top 5
        top_words = sorted(word_counts, reverse=True)[:5]
        for count, word in top_words:
            yield (word, count)

if __name__ == '__main__':
    MRWordFreqCount.run()