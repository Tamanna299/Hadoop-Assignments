from mrjob.job import MRJob
from mrjob.step import MRStep

class CountMovieRatings(MRJob):

    def steps(self):
        return [MRStep(mapper=self.mapper_get_movies,
                       reducer=self.reducer_count_movie_ratings),
                MRStep(reducer=self.reducer_sort_counts)
                ]

    def mapper_get_movies(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t')
        yield movieID, 1

    def reducer_count_movie_ratings(self, key, values):
        yield None, (sum(values), key)

    def reducer_sort_counts(self, _, values):
        for count, key in sorted(values):
                yield key, int(count)

if __name__ == '__main__':
    CountMovieRatings.run()
