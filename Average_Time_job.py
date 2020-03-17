from mrjob.job import MRJob
from mrjob.step import MRStep
import re



class MRBikeshare(MRJob):
    # def steps(self):
    #     return {
    #         MRStep(mapper = self.mapper,
    #                reducer = self.reducer)
    #     }

    def mapper(self, _, line):
        year,items = line.split('\t')
        items = items[1:-1]
        name, time, user_type = items.split(', ')
        time = int(time)
        yield user_type, time

    def reducer(self, key, values):
        total_time = 0
        num_elemets = 0
        for value in values:
            total_time += value
            num_elemets += 1
        yield key, (total_time/num_elemets)

if __name__ == '__main__':
    MRBikeshare.run()