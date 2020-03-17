from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class MRBikePreprocessed(MRJob):
    def mapper(self, _, list):
        (trip_id, trip_start_time, trip_stop_time, trip_duration_seconds, from_station_id, from_station_name, to_station_id, to_station_name, user_type)=list.split(',')
        trip_duration_seconds = int(trip_duration_seconds)
        trip_year = datetime.strptime(trip_start_time, '%d/%m/%Y %H:%M')
        yield trip_year.year, (from_station_name, trip_duration_seconds, user_type)

if __name__ == '__main__':
    MRBikePreprocessed.run()