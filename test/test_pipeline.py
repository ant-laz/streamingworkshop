import unittest


import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, equal_to_per_window
from apache_beam.transforms.window import TimestampedValue, IntervalWindow
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.utils.timestamp import Duration


import my_pipeline

#######################################################################################
# TESTS FOR TASK 1 : Write a DoFn "CreateTaxiPoint"
#######################################################################################
class TestTaxiPointCreation(unittest.TestCase):
    def test_task_1_taxi_point_creation(self):
        taxipoint = my_pipeline.TaxiPoint(
            ride_id = "277278c9-9e5e-4aa9-b1b9-3e38e2133e5f",
            point_idx = 0,
            latitude = 40.73362,
            longitude = -73.97472,
            timestamp = "2023-04-21T16:17:09.7316-04:00",
            meter_reading = 0,
            meter_increment = 0.06205036,
            ride_status = "pickup",
            passenger_count = 2
        )
        taxijson = {
            "ride_id": "277278c9-9e5e-4aa9-b1b9-3e38e2133e5f", 
            "point_idx": 0, 
            "latitude": 40.73362, 
            "longitude": -73.97472, 
            "timestamp": "2023-04-21T16:17:09.7316-04:00", 
            "meter_reading": 0, 
            "meter_increment": 0.06205036, 
            "ride_status": "pickup", 
            "passenger_count": 2
        }
          
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True

        with TestPipeline(options=options) as p:
            test_stream = TestStream().advance_watermark_to(0).add_elements([
                taxijson
            ]).advance_watermark_to_infinity()

            output = (p | 
                       test_stream | 
                       beam.ParDo(my_pipeline.CreateTaxiPoint())
                      )
            EXPECTED_OUTPUT = [taxipoint] 
            assert_that(output, equal_to(EXPECTED_OUTPUT))           

#######################################################################################
# TESTS FOR TASK 2 : Write a DoFn "AddTimestampDoFn"
#######################################################################################
class TestAddTimestampDoFn(unittest.TestCase):

    class ExtractTimestamp(beam.DoFn):
        def process(
            self,
            elem,
            timestamp=beam.DoFn.TimestampParam,
            window=beam.DoFn.WindowParam):
            yield repr(timestamp.micros)

    def test_task_2_add_timestamp_dofn(self):
        taxipoint = my_pipeline.TaxiPoint(
            ride_id = "277278c9-9e5e-4aa9-b1b9-3e38e2133e5f",
            point_idx = 0,
            latitude = 40.73362,
            longitude = -73.97472,
            timestamp = "2023-04-21T16:17:09.7316-04:00",
            meter_reading = 0,
            meter_increment = 0.06205036,
            ride_status = "pickup",
            passenger_count = 2
        )
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            test_stream = TestStream().advance_watermark_to(0).add_elements([
                taxipoint
            ]).advance_watermark_to_infinity()

            taxi_ts = (p | 
                       test_stream | 
                       beam.ParDo(my_pipeline.AddTimestampDoFn()) |
                       beam.ParDo(self.ExtractTimestamp())
                      )
            
            # The transform beam.transforms.window.TimestampedValue
            # assigns timestamps to PCollection elements
            # however the returned type is still my_pipeline.TaxiPoint
            # to access the timestamp applied by beam.transforms.window.TimestampedValue
            # there is a special DoFn argument, "beam.DoFn.TimestampParam"
            # see this example:
            # https://beam.apache.org/documentation/transforms/python/elementwise/pardo/
            # In this test we use an inner class ExtractTimestamp to create a DoFn
            # to pull out the timestamp assigned by my_pipeline.AddTimestampDoFn
            # the literal 1682108229731600 correspond to the unix timestamp
            # equivalent of 2023-04-21T16:17:09.7316-04:00

            EXPECTED_TAXI_TS = ['1682108229731600']
            assert_that(taxi_ts, equal_to(EXPECTED_TAXI_TS))  

########################################################################################
#  TESTS FOR TASK 3 : Write a PTransform "AddKeysToTaxiRides"
########################################################################################
class TestAddingKeysToTaxiRides(unittest.TestCase):
    def test_task_3_adding_keys_to_taxi_rides(self):
        taxipoint1 = my_pipeline.TaxiPoint(
            ride_id = "1",
            point_idx = 0,
            latitude = 40.73362,
            longitude = -73.97472,
            timestamp = "2023-04-21T16:17:09.7316-04:00",
            meter_reading = 0,
            meter_increment = 0.06205036,
            ride_status = "pickup",
            passenger_count = 2
        )
        taxipoint2 = my_pipeline.TaxiPoint(
            ride_id = "2",
            point_idx = 0,
            latitude = 40.73362,
            longitude = -73.97472,
            timestamp = "2023-04-21T16:17:09.7316-04:00",
            meter_reading = 0,
            meter_increment = 0.06205036,
            ride_status = "pickup",
            passenger_count = 2
        )        
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            test_stream = TestStream().advance_watermark_to(0).add_elements([
               TimestampedValue(taxipoint1,0),
               TimestampedValue(taxipoint2,1)
            ]).advance_watermark_to_infinity()

            taxi_keys = (p | 
                       test_stream | 
                       my_pipeline.AddKeysToTaxiRides()
                      )

            # WithKeys returns a tuple (K,V) 
            # K - key, here it is  my_pipeline.TaxiPoint.ride_id
            # V - PCollection element passed to WithKeys, here a my_pipeline.TaxiPoint     
            EXPECTED_TAXI_KEYS = [
                ("1",taxipoint1),
                ("2",taxipoint2)
                ]
            pass
            assert_that(taxi_keys, equal_to(EXPECTED_TAXI_KEYS))      

########################################################################################
# TESTS FOR TASK 4 : Write a PTransform "TaxiSessioning"
########################################################################################
class TestTaxiRideSessions(unittest.TestCase):
    def test_task_4_taxi_ride_sessions(self):
        taxipoint1 = my_pipeline.TaxiPoint(
            ride_id = "1",
            point_idx = 0,
            latitude = 40.73362,
            longitude = -73.97472,
            timestamp = "2023-04-21T16:17:09.7316-04:00",
            meter_reading = 0,
            meter_increment = 0.06205036,
            ride_status = "pickup",
            passenger_count = 2
        )
        taxipoint2 = my_pipeline.TaxiPoint(
            ride_id = "2",
            point_idx = 0,
            latitude = 40.73362,
            longitude = -73.97472,
            timestamp = "2023-04-21T16:17:09.7316-04:00",
            meter_reading = 0,
            meter_increment = 0.06205036,
            ride_status = "pickup",
            passenger_count = 2
        )        
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            test_stream = TestStream().advance_watermark_to(0).add_elements([
               TimestampedValue(("1",taxipoint1),0),
               TimestampedValue(("2",taxipoint2),1)
            ]).advance_watermark_to_infinity()
   
            taxi_counts = (p | 
                       test_stream | 
                       my_pipeline.TaxiSessioning() |
                       beam.CombinePerKey(CountCombineFn())
                      ) 
            
            # expected_window_to_elements: A dictionary where the keys are the windows
            # to check and the values are the elements associated with each window.
            #
            # Aggregate functions are applied per-window & per-key automatically
            # From the transform catalog & documentation, CombinePerKey will 
            # produce a single value per window & key.
            # beam.combiners.CountCombineFn is a premade function 
            # that saves us  writing our own function to calculate pCollection size ! 
            #             
            # Session windows use interval windows internally
            # https://beam.apache.org/releases/pydoc/2.2.0/_modules/apache_beam/transforms/window.html#Sessions
            # The Interval Window is defined as
            # IntervalWindow(start_timestamp, start_timestamp + self.gap_size)
            # As per our test stream setup, start_timestamp = 0
            # As per our code setup, the session gap_size is 480 (8 mins)
            #
            # Per session window, we need to specify the expected count per key
            # Per session window, there is a list of tuples
            # Each tuple is the count per key in that session window ('key', count)
            EXPECTED_WINDOW_COUNTS = {
                IntervalWindow(0,480): [('1', 1)],
                IntervalWindow(1,481): [('2', 1)],
                }
            assert_that(taxi_counts, 
                        equal_to_per_window(EXPECTED_WINDOW_COUNTS),
                        reify_windows=True)            

#######################################################################################
# TESTS FOR TASK 5 : Write a DoFn "BusinessRulesDoFn" for taxi ride statistics
#######################################################################################
class TestStatsCalculation(unittest.TestCase):
    def test_task_5_stats_calculation(self):
        taxipoint1 = my_pipeline.TaxiPoint(
            ride_id = "1",
            point_idx = 0,
            latitude = 40.73362,
            longitude = -73.97472,
            timestamp = "2023-04-21T16:17:09.7316-04:00",
            meter_reading = 0,
            meter_increment = 0.06205036,
            ride_status = "pickup",
            passenger_count = 2
        )
        taxipoint2 = my_pipeline.TaxiPoint(
            ride_id = "1",
            point_idx = 58,
            latitude = 40.73465,
            longitude = -73.97906,
            timestamp = "2023-04-21T16:21:23.22081-04:00",
            meter_reading = 3.598921,
            meter_increment = 0.06205036,
            ride_status = "enroute",
            passenger_count = 2
        )
        taxipoint3 = my_pipeline.TaxiPoint(
            ride_id = "1",
            point_idx = 278,
            latitude = 40.74541000000001,
            longitude = -73.98677,
            timestamp = "2023-04-21T16:37:24.7316-04:00",
            meter_reading = 17.25,
            meter_increment = 0.06205036,
            ride_status = "dropoff",
            passenger_count = 2
        )                    
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        with TestPipeline(options=options) as p:
            test_stream = TestStream().advance_watermark_to(0).add_elements([
                    #We create a test stream of TaxiPoints with keys & timestamps
                    TimestampedValue(("1",taxipoint1), 0),
                    TimestampedValue(("1",taxipoint2), 0),
                    TimestampedValue(("1",taxipoint3), 0)
            ]).advance_watermark_to(60).advance_processing_time(60).advance_watermark_to_infinity()

            taxi_stats = (p | 
                       test_stream | 
                       # Pick the simplest window for testing, the fixed window
                       beam.WindowInto(beam.window.FixedWindows(60),
                                       trigger=AfterWatermark(),
                                       allowed_lateness=Duration(seconds=0),
                                       accumulation_mode=AccumulationMode.DISCARDING) |
                       # BusinessRulesDoFn expects grouped after windowing                                       
                       beam.GroupByKey() |                                       
                       beam.ParDo(my_pipeline.BusinessRulesDoFn())
                      )

            EXPECTED_TAXI_STATS = [
                {
                    'ride_id': "1",
                    'duration':  1215.0,
                    'min_timestamp': "2023-04-21T16:17:09.731600-04:00",
                    'max_timestamp': "2023-04-21T16:37:24.731600-04:00",
                    'count': 3,
                    'init_status': "pickup",
                    'end_status': "dropoff",
                    'trigger': "ON TIME",  
                    'window_start': "1970-01-01T00:00:00Z",
                    'window_end': "1970-01-01T00:01:00Z"
            }
            ]
            assert_that(taxi_stats, equal_to(EXPECTED_TAXI_STATS))

########################################################################################
# TESTS FOR TASK 6 : PTransform to represent the core pipeline logic (excludes I/O)
########################################################################################
class TestEndToEndPipeline(unittest.TestCase):
    def test_task_6_end_to_end_pipeline(self):
 
        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
 
        with TestPipeline(options=options) as p:
 
            taxiride1 = "{\"ride_id\":\"1\",\
                          \"point_idx\":1,\
                          \"latitude\":0.0,\
                          \"longitude\":0.0,\
                          \"timestamp\":\"2023-04-21T16:17:00Z\",\
                          \"meter_reading\":0.0,\
                          \"meter_increment\":0.1,\
                          \"ride_status\":\"pickup\",\
                          \"passenger_count\":1}"
            taxiride2 = "{\"ride_id\":\"1\",\
                          \"point_idx\":2,\
                          \"latitude\":0.0,\
                          \"longitude\":0.0,\
                          \"timestamp\":\"2023-04-21T16:17:30Z\",\
                          \"meter_reading\":0.1,\
                          \"meter_increment\":0.1,\
                          \"ride_status\":\"enroute\",\
                          \"passenger_count\":1}"
            taxiride3 = "{\"ride_id\":\"1\",\
                          \"point_idx\":3,\
                          \"latitude\":0.0,\
                          \"longitude\":0.0,\
                          \"timestamp\":\"2023-04-21T16:18:00Z\",\
                          \"meter_reading\":0.2,\
                          \"meter_increment\":0.1,\
                          \"ride_status\":\"dropoff\",\
                          \"passenger_count\":1}"                        
            
            unixwtrmark = 1682094000 #2023-04-21T16:20:00 --> 2 min after final event
            test_stream = TestStream().advance_watermark_to(0).add_elements([
                taxiride1,
                taxiride2,
                taxiride3
            ]).advance_watermark_to(unixwtrmark
             ).advance_watermark_to_infinity()

 
            taxi_stats = (p | 
                          test_stream | 
                          my_pipeline.TaxiStatsTransform()
                           )

            # Session windows use interval windows internally
            # https://beam.apache.org/releases/pydoc/2.2.0/_modules/apache_beam/transforms/window.html#Sessions
            # The Interval Window is defined as
            # IntervalWindow(start_timestamp, start_timestamp + self.gap_size)
            # As per our test stream setup, max_timesatmp = 2023-04-21T16:18:00Z
            # As per our code setup, the session gap_size is 480 (8 mins)
            # Therefore the window_end is = max_timestamp + gap_size
            # ............. window_end is = 2023-04-21T16:18:00Z + 8mins
            # ............. window_end is = 2023-04-21T16:16:00Z
            #
            # By setting unixwtrmark = 1682094000 (2023-04-21T16:20:00)
            # We advanced the watermark past the end of the window end
            # This matches our trigger "trigger=AfterWatermark()"
            
            EXPECTED_TAXI_STATS = [
                {
                    'ride_id': "1",
                    'duration':  60.0,
                    'min_timestamp': "2023-04-21T16:17:00+00:00",
                    'max_timestamp': "2023-04-21T16:18:00+00:00",
                    'count': 3,
                    'init_status': "pickup",
                    'end_status': "dropoff",
                    'trigger': "ON TIME",  
                    'window_start': "2023-04-21T16:17:00Z",
                    'window_end': "2023-04-21T16:26:00Z"
            }
            ]
            assert_that(taxi_stats, equal_to(EXPECTED_TAXI_STATS))
 

if __name__ == "__main__":
    unittest.main()