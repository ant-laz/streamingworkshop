# Copyright 2023 Google LLC

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

########################################################################################
# Imports
########################################################################################
from typing import Optional

import json
import typing
from dateutil import parser
from datetime import datetime


import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import Sessions
from apache_beam.transforms.window import TimestampCombiner
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.utils.timestamp import Duration
from apache_beam import PCollection
from apache_beam.io.fileio import WriteToFiles
from apache_beam.io.textio import ReadFromText
from apache_beam.transforms.window import TimestampedValue

########################################################################################
# Schema
########################################################################################

# Great intro to Beam schemas in python: https://www.youtube.com/watch?v=zx4p-UNSmrA

# First we create a class that inherits from NamedTuple, this is our Schema
#
# To actually create an instance of TaxiPoint you can leverage dictionary unpacking
# Let's say you have a dictionary d = {"ride_id": asdf, ...,"passenger_count": 8}
# This dictionary's keys match the fields of TaxiPoint. 
# In this case, you use dictionary unpacking '**' to make class construction easy.
# Dictionary unpacking is when passing a dictionary to a function, 
# the key-value pairs can be unpacked into keyword arguments in a function call 
# where the dictionary keys match the parameter names of the function.
# So the call to the constructor looks like ==> TaxiPoint(**d)
class TaxiPoint(typing.NamedTuple):
    ride_id: str
    point_idx: int
    latitude: float
    longitude: float
    timestamp: str
    meter_reading: float
    meter_increment: float
    ride_status: str
    passenger_count: int

# Second we let Beam know about our Schema by registering it
beam.coders.registry.register_coder(TaxiPoint, beam.coders.RowCoder)

########################################################################################
# TASK 1 : A DoFn that converts from python dict to a Beam Schema
########################################################################################
class CreateTaxiPoint(beam.DoFn):

    def process(self, element):
        yield TaxiPoint(**element)

########################################################################################
# TASK 2 : Write a DoFn "AddTimestampDoFn"
########################################################################################
class AddTimestampDoFn(beam.DoFn):

   def process(self, element):
       element_timestamp: float = parser.parse(element.timestamp).timestamp()
       yield TimestampedValue(element, element_timestamp)

########################################################################################
#  TASK 3 : Write a PTransform "AddKeysToTaxiRides"
########################################################################################

class AddKeysToTaxiRides(beam.PTransform):
 
    def expand(self, pcoll):
        keys: PCollection[tuple(str,TaxiPoint)] = pcoll | "addkeys" >> beam.WithKeys(
            lambda e: e.ride_id
            )
        return keys

########################################################################################
#  TASK 4 : Write a PTransform "TaxiSessioning"
########################################################################################

class TaxiSessioning(beam.PTransform):
 
    def expand(self, pcoll):
        windowed: PCollection[tuple(str,TaxiPoint)] = pcoll | beam.WindowInto(
            # WINDOW SET UP STEP 1 - Window Function
            # Divide a PCollection into session windows.
            # This is applied on a per-key basis.
            # Each session must be separated by a time gap of at least 1 minute
            # Why ? 8 minute was guessed to be a good avg length between NYC taxi rides
            # Why not lower? Connectivity issues cause gaps in the same taxi ride.
            windowfn=Sessions(480.0),
            # WINDOW SET UP STEP 2 - Window Trigger
            # Fire exactly once when the watermark passes the end of the window.
            # Why ? Beacuse we only want to have 1 set of results per session window.
            trigger=AfterWatermark(),
            # WINDOW SET UP STEP 3 - Window accumulation mode
            # Required because we adding a trigger.
            # Controls what to do with data when a trigger fires multiple times.
            # The selected trigger AfterWatermark() will only fire 1 pane of results.
            # Setting accumulation mode to the default of DISCARDING.
            accumulation_mode=AccumulationMode.DISCARDING,
            # WINDOW SET UP STEP 4 - Window allowed latness
            # If element arrives outside of its assigned window, it's late.
            allowed_lateness=Duration(seconds=0)
            )
        return windowed

#######################################################################################
# TASK 5 : Write a DoFn "BusinessRulesDoFn" for taxi ride statistics
#######################################################################################

class BusinessRulesDoFn(beam.DoFn):
    """This DoFn applies some business rules to a group of messages in a window.
        It leverages the grouping by session done thanks to the Window applied
        in the pipeline.
        Here, we will be only handling messages that belong to the same session, 
        identified by ride_id.
        """

    def __init__(self):
        # Create metrics to count occurernce of timestamp types
        # These are not visible with DirectRunner only DataflowRunner
        # See these docs for details
        # https://beam.apache.org/documentation/programming-guide/#metrics
        self.correct_timestamps = beam.metrics.Metrics.counter(
            self.__class__, 
            'correct_timestamps')
        self.wrong_timestamps = beam.metrics.Metrics.counter(
            self.__class__, 
            'wrong_timestamps')

    def _parse_timestamp(self, s):
        # Default value in case of error
        d = datetime(1979, 2, 4, 0, 0, 0)
        try:
            d = parser.parse(s)
            self.correct_timestamps.inc()
        except ValueError:
            self.wrong_timestamps.inc()
        return d   

    def taxi_point_analyzer(self, points: list[TaxiPoint]) -> dict:
        info = {'min_timestamp' : None,
                'max_timestamp' : None,
                'init_status' : "NA",
                'end_status' : "NA",
                'count' : 0,                     
                'duration': 0}
        # Find start time & status. Find end time & status. Calculate stats.
        for taxi_point in points:
            event_timestamp = self._parse_timestamp(taxi_point.timestamp)
            ride_status = taxi_point.ride_status
            if not info["min_timestamp"]:
                info["min_timestamp"] = event_timestamp
            if not info["max_timestamp"]:
                info["max_timestamp"] = event_timestamp
            if event_timestamp <= info["min_timestamp"]:
                info["min_timestamp"] = event_timestamp
                info["init_status"] = ride_status
            if event_timestamp >= info["max_timestamp"]:
                info["max_timestamp"] = event_timestamp
                info["end_status"] = ride_status
            info["count"] += 1
        info["duration"] = (info["max_timestamp"]-info["min_timestamp"]).total_seconds()
        info["min_timestamp"] = info["min_timestamp"].isoformat()
        info["max_timestamp"] = info["max_timestamp"].isoformat()
        return info

    
    def window_analzyer(self, window: beam.DoFn.WindowParam) -> dict:
        return   {
            'window_start': window.start.to_rfc3339(),  # rfc3339 is an iso formater 
            'window_end': window.end.to_rfc3339()       
        }

    def pane_analzyer(self, pane_info: beam.DoFn.PaneInfoParam) -> dict:
        # A pane is the aggregated results of each window.
        # In pipeline step "sessions" the session window was given a trigger config.
        # Beam uses this trigger to determine when to emit/fire panes.
        # We use the pane_info additional param in our DoFn to inspect pane firing.
        timing = "N/A"
        if pane_info.timing == 0:
            timing = "EARLY"
        elif pane_info.timing == 1:
            timing = "ON TIME"
        elif pane_info.timing == 2:
            timing = "LATE"
        else:
            timing = "UNKNOWN" 
        return {'trigger': timing}  # early, on time (watermark) or late            

    def process(self, 
                element,
                window=beam.DoFn.WindowParam,
                pane_info=beam.DoFn.PaneInfoParam):
        # element is a tuple ==> PCollection[tuple(str,list(TaxiPoint))]
        ride_id, list_of_taxi_point = element
        rideinfo: dict = {'ride_id': ride_id,}

        pointinfo: dict  = self.taxi_point_analyzer(list_of_taxi_point)

        windowinfo: dict = self.window_analzyer(window)

        paneinfo: dict = self.pane_analzyer(pane_info)

        yield rideinfo | pointinfo | windowinfo | paneinfo

########################################################################################
# TASK 6 : PTransform to represent the core pipeline logic (excludes input + output)
########################################################################################

class TaxiStatsTransform(beam.PTransform):
 
    def expand(self, pcoll):
        
        ridedata: PCollection[dict] = pcoll |"parse json strings">> beam.Map(json.loads)
        schema: PCollection[TaxiPoint] = ridedata | "apply schemas" >> beam.ParDo(
            CreateTaxiPoint()).with_output_types(TaxiPoint)
        tstamp: PCollection[TaxiPoint] = schema | "timestamping" >> beam.ParDo(
            AddTimestampDoFn())
        key: PCollection[tuple(str,TaxiPoint)] = tstamp | "key" >> AddKeysToTaxiRides()
        win: PCollection[tuple(str,TaxiPoint)] = key | "sessions" >> TaxiSessioning()
        grp: PCollection[tuple(str,list(TaxiPoint))] = win | "group">> beam.GroupByKey()
        stats: PCollection[dict] = grp | "stats" >> beam.ParDo(BusinessRulesDoFn())
        return stats

#######################################################################################
# Pipeline
#######################################################################################       

def run(
    beam_options: Optional[PipelineOptions] = None
) -> None:

    with beam.Pipeline(options=beam_options) as pipeline:
        rides: PCollection[str] = pipeline | "Read ndjson input" >> ReadFromText(
            file_pattern=beam_options.input_filename)
        calculations: PCollection[dict] = rides | "calculations" >> TaxiStatsTransform()
        writeablecalculations: PCollection[str] = calculations | beam.Map(json.dumps)
        writeablecalculations | WriteToFiles(path=beam_options.output_filename)