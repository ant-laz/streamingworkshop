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

import apache_beam as beam
from dateutil import parser

class AnalyzeElement(beam.DoFn):
  
  # DoFn to help inspect the output of pipeline stages
  # Based on the example here: 
  # https://beam.apache.org/documentation/transforms/python/elementwise/pardo/
  
  def process(
      self,
      elem,
      timestamp=beam.DoFn.TimestampParam,
      window=beam.DoFn.WindowParam):
    
    min_ts = parser.parse("2023-01-01").timestamp()
    max_ts = parser.parse("2023-12-31").timestamp()

    begin = "\n####### BEGIN details of this element #####\n"
    elem_info = ""
    timestamp_info = ""
    window_info = ""
    end = "\n####### END details of this element #####\n"

    elem_info = "".join([
        '>>>> element\n',
        str(elem),
        '\n',
        str(type(elem)),
        '\n',       
    ])

    if timestamp is None:
       timestamp_info = ">>>> timestamp\n no timestamp found"
    else:
        timestamp_info = "".join([
            '>>>> timestamp\n',
            'type(timestamp) -> {}\n'.format(repr(type(timestamp))),
            'timestamp.micros -> {}\n'.format(repr(timestamp.micros)),
            'timestamps on elements look incorrect, did you assign?\n' if ( timestamp < min_ts or timestamp > max_ts) else '',
            '' if (timestamp.micros < 0 or timestamp < min_ts or timestamp > max_ts) else 'timestamp.to_rfc3339() -> {}\n'.format(repr(timestamp.to_rfc3339())),
            '' if (timestamp.micros < 0 or timestamp < min_ts or timestamp > max_ts) else 'timestamp.to_utc_datetime() -> {}\n'.format(repr(timestamp.to_utc_datetime())), 
        ])

    if window is None:
       window_info = ">>>> window\n no window found" 
    else:
        window_info = "".join([
            '>>>>window\n',
            'type(window) -> {}\n'.format(repr(type(window))),
            'window.start -> {}\n'.format(window.start),
            'window.start (utc datetime) -> {} \n'.format(window.start.to_utc_datetime()) if (window.start > min_ts and window.start < max_ts) else '',
            'window.end -> {}\n'.format(window.end),
            'window.end (utc datetime) -> {} \n'.format(window.end.to_utc_datetime()) if (window.end > min_ts and window.end < max_ts) else '',
            'window.max_timestamp() -> {} \n'.format(window.max_timestamp()) if (window.max_timestamp() > min_ts and window.max_timestamp() < max_ts) else '',
            'window.max_timestamp() (utc datetime) -> {} \n'.format(window.max_timestamp().to_utc_datetime()) if (window.max_timestamp() > min_ts and window.max_timestamp() < max_ts) else '',            
            'windows on elements look incorrect, did you assign?\n' if (window.start < min_ts or window.end > max_ts) else '',            
        ])                 

    yield '\n'.join([begin, elem_info, timestamp_info, window_info, end]) 