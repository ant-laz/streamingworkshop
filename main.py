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

import logging

from apache_beam.options.pipeline_options import PipelineOptions

from my_pipeline import pipeline


class MyOptions(PipelineOptions):
  @classmethod
  def _add_argparse_args(cls, parser):
    parser.add_argument(
        '--input_filename',
        help='In data/input folder, the file to of taxi rides to stream.')
    parser.add_argument(
        '--output_filename', 
        help='In data/output folder, the file to stream output to.')

if __name__ == "__main__":

    # enable logging so we can see info on pipeline progress
    logging.getLogger().setLevel(logging.INFO)

    #Grab the pipeline configuration from command line flags
    pipeline.run(beam_options = MyOptions())

