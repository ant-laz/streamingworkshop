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

# Based upon these instructions & examples:
# https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/
# https://github.com/apache/beam-starter-python
# https://github.com/apache/beam/tree/master/sdks/python/apache_beam/examples/complete/juliaset

from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'apache-beam[gcp]',
]

setup(
    name="Beam Summit 2023 Workshop: Building a streamign pipeline with python",
    version="1.0",
    description="Code & Materials for a summit at the Beam Summit 2023.",
    author="Anthony Lazzaro & Israel Herraiz",
    packages=find_packages(),
    install_requires=REQUIRED_PACKAGES,
)