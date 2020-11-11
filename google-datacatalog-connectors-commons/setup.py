#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import setuptools

release_status = 'Development Status :: 4 - Beta'

with open('README.md') as readme_file:
    readme = readme_file.read()

setuptools.setup(
    name='google-datacatalog-connectors-commons',
    version='0.6.2',
    author='Google LLC',
    description='Common resources for Data Catalog connectors',
    packages=setuptools.find_packages(where='./src'),
    namespace_packages=['google', 'google.datacatalog_connectors'],
    package_dir={'': 'src'},
    include_package_data=True,
    install_requires=('google-cloud-monitoring>=1,<2', 'python-dateutil',
                      'google-cloud-datacatalog>=2'),
    setup_requires=('pytest-runner',),
    tests_require=('mock==3.0.5', 'pytest', 'pytest-cov',
                   'google-datacatalog-connectors-commons-test>=0.6.0'),
    classifiers=[
        release_status,
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Operating System :: OS Independent',
        'Topic :: Internet',
    ],
    long_description=readme,
    long_description_content_type='text/markdown',
    platforms='Posix; MacOS X; Windows',
)
