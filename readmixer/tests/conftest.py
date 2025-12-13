# Copyright 2025 GeneInfoSec
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Pytest configuration and fixtures for ReadMixer tests.
"""

import os
import sys

import pytest

# Ensure the parent directory is in the path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def test_dir():
    """Return the path to the test directory."""
    return os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def test_fastq_gz(test_dir):
    """Return the path to the test FASTQ.gz file."""
    return os.path.join(test_dir, "testing.fastq.gz")

