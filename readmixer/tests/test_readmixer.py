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
Unit tests for ReadMixer.

These tests verify core functionality using a minimal test dataset (testing.fastq.gz).
Run with: python -m pytest tests/ -v
"""

import gzip
import os
import sys
import tempfile
import shutil

import pytest

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import process.util as util
from process.scramble import (
    find_fastq_files,
    count_reads_streaming,
    open_fastq_file,
    extract_description,
)


# Test data paths
TEST_DIR = os.path.dirname(os.path.abspath(__file__))
TEST_FASTQ_GZ = os.path.join(TEST_DIR, "testing.fastq.gz")


class TestFastqReading:
    """Tests for FASTQ file reading functionality."""

    def test_gzip_line_count(self):
        """Verify gzip file line counting works correctly."""
        line_count = util.gzip_file_line_count(TEST_FASTQ_GZ)
        # testing.fastq.gz contains 10 reads = 40 lines
        assert line_count == 40, f"Expected 40 lines, got {line_count}"

    def test_line_count_divisible_by_four(self):
        """Verify FASTQ file has valid format (lines divisible by 4)."""
        line_count = util.gzip_file_line_count(TEST_FASTQ_GZ)
        assert line_count % 4 == 0, "FASTQ line count must be divisible by 4"

    def test_open_fastq_file_yields_reads(self):
        """Verify open_fastq_file yields complete reads."""
        reads = list(open_fastq_file(TEST_FASTQ_GZ))
        assert len(reads) == 10, f"Expected 10 reads, got {len(reads)}"

    def test_read_structure(self):
        """Verify each read has correct 4-line structure."""
        for read in open_fastq_file(TEST_FASTQ_GZ):
            header, sequence, plus, quality = read
            assert header.startswith("@"), "Header must start with @"
            assert plus.strip() == "+", "Third line must be +"
            assert len(sequence.strip()) == len(quality.strip()), \
                "Sequence and quality lengths must match"


class TestDescriptionExtraction:
    """Tests for read description extraction."""

    def test_extract_description_removes_at_symbol(self):
        """Verify @ symbol is removed from description."""
        desc = extract_description("@ERR031880.1 test description\n")
        assert not desc.startswith("@"), "Description should not start with @"
        assert desc.startswith("ERR031880.1"), "Description content preserved"

    def test_extract_description_strips_newline(self):
        """Verify newlines are stripped from description."""
        desc = extract_description("@test\n")
        assert "\n" not in desc, "Newline should be stripped"


class TestRandomGeneration:
    """Tests for cryptographic random generation functions."""

    def test_random_bases_length(self):
        """Verify random_bases returns exactly 4 bases."""
        bases = util.random_bases()
        assert len(bases) == 4, f"Expected 4 bases, got {len(bases)}"

    def test_random_bases_valid_characters(self):
        """Verify random_bases only produces valid DNA bases."""
        valid_bases = set("ACGT")
        for _ in range(100):
            bases = util.random_bases()
            for base in bases:
                assert base in valid_bases, f"Invalid base: {base}"

    def test_random_tag_length(self):
        """Verify random_tag produces correct length."""
        tag = util.random_tag(7)
        # Each unit produces 4 bases
        assert len(tag) == 28, f"Expected 28 chars (7*4), got {len(tag)}"

    def test_random_alphanumeric_string_length(self):
        """Verify random_alphanumeric_string produces correct length."""
        result = util.random_alphanumeric_string(40)
        assert len(result) == 40, f"Expected 40 chars, got {len(result)}"

    def test_random_alphanumeric_string_characters(self):
        """Verify random_alphanumeric_string only uses alphanumeric chars."""
        import string
        valid_chars = set(string.ascii_letters + string.digits)
        result = util.random_alphanumeric_string(100)
        for char in result:
            assert char in valid_chars, f"Invalid character: {char}"


class TestIntToBase:
    """Tests for integer to DNA base conversion."""

    def test_int_to_base_mapping(self):
        """Verify correct integer to base mapping."""
        assert util.int_to_base(0) == "A"
        assert util.int_to_base(1) == "C"
        assert util.int_to_base(2) == "T"
        assert util.int_to_base(3) == "G"


class TestFileDiscovery:
    """Tests for FASTQ file discovery."""

    def test_find_fastq_files_in_directory(self):
        """Verify find_fastq_files discovers test file."""
        files = list(find_fastq_files(TEST_DIR))
        gz_files = [f for f in files if f.endswith(".gz")]
        assert len(gz_files) >= 1, "Should find at least the test file"
        assert any("testing.fastq.gz" in f for f in gz_files), \
            "Should find testing.fastq.gz"


class TestCountReads:
    """Tests for read counting functionality."""

    def test_count_reads_streaming(self):
        """Verify streaming read count is accurate."""
        files = list(find_fastq_files(TEST_DIR))
        gz_files = [f for f in files if "testing.fastq.gz" in f]
        
        total_reads, read_info, estimated_size = count_reads_streaming(iter(gz_files))
        
        assert total_reads == 10, f"Expected 10 reads, got {total_reads}"
        assert len(read_info) == 1, "Should have info for one file"
        assert estimated_size > 0, "Estimated size should be positive"


class TestResourceManager:
    """Tests for resource management utilities."""

    def test_system_capabilities_detection(self):
        """Verify system capabilities are detected."""
        caps = util.SystemCapabilities()
        assert caps.total_ram > 0, "Total RAM should be positive"
        assert caps.available_ram > 0, "Available RAM should be positive"
        assert caps.cpu_count > 0, "CPU count should be positive"

    def test_format_bytes(self):
        """Verify byte formatting works correctly."""
        caps = util.SystemCapabilities()
        assert "KB" in caps.format_bytes(1024)
        assert "MB" in caps.format_bytes(1024 * 1024)
        assert "GB" in caps.format_bytes(1024 * 1024 * 1024)

    def test_resource_manager_initialization(self):
        """Verify ResourceManager initializes without error."""
        rm = util.ResourceManager()
        assert rm.max_ram_bytes > 0, "Max RAM should be configured"

    def test_optimal_buffer_size(self):
        """Verify buffer size calculation returns valid value."""
        rm = util.ResourceManager()
        buffer_size = rm.get_optimal_buffer_size()
        assert buffer_size >= 1024 * 1024, "Buffer should be at least 1MB"


class TestIntegration:
    """Integration tests for full scrambling workflow."""

    def test_scramble_minimal_dataset(self):
        """Test full scrambling on minimal test dataset."""
        import process
        
        # Create temporary output directory
        with tempfile.TemporaryDirectory() as temp_output:
            # Run scramble on test data
            process.scramble(
                loglevel="ERROR",
                workingpath=TEST_DIR,
                zipup=False,
                outputdir=temp_output,
                chunk_size=100,
                num_workers=1,
                max_ram_gb=1
            )
            
            # Verify output files were created
            output_files = os.listdir(temp_output)
            
            # Should have FASTQ output file(s)
            fastq_files = [f for f in output_files if f.endswith(".fastq")]
            assert len(fastq_files) >= 1, "Should create at least one FASTQ output"
            
            # Should have key CSV file(s)
            csv_files = [f for f in output_files if f.endswith(".csv")]
            assert len(csv_files) >= 1, "Should create at least one key CSV file"
            
            # Verify output contains scrambled reads
            for fastq_file in fastq_files:
                filepath = os.path.join(temp_output, fastq_file)
                with open(filepath, "r") as f:
                    lines = f.readlines()
                # Should have 40 lines (10 reads * 4 lines each)
                assert len(lines) == 40, f"Expected 40 lines in output, got {len(lines)}"
                # Headers should be anonymized
                for i in range(0, len(lines), 4):
                    assert lines[i].startswith("@ReadMixerSecured"), \
                        "Headers should be anonymized"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

