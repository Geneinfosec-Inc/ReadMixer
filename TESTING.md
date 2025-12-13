# ReadMixer Testing Guide

This document describes the testing infrastructure for ReadMixer, including test data, test cases, and expected results.

## Test Data

### testing.fastq.gz

Location: `readmixer/tests/testing.fastq.gz`

A minimal FASTQ file containing 10 sequencing reads extracted from real sequencing data. This file is used for unit and integration testing.

**Specifications:**
- **Format:** Gzip-compressed FASTQ
- **Read count:** 10 reads
- **Line count:** 40 lines (4 lines per read)
- **File size:** ~1 KB

**Sample content (first read):**
```
@ERR031880.1 A819WDABXX:6:1101:1469:1974#NGACCAAT/1
NCTCCATAGTCTGTACTTGTATGCTCTGCTGCCTCAGGTCATCTGCAGAATGAATGGCTGGGGTGTTTGTCTTTTTCCCCCCCCCGGACC
+
#1./-/6/04<<3<>???;????;<<<<<<::<><.6<591:497<4<<<<<11>?=?################################
```

## Running Tests

### Prerequisites

Ensure you have the required dependencies installed:

```bash
cd readmixer
pip install pytest
```

### Run All Tests

```bash
cd readmixer
python -m pytest tests/ -v
```

### Run Specific Test Classes

```bash
# Run only FASTQ reading tests
python -m pytest tests/test_readmixer.py::TestFastqReading -v

# Run only integration tests
python -m pytest tests/test_readmixer.py::TestIntegration -v
```

### Run with Coverage (optional)

```bash
pip install pytest-cov
python -m pytest tests/ -v --cov=process --cov-report=term-missing
```

## Test Cases

### TestFastqReading

Tests for FASTQ file reading functionality.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_gzip_line_count` | Count lines in gzipped FASTQ | 40 lines |
| `test_line_count_divisible_by_four` | Validate FASTQ format | Line count % 4 == 0 |
| `test_open_fastq_file_yields_reads` | Read parsing | 10 reads yielded |
| `test_read_structure` | Validate 4-line read structure | Header starts with @, plus line is +, sequence and quality lengths match |

### TestDescriptionExtraction

Tests for read description/header extraction.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_extract_description_removes_at_symbol` | Remove @ from header | Description does not start with @ |
| `test_extract_description_strips_newline` | Clean whitespace | No newlines in output |

### TestRandomGeneration

Tests for cryptographic random generation functions used in obfuscation.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_random_bases_length` | Generate 4 random bases | Exactly 4 characters |
| `test_random_bases_valid_characters` | Valid DNA bases only | Only A, C, T, G |
| `test_random_tag_length` | Generate random tag | 28 characters (7 × 4) |
| `test_random_alphanumeric_string_length` | Generate file name | 40 characters |
| `test_random_alphanumeric_string_characters` | Alphanumeric only | Only [a-zA-Z0-9] |

### TestIntToBase

Tests for integer to DNA base conversion.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_int_to_base_mapping` | Numeric encoding | 0→A, 1→C, 2→T, 3→G |

### TestFileDiscovery

Tests for FASTQ file discovery in directories.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_find_fastq_files_in_directory` | Locate FASTQ files | Finds testing.fastq.gz |

### TestCountReads

Tests for read counting functionality.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_count_reads_streaming` | Count reads in file | 10 reads, 1 file, size > 0 |

### TestResourceManager

Tests for system resource management.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_system_capabilities_detection` | Detect system resources | RAM > 0, CPU > 0 |
| `test_format_bytes` | Format byte values | Correct KB/MB/GB formatting |
| `test_resource_manager_initialization` | Initialize manager | Max RAM configured |
| `test_optimal_buffer_size` | Calculate buffer size | At least 1 MB |

### TestIntegration

End-to-end integration tests.

| Test | Description | Expected Result |
|------|-------------|-----------------|
| `test_scramble_minimal_dataset` | Full scrambling workflow | Output FASTQ with anonymized headers, key CSV file generated |

## Expected Output

### Successful Scrambling

When running ReadMixer on the test data:

```bash
python readmixer.py -s -p tests/ -o test_output/
```

**Expected output files:**
1. `ReadMixer.<random_id>.part000.fastq` - Scrambled FASTQ with anonymized headers
2. `ReadMixerKeys.<random_id>.testing.fastq.gz.csv` - Key file mapping original to scrambled reads

**Anonymized header format:**
```
@ReadMixerSecured ACGTACGTACGTACGTACGTACGTACGT
```

**Key file format (CSV):**
```csv
file_name,read_description,secure_index,original_index,tag,output_file_index
testing.fastq.gz,ERR031880.1 A819WDABXX...,0,5,ACGTACGT...,0
```

### Test Output Example

```
$ python -m pytest tests/ -v

========================= test session starts ==========================
platform linux -- Python 3.11.x, pytest-x.x.x
collected 18 items

tests/test_readmixer.py::TestFastqReading::test_gzip_line_count PASSED
tests/test_readmixer.py::TestFastqReading::test_line_count_divisible_by_four PASSED
tests/test_readmixer.py::TestFastqReading::test_open_fastq_file_yields_reads PASSED
tests/test_readmixer.py::TestFastqReading::test_read_structure PASSED
tests/test_readmixer.py::TestDescriptionExtraction::test_extract_description_removes_at_symbol PASSED
tests/test_readmixer.py::TestDescriptionExtraction::test_extract_description_strips_newline PASSED
tests/test_readmixer.py::TestRandomGeneration::test_random_bases_length PASSED
tests/test_readmixer.py::TestRandomGeneration::test_random_bases_valid_characters PASSED
tests/test_readmixer.py::TestRandomGeneration::test_random_tag_length PASSED
tests/test_readmixer.py::TestRandomGeneration::test_random_alphanumeric_string_length PASSED
tests/test_readmixer.py::TestRandomGeneration::test_random_alphanumeric_string_characters PASSED
tests/test_readmixer.py::TestIntToBase::test_int_to_base_mapping PASSED
tests/test_readmixer.py::TestFileDiscovery::test_find_fastq_files_in_directory PASSED
tests/test_readmixer.py::TestCountReads::test_count_reads_streaming PASSED
tests/test_readmixer.py::TestResourceManager::test_system_capabilities_detection PASSED
tests/test_readmixer.py::TestResourceManager::test_format_bytes PASSED
tests/test_readmixer.py::TestResourceManager::test_resource_manager_initialization PASSED
tests/test_readmixer.py::TestResourceManager::test_optimal_buffer_size PASSED
tests/test_readmixer.py::TestIntegration::test_scramble_minimal_dataset PASSED

========================= 18 passed in X.XXs ===========================
```

## Troubleshooting

### Import Errors

If you encounter import errors, ensure you're running tests from the `readmixer` directory:

```bash
cd readmixer
python -m pytest tests/ -v
```

### Missing Dependencies

Install test dependencies:

```bash
pip install pytest pycryptodome psutil numpy
```

### Permission Errors

Ensure write permissions for the test output directory:

```bash
chmod 755 tests/
```

## Adding New Tests

1. Add test functions to `tests/test_readmixer.py`
2. Follow the naming convention: `test_<description>`
3. Group related tests in classes: `class Test<Category>`
4. Update this document with new test descriptions

## License

Copyright 2025 GeneInfoSec

Licensed under the Apache License, Version 2.0.

