# ReadMixer - Dynamic FASTQ Obfuscation Tool
[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18188647.svg)](https://doi.org/10.5281/zenodo.18188647)
[![Software Heritage](https://archive.softwareheritage.org/badge/swh:1:dir:f49a93f67076731af7becfa3061b02d6b2fde061/)](https://archive.softwareheritage.org/swh:1:dir:f49a93f67076731af7becfa3061b02d6b2fde061/)

![ReadMixer](/read-mixer-simple-cartoon.jpg)


A high-performance tool for obfuscating FASTQ files with dynamic resource management capabilities. The tool automatically detects system capabilities and optimizes resource usage for both laptop and supercomputer environments.

## Features

- **Dynamic Resource Management**: Automatically detects available RAM, CPU cores, and disk space
- **Memory-Efficient Processing**: Streaming approach with external sorting for large datasets
- **Multi-platform Support**: Optimized for both limited-resource laptops and high-performance clusters
- **Parallel Processing**: Configurable worker processes for optimal throughput
- **Compression Support**: Optional gzip compression of output files (disabled by default)
- **Robust Processing**: Improved error handling and cleanup with timeout protection

## System Requirements

- Python 3.11 (recommended for best compatibility)
- GCC >= 9.3 (for pip-only installation) OR Anaconda/Miniconda (recommended for older systems)
- Minimum 1GB RAM (will auto-adjust for available resources)
- Sufficient disk space (tool will check requirements automatically)

**Technology stack**: ReadMixer is written with Python 3 and tested with Python 3.6.9, Python 3.10.12, and Python 3.11. It was developed in Ubuntu using the Windows Subsystem for Linux.

**Status**: ReadMixer is an open-source application created by GeneInfoSec.

## Installation

### Method 1: Using Conda with environment.yml (Recommended)

This method is **strongly recommended** for cluster environments or systems with older GCC versions (< 9.3):

```bash
# Install Anaconda/Miniconda if not already installed
# Navigate to the readmixer directory
cd readmixer

# Create environment from environment.yml file
conda env create -f environment.yml

# Activate the environment
conda activate readmixer-env
# OR on some systems:
source $HOME/anaconda3/bin/activate readmixer-env
```

### Method 2: Using Conda with Manual Package Installation

For systems with GCC < 9.3 (e.g., GCC 4.8.5 on older cluster systems):

```bash
# Create conda environment with numpy and contourpy pre-installed
conda create -y -n readmixer-env python=3.11 numpy=2.3 contourpy=1.3

# Activate the environment
conda activate readmixer-env
# OR on some systems:
source $HOME/anaconda3/bin/activate readmixer-env

# Install remaining dependencies with pip
cd readmixer
pip install -r requirements-pip.txt
```

**Why this method?** NumPy and contourpy require GCC >= 9.3 to build from source. Using conda pre-compiled binaries bypasses this requirement, making installation possible on older systems.

### Method 3: Using pip (Requires GCC >= 9.3)

Only use this method if you have GCC 9.3 or newer:

```bash
# Clone repository
git clone <repository-url>
cd ReadMixer/readmixer

# Using virtual environment (recommended)
python -m venv readmixer-env
source readmixer-env/bin/activate  # On Windows: readmixer-env\Scripts\activate
pip install -r requirements.txt

# Using system Python (not recommended)
pip install -r requirements.txt
```

### Method 4: Using Docker

For users who prefer containerized deployment:

**Standard Docker (requires base image with GCC >= 9.3):**
```bash
cd readmixer
docker build -t readmixer:latest .
docker run -v /path/to/data:/data readmixer:latest -s -p /data
```

**Docker with Conda (works on any system):**
```bash
cd readmixer
docker build -f Dockerfile.conda -t readmixer-conda:latest .
docker run -v /path/to/data:/data readmixer-conda:latest -s -p /data
```

### Additional Resources

For detailed installation instructions, troubleshooting, and environment-specific recommendations, see [INSTALLATION.md](readmixer/INSTALLATION.md).

## Usage

### Basic Obfuscation
```bash
python readmixer.py -s -p /path/to/fastq/files
```

### Advanced Usage with Resource Control

#### Manual RAM Limit
```bash
# Limit to 8GB RAM
python readmixer.py -s -p /path/to/fastq/files -m 8

# Use 32GB RAM for large datasets
python readmixer.py -s -p /path/to/fastq/files -m 32
```

#### Manual Worker and Chunk Size Control
```bash
# Use 16 workers with 2M read chunks
python readmixer.py -s -p /path/to/fastq/files -w 16 -c 2000000

# Let system auto-optimize (recommended)
python readmixer.py -s -p /path/to/fastq/files
```

#### Full Example with All Options
```bash
python readmixer.py -s -p /path/to/fastq/files -o /output/directory -z -m 16 -w 8 -c 1000000 -l info
```

## Command Line Arguments

### Core Operations
- `-s, --scramble`: Scramble/obfuscate files in specified location
- `-p, --pathname`: Input directory path (default: current directory)
- `-o, --output`: Output directory path

### Resource Management
- `-m, --max-ram`: Maximum RAM to use in GB (auto-detected if not specified)
- `-w, --workers`: Number of worker processes (auto-optimized if not specified)
- `-c, --chunk_size`: Chunk size in number of reads (auto-calculated if not specified)

### Additional Options
- `-z, --zip`: Enable gzip compression of output files (by default, output files are NOT compressed)
- `-l, --loglevel`: Logging level (error, warning, info, debug, critical)

## Automatic Resource Optimization

### Memory Management
The tool automatically:
- Detects total and available system RAM
- Reserves memory for the OS and other processes
- Calculates optimal chunk sizes based on available memory
- Monitors memory usage during processing
- Issues warnings when memory usage exceeds 85%

### CPU Optimization
- Detects physical and logical CPU cores
- Optimizes worker count for I/O vs CPU intensive tasks
- Balances worker count with available RAM per worker

### Disk Space Management
- Checks available disk space before processing with realistic estimates
- Accounts for peak temporary file usage during chunk processing
- Prevents processing if insufficient disk space (with safety margins)
- Improved disk space calculations based on actual usage patterns

### Buffer Size Optimization
- Dynamically sets file I/O buffer sizes based on available RAM
- Uses larger buffers on high-memory systems for better performance

## Performance Benchmarks

### Real-World Performance Data

ReadMixer has been extensively benchmarked on high-performance systems. The following chart shows actual performance data from a production AMD Ryzen 9 9950X system (32 cores, 96GB RAM):

![ReadMixer Performance Analysis](/readmixer/readmixer_performance.png)

**Key Performance Metrics:**
- **Average Processing Rate**: 1.150 ± 0.008 GB/min
- **Linear Scaling**: Excellent linear relationship between input size and processing time
- **Consistency**: <1% variation in processing rate across different dataset sizes
- **Memory Efficiency**: Uses ~50GB RAM for optimal performance with 9 workers and 5M read chunks

### Performance Estimates by System Configuration

#### High-End Workstation (AMD Ryzen 9 9950X - Benchmarked)
- **Specs**: 32 cores, 96GB RAM
- **Optimal Config**: 9 workers, 5M read chunks, 50GB RAM usage
- **Performance**: 1.15 GB/min
- **Processing Times**:
  - 10GB dataset: ~9 minutes
  - 50GB dataset: ~43 minutes  
  - 100GB dataset: ~87 minutes (1.4 hours)
  - 500GB dataset: ~435 minutes (7.3 hours)
  - 1TB dataset: ~870 minutes (14.5 hours)

#### Mid-Range System (4 cores, 24GB RAM - Estimated)
Based on scaling analysis and resource constraints:
- **Optimal Config**: 4 workers, 1.5M read chunks, 7.2GB RAM usage
- **Estimated Performance**: 0.40-0.50 GB/min (35-43% of high-end system)
- **Processing Times**:
  - 10GB dataset: ~20-25 minutes
  - 50GB dataset: ~100-125 minutes (1.7-2.1 hours)
  - 100GB dataset: ~200-250 minutes (3.3-4.2 hours)
  - 500GB dataset: ~1000-1250 minutes (16.7-20.8 hours)
  - 1TB dataset: ~2000-2500 minutes (33-42 hours)

*Note: 4-core estimates are conservative and based on linear CPU scaling with memory constraints. Actual performance may vary based on storage speed, RAM speed, and system configuration.*

#### Laptop System (4 cores, 8GB RAM - Estimated)
- **Optimal Config**: 3 workers, 800K read chunks, 2.9GB RAM usage
- **Estimated Performance**: 0.25-0.35 GB/min (22-30% of high-end system)
- **Processing Times**:
  - 10GB dataset: ~29-40 minutes
  - 50GB dataset: ~143-200 minutes (2.4-3.3 hours)
  - 100GB dataset: ~286-400 minutes (4.8-6.7 hours)
  - Larger datasets: Not recommended due to memory constraints

### Performance Factors

#### CPU Scaling
- Performance scales roughly linearly with available CPU cores
- I/O intensive workload benefits from moderate parallelization
- Optimal worker count is typically 50-75% of available cores

#### Memory Impact
- Larger chunks improve efficiency but require more RAM
- Memory constraint is often the limiting factor, not CPU
- System automatically balances chunk size vs worker count

#### Storage Performance
- SSD storage provides 2-3x better performance than traditional HDDs
- Network storage may reduce performance by 20-50%
- Temporary file I/O is the primary bottleneck for large datasets

#### Input Data Characteristics
- Larger FASTQ files process more efficiently (better chunk utilization)
- Compressed input files add ~10-15% processing overhead
- File fragmentation can impact performance on traditional HDDs

### Optimization Recommendations

#### For Maximum Throughput
```bash
# Let system auto-optimize (recommended)
python readmixer.py -s -p /path/to/data -l info

# For high-memory systems, increase RAM limit
python readmixer.py -s -p /path/to/data -m 64 -l info
```

#### For Resource-Constrained Systems
```bash
# Limit RAM usage
python readmixer.py -s -p /path/to/data -m 4 -w 2 -c 500000

# Enable compression to save disk space
python readmixer.py -s -p /path/to/data -m 4 -w 2 -z
```

#### For Debugging Performance Issues
```bash
# Detailed logging to analyze bottlenecks
python readmixer.py -s -p /path/to/data -l debug
```

## Performance Examples

### Laptop Environment (8GB RAM, 4 cores)
```bash
# System will automatically use:
# - ~5GB RAM for processing
# - 6-8 worker processes
# - 2MB I/O buffers
# - Chunk sizes around 800K-1M reads
python readmixer.py -s -p /data/fastq -l info
```

### Workstation Environment (32GB RAM, 16 cores)
```bash
# System will automatically use:
# - ~24GB RAM for processing  
# - 16-24 worker processes
# - 4MB I/O buffers
# - Chunk sizes around 4-6M reads
python readmixer.py -s -p /data/fastq -l info
```

### Supercomputer Environment (512GB RAM, 64 cores)
```bash
# System will automatically use:
# - ~400GB RAM for processing
# - 16-32 worker processes (limited by I/O)
# - 8MB I/O buffers
# - Chunk sizes around 50-100M reads
python readmixer.py -s -p /data/fastq -l info
```

## Logging and Monitoring

Set logging level to `info` to see detailed resource usage and processing status:

```bash
python readmixer.py -s -p /data/fastq -l info
```

Example output:
```
System capabilities detected:
  Total RAM: 32.0 GB
  Available RAM: 28.5 GB
  CPU cores: 8 physical, 16 logical
  Max RAM for processing: 22.8 GB
Calculated optimal chunk size: 2,500,000 reads
Optimal worker count: 12
Estimated memory per chunk: 3.0 GB
Disk space check passed:
  Peak space needed: 45.2 GB
  Available space: 125.7 GB
  Safety margin: 80.5 GB
Starting chunk processing...
All 10 chunks processed successfully
Merging output files...
Process completed in 1234.5678 seconds
```

### Monitoring Features

- **Resource Detection**: Automatic system capability detection and optimization
- **Memory Monitoring**: Real-time memory usage tracking with warnings at 85%+ usage
- **Disk Space Validation**: Pre-processing checks with realistic space estimates
- **Processing Status**: Clear log messages showing processing phases and completion

## Error Handling

The tool includes comprehensive error handling:
- Validates FASTQ file formats
- Checks file permissions
- Monitors memory usage with warnings
- Graceful cleanup of temporary files
- Detailed error messages with suggestions

## Advanced Features

### Memory Pressure Handling
- Automatic memory usage monitoring
- Warnings when memory usage exceeds safe thresholds
- Graceful degradation under memory pressure

### Temporary File Management
- Creates temporary directories for intermediate processing
- Robust cleanup with timeout protection to prevent hanging
- Automatic cleanup even if processing is interrupted
- Optimized temporary file locations

### File Format Support
- Supports both compressed (.gz) and uncompressed FASTQ files
- Automatic format detection
- Preserves file integrity during processing

## Troubleshooting

### Installation Issues

**"NumPy requires GCC >= 9.3" error when running `pip install -r requirements.txt`:**

This error occurs on systems with older GCC versions (e.g., GCC 4.8.5 on older cluster systems). The error message looks like:
```
../meson.build:28:4: ERROR: Problem encountered: NumPy requires GCC >= 9.3
```

**Quick Fix:** See [QUICKSTART_OLD_GCC.md](readmixer/QUICKSTART_OLD_GCC.md) for a step-by-step guide.

**Solution:** Use conda to install numpy and contourpy instead of pip:
```bash
# Method 1: Use environment.yml (easiest)
conda env create -f environment.yml
conda activate readmixer-env

# Method 2: Manual installation
conda create -y -n readmixer-env python=3.11 numpy=2.3 contourpy=1.3
conda activate readmixer-env
pip install -r requirements-pip.txt
```

**"contourpy" build error:**
Similar to numpy, contourpy may fail with older GCC versions. Use the same conda-based solution above.

**Checking your GCC version:**
```bash
gcc --version
```
If the version is less than 9.3, use the conda installation method.

### Runtime Issues

**"Insufficient disk space" error:**
- Check available disk space in output directory
- Consider using a different output location with more space
- The tool requires significant temporary space (up to 8-10x input size during peak processing)
- Use `-z` flag to compress output files if disk space is limited

**High memory usage warnings:**
- Reduce chunk size with `-c` parameter
- Limit RAM usage with `-m` parameter
- Close other memory-intensive applications

**Process hangs or takes too long:**
- Normal processing time is ~2-3 hours for 100M reads
- If process hangs for >3 hours, it may be stuck in cleanup - safe to kill
- Check disk space - insufficient space can cause hanging during file operations

**Slow processing:**
- Increase worker count with `-w` parameter (if you have available CPU cores)
- Use SSD storage for better I/O performance
- Ensure sufficient RAM for larger chunk sizes
- Consider using `-z` for compression to reduce final file sizes

### Performance Tips

1. **For large datasets**: Let the system auto-optimize resources (recommended)
2. **For repeated processing**: Note the optimal parameters from logs and reuse them
3. **For limited disk space**: Use `-z` flag to compress output files
4. **For limited resources**: Set explicit RAM limits with `-m`
5. **For debugging**: Use `-l info` for processing status or `-l debug` for detailed information
6. **Expected timing**: ~2-3 hours for 100M reads, ~30-60 minutes for 10M reads

## Testing

ReadMixer includes a comprehensive test suite to verify functionality. For detailed testing documentation, see [TESTING.md](TESTING.md).

### Quick Start

```bash
cd readmixer
python -m pytest tests/ -v
```

### Test Data

A minimal test dataset (`tests/testing.fastq.gz`) is included with 10 sequencing reads for unit and integration testing.

### Test Coverage

The test suite includes 19 tests covering:

| Category | Description |
|----------|-------------|
| FASTQ Reading | File parsing, format validation, read counting |
| Cryptographic Functions | Random base generation, tag creation, alphanumeric strings |
| Resource Management | System capability detection, buffer sizing |
| Integration | End-to-end scrambling workflow |

### Example Test Run

```bash
$ cd readmixer
$ python -m pytest tests/ -v

============================= test session starts ==============================
collected 19 items

tests/test_readmixer.py::TestFastqReading::test_gzip_line_count PASSED
tests/test_readmixer.py::TestFastqReading::test_open_fastq_file_yields_reads PASSED
tests/test_readmixer.py::TestRandomGeneration::test_random_bases_valid_characters PASSED
tests/test_readmixer.py::TestResourceManager::test_system_capabilities_detection PASSED
tests/test_readmixer.py::TestIntegration::test_scramble_minimal_dataset PASSED
...
============================== 19 passed =======================================
```

### Running with Test Data

You can also manually test ReadMixer using the included test data:

```bash
cd readmixer
python readmixer.py -s -p tests/ -o test_output/ -l info
```

This will scramble the test FASTQ file and produce:
- `ReadMixer.<id>.part000.fastq` - Anonymized output with scrambled read order
- `ReadMixerKeys.<id>.testing.fastq.gz.csv` - Key file mapping original to scrambled reads

## Citation

If you use ReadMixer in your research, please cite it! Proper citation helps us track the impact of our work and supports continued development.

### How to Cite

**Text Citation:**
```
Sawaya, S., & Parks, J. (2025). ReadMixer - Dynamic FASTQ Obfuscation Tool 
(Version 1.0.1) [Computer software]. https://github.com/Geneinfosec-Inc/ReadMixer
```

**BibTeX:**
```bibtex
@software{sawaya_parks_2025_readmixer,
  author       = {Sawaya, Sterling and Parks, Joseph},
  title        = {ReadMixer - Dynamic FASTQ Obfuscation Tool},
  year         = 2025,
  publisher    = {GitHub},
  url          = {https://github.com/Geneinfosec-Inc/ReadMixer},
  version      = {1.0.1}
}
```

### Citation File

A `CITATION.cff` file is included in this repository with complete citation metadata. GitHub and other platforms can automatically generate citations from this file.

### Archival and Preservation

ReadMixer is committed to long-term preservation and accessibility:

- **GitHub**: Primary development and version control
- **Zenodo**: Long-term archival with DOI (Digital Object Identifier) for academic citations
- **Software Heritage**: Universal source code archive for permanent preservation

For detailed information about our archival strategy, preservation locations, and persistent identifiers, see [ARCHIVAL_STRATEGY.md](ARCHIVAL_STRATEGY.md).

## License

Copyright 2025 GeneInfoSec, created by Sterling Sawaya and Joseph Parks

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

See the [LICENSE](LICENSE) file for full details.

## Support
For support or questions, please contact: [joe@geneinfosec.com](mailto:joe@geneinfosec.com)
