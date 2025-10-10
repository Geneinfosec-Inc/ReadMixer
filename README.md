# ReadMixer - Dynamic FASTQ Encryption Tool

A high-performance tool for encrypting FASTQ files with dynamic resource management capabilities. The tool automatically detects system capabilities and optimizes resource usage for both laptop and supercomputer environments.

## Features

- **Dynamic Resource Management**: Automatically detects available RAM, CPU cores, and disk space
- **Memory-Efficient Processing**: Streaming approach with external sorting for large datasets
- **Multi-platform Support**: Optimized for both limited-resource laptops and high-performance clusters
- **Parallel Processing**: Configurable worker processes for optimal throughput
- **Compression Support**: Optional gzip compression of output files (disabled by default)
- **Robust Processing**: Improved error handling and cleanup with timeout protection

## System Requirements

- Python 3.7+
- Required packages: `pip install -r requirements.txt`
- Minimum 1GB RAM (will auto-adjust for available resources)
- Sufficient disk space (tool will check requirements automatically)

**Technology stack**: ReadMixer is written with Python 3 and tested with Python 3.6.9, Python 3.10.12, and Python 3.11. It was developed in Ubuntu using the Windows Subsystem for Linux.

**Status**: ReadMixer is an open-source application created by GeneInfoSec.

## Installation

### Using Conda (Recommended)
```bash
# Install Anaconda/Miniconda if not already installed
# Create a conda environment for ReadMixer
conda create -y -n readmixer-env python=3.11
# Activate the environment
source $HOME/anaconda3/bin/activate readmixer-env
# Install dependencies
pip install -r requirements.txt
```

### Using pip
```bash
# Clone repository
git clone <repository-url>
cd ReadMixer

# Using virtual environment (recommended)
python -m venv readmixer-env
source readmixer-env/bin/activate  # On Windows: readmixer-env\Scripts\activate
pip install -r requirements.txt

# Using system Python (not recommended for large files)
pip install -r requirements.txt
```

## Usage

### Basic Encryption
```bash
python readmixer.py -e -p /path/to/fastq/files
```

### Advanced Usage with Resource Control

#### Manual RAM Limit
```bash
# Limit to 8GB RAM
python readmixer.py -e -p /path/to/fastq/files -m 8

# Use 32GB RAM for large datasets
python readmixer.py -e -p /path/to/fastq/files -m 32
```

#### Manual Worker and Chunk Size Control
```bash
# Use 16 workers with 2M read chunks
python readmixer.py -e -p /path/to/fastq/files -w 16 -c 2000000

# Let system auto-optimize (recommended)
python readmixer.py -e -p /path/to/fastq/files
```

#### Full Example with All Options
```bash
python readmixer.py -e -p /path/to/fastq/files -o /output/directory -z -m 16 -w 8 -c 1000000 -l info
```

## Command Line Arguments

### Core Operations
- `-e, --encrypt`: Encrypt files in specified location
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

![ReadMixer Performance Analysis](readmixer_performance.png)

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
python readmixer.py -e -p /path/to/data -l info

# For high-memory systems, increase RAM limit
python readmixer.py -e -p /path/to/data -m 64 -l info
```

#### For Resource-Constrained Systems
```bash
# Limit RAM usage
python readmixer.py -e -p /path/to/data -m 4 -w 2 -c 500000

# Enable compression to save disk space
python readmixer.py -e -p /path/to/data -m 4 -w 2 -z
```

#### For Debugging Performance Issues
```bash
# Detailed logging to analyze bottlenecks
python readmixer.py -e -p /path/to/data -l debug
```

## Performance Examples

### Laptop Environment (8GB RAM, 4 cores)
```bash
# System will automatically use:
# - ~5GB RAM for processing
# - 6-8 worker processes
# - 2MB I/O buffers
# - Chunk sizes around 800K-1M reads
python readmixer.py -e -p /data/fastq -l info
```

### Workstation Environment (32GB RAM, 16 cores)
```bash
# System will automatically use:
# - ~24GB RAM for processing  
# - 16-24 worker processes
# - 4MB I/O buffers
# - Chunk sizes around 4-6M reads
python readmixer.py -e -p /data/fastq -l info
```

### Supercomputer Environment (512GB RAM, 64 cores)
```bash
# System will automatically use:
# - ~400GB RAM for processing
# - 16-32 worker processes (limited by I/O)
# - 8MB I/O buffers
# - Chunk sizes around 50-100M reads
python readmixer.py -e -p /data/fastq -l info
```

## Logging and Monitoring

Set logging level to `info` to see detailed resource usage and processing status:

```bash
python readmixer.py -e -p /data/fastq -l info
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

### Common Issues

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

## License

[Include your license information here]

## Support

[Include support/contact information here]
