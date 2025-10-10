import gzip
import logging
from Crypto.Random import random
import secrets
import shutil
import string
import psutil
import os
import math
import time


# function with our specified numeric base replacement
def int_to_base(base_int):
    if base_int == 0:
        return "A"
    elif base_int == 1:
        return "C"
    elif base_int == 2:
        return "T"
    elif base_int == 3:
        return "G"

###Function to determine number of lines in gzipped file
def gzip_file_line_count(fname):
    try:
        with gzip.open(fname,"rt") as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    except:
        logging.error("Fatal error accessing files for processing. Please ensure permissions are correct. Terminating.")
        exit()

###Function to determine number of lines in file
def file_line_count(fname):
    try:
        with open(fname,"rt") as f:
            for i, l in enumerate(f):
                pass
        return i + 1
    except:
        logging.error("Fatal error accessing files for processing. Please ensure permissions are correct. Terminating.")
        exit()

#Function to get a random string, used here only for file names
def random_alphanumeric_string(length):
    letters_and_digits = string.ascii_letters + string.digits
    result_str = ''.join((random.choice(letters_and_digits) for i in range(length)))
    return result_str


##Crypto function to derive four random base pairs by choosing numbers 0-3
def random_bases():
    rand_dex = secrets.randbits(16)
    rightmost_mask = 0b00000011
    rand_bases = int_to_base((rand_dex >> 0) & rightmost_mask)
    rand_bases += int_to_base((rand_dex >> 2) & rightmost_mask)
    rand_bases += int_to_base((rand_dex >> 4) & rightmost_mask)
    rand_bases += int_to_base((rand_dex >> 6) & rightmost_mask)
    return rand_bases


def random_tag(StringLength):
    
    aSeq=""
    
    for x in range(StringLength):
        aSeq +=  random_bases()
    return aSeq


class SystemCapabilities:
    """Class to detect and report system capabilities."""
    
    def __init__(self):
        self.total_ram = psutil.virtual_memory().total
        self.available_ram = psutil.virtual_memory().available
        self.cpu_count = psutil.cpu_count()
        self.cpu_count_logical = psutil.cpu_count(logical=True)
        
    def get_available_disk_space(self, path='.'):
        """Get available disk space for a given path in bytes."""
        try:
            return shutil.disk_usage(path).free
        except Exception:
            # Fallback if disk usage check fails
            return 10 * 1024 * 1024 * 1024  # 10GB default
    
    def get_memory_info(self):
        """Get current memory usage information."""
        memory = psutil.virtual_memory()
        return {
            'total': memory.total,
            'available': memory.available,
            'percent_used': memory.percent,
            'free': memory.free
        }
    
    def format_bytes(self, bytes_value):
        """Format bytes to human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024.0:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024.0
        return f"{bytes_value:.1f} PB"


class ResourceManager:
    """Manages system resources and provides optimal configuration for processing."""
    
    def __init__(self, max_ram_gb=None, logger=None):
        self.capabilities = SystemCapabilities()
        self.logger = logger or logging.getLogger(__name__)
        
        # Set maximum RAM to use (in bytes)
        if max_ram_gb is not None:
            self.max_ram_bytes = max_ram_gb * 1024 * 1024 * 1024
            self.logger.info(f"Manual RAM limit set to {max_ram_gb:.1f} GB")
        else:
            # Use 80% of available RAM by default, but leave at least 2GB free
            available_ram = self.capabilities.available_ram
            min_free_ram = min(2 * 1024 * 1024 * 1024, available_ram * 0.2)  # 2GB or 20%
            self.max_ram_bytes = max(available_ram - min_free_ram, 1024 * 1024 * 1024)  # At least 1GB
        
        self._log_system_info()
    
    def _log_system_info(self):
        """Log system information."""
        caps = self.capabilities
        self.logger.info(f"System capabilities detected:")
        self.logger.info(f"  Total RAM: {caps.format_bytes(caps.total_ram)}")
        self.logger.info(f"  Available RAM: {caps.format_bytes(caps.available_ram)}")
        self.logger.info(f"  CPU cores: {caps.cpu_count} physical, {caps.cpu_count_logical} logical")
        self.logger.info(f"  Max RAM for processing: {caps.format_bytes(self.max_ram_bytes)}")
    
    def get_optimal_chunk_size(self, total_reads, estimated_read_size=400, num_workers=None):
        """
        Calculate optimal chunk size based on available RAM and data size.
        
        Args:
            total_reads: Total number of reads to process
            estimated_read_size: Estimated size per read in bytes
            num_workers: Number of workers (if known), used to calculate total memory usage
            
        Returns:
            Optimal chunk size (number of reads)
        """
        # Estimate memory needed per read (read data + metadata + overhead)
        memory_per_read = estimated_read_size * 3  # 3x overhead for processing
        
        # Calculate how many reads we can fit in our RAM budget
        # Reserve some RAM for OS and other operations
        usable_ram = self.max_ram_bytes * 0.8
        
        # If we know the number of workers, account for parallel memory usage
        if num_workers is not None:
            # Each worker will hold one chunk in memory simultaneously
            usable_ram_per_worker = usable_ram / num_workers
            max_reads_per_chunk = int(usable_ram_per_worker / memory_per_read)
        else:
            # Conservative estimate assuming some parallelization
            max_reads_per_chunk = int(usable_ram / (memory_per_read * 4))  # Assume 4 workers
        
        # Don't make chunks too small (minimum 10k reads) or too large (maximum 5M reads)
        min_chunk_size = min(10000, total_reads)
        max_chunk_size = min(5000000, max_reads_per_chunk)  # Reduced from 10M to 5M
        
        # If we can fit all reads in memory with good chunk size, use a reasonable chunk size
        if total_reads <= max_reads_per_chunk:
            chunk_size = min(total_reads, 1000000)  # 1M default
        else:
            # Calculate number of chunks needed
            num_chunks = math.ceil(total_reads / max_reads_per_chunk)
            chunk_size = math.ceil(total_reads / num_chunks)
        
        chunk_size = max(min_chunk_size, min(chunk_size, max_chunk_size))
        
        self.logger.info(f"Calculated optimal chunk size: {chunk_size:,} reads")
        self.logger.info(f"Estimated memory per chunk: {self.capabilities.format_bytes(chunk_size * memory_per_read)}")
        
        return chunk_size
    
    def get_optimal_worker_count(self, io_intensive=True, chunk_size=None, estimated_read_size=400):
        """
        Get optimal number of worker processes, considering memory constraints.
        
        Args:
            io_intensive: True if workload is I/O intensive, False if CPU intensive
            chunk_size: Size of chunks (if known), used to calculate memory per worker
            estimated_read_size: Estimated size per read in bytes
            
        Returns:
            Optimal number of workers
        """
        # Start with CPU-based calculation
        if io_intensive:
            # For I/O intensive tasks, can use more workers than CPU cores
            max_workers_by_cpu = min(self.capabilities.cpu_count_logical, 16)
        else:
            # For CPU intensive tasks, match physical CPU cores
            max_workers_by_cpu = self.capabilities.cpu_count
        
        # Ensure we have at least 1 worker
        max_workers_by_cpu = max(1, max_workers_by_cpu)
        
        # Calculate memory constraints
        usable_ram = self.max_ram_bytes * 0.8  # Leave 20% for OS
        
        if chunk_size is not None:
            # Calculate memory needed per worker based on chunk size
            memory_per_read = estimated_read_size * 3  # 3x overhead for processing
            memory_per_worker = chunk_size * memory_per_read
            max_workers_by_ram = max(1, int(usable_ram / memory_per_worker))
            
            self.logger.info(f"Memory constraint: {max_workers_by_ram} workers max "
                           f"(each using {self.capabilities.format_bytes(memory_per_worker)})")
        else:
            # Conservative estimate - each worker needs at least 512MB
            max_workers_by_ram = max(1, int(usable_ram / (512 * 1024 * 1024)))
        
        # Use the most restrictive constraint
        workers = min(max_workers_by_cpu, max_workers_by_ram)
        
        self.logger.info(f"Optimal worker count: {workers} (CPU limit: {max_workers_by_cpu}, RAM limit: {max_workers_by_ram})")
        return workers
    
    def get_optimal_buffer_size(self):
        """Get optimal buffer size for file I/O operations."""
        # Base buffer size on available RAM
        if self.max_ram_bytes >= 16 * 1024 * 1024 * 1024:  # 16GB+
            buffer_size = 8 * 1024 * 1024  # 8MB
        elif self.max_ram_bytes >= 8 * 1024 * 1024 * 1024:  # 8GB+
            buffer_size = 4 * 1024 * 1024  # 4MB
        elif self.max_ram_bytes >= 4 * 1024 * 1024 * 1024:  # 4GB+
            buffer_size = 2 * 1024 * 1024  # 2MB
        else:
            buffer_size = 1024 * 1024  # 1MB
        
        return buffer_size
    
    def check_disk_space_requirements(self, input_size, output_path='.'):
        """
        Check if there's enough disk space for encryption.
        
        Args:
            input_size: Size of input data in bytes
            output_path: Path where output will be written
            
        Returns:
            bool: True if sufficient space, False otherwise
        """
        # Realistic disk space estimation for encryption based on actual usage patterns
        # Estimate final output size based on observed expansion ratios
        estimated_fastq_size = input_size * 3.2  # Realistic decompression + formatting
        estimated_csv_size = input_size * 1.7    # CSV metadata with read descriptions
        final_output_size = estimated_fastq_size + estimated_csv_size
        
        # Peak temporary space during chunk processing (before merging)
        temp_chunk_space = final_output_size * 0.8  # Chunks are processed in parallel
        
        # Total peak space needed (temp + final output, but not both at full size)
        peak_space_needed = temp_chunk_space + final_output_size
        
        # Add 10% safety buffer (more reasonable)
        total_space_needed = peak_space_needed * 1.1
        
        available_space = self.capabilities.get_available_disk_space(output_path)
        
        if available_space < total_space_needed:
            self.logger.error(f"Insufficient disk space!")
            self.logger.error(f"  Peak space needed: {self.capabilities.format_bytes(total_space_needed)}")
            self.logger.error(f"  Available space: {self.capabilities.format_bytes(available_space)}")
            self.logger.error(f"  Shortfall: {self.capabilities.format_bytes(total_space_needed - available_space)}")
            return False
        
        self.logger.info(f"Disk space check passed:")
        self.logger.info(f"  Peak space needed: {self.capabilities.format_bytes(total_space_needed)}")
        self.logger.info(f"  Available space: {self.capabilities.format_bytes(available_space)}")
        self.logger.info(f"  Safety margin: {self.capabilities.format_bytes(available_space - total_space_needed)}")
        return True
    
    def monitor_memory_usage(self):
        """Monitor current memory usage and return info."""
        memory_info = self.capabilities.get_memory_info()
        if memory_info['percent_used'] > 90:
            self.logger.warning(f"High memory usage: {memory_info['percent_used']:.1f}%")
        
        return memory_info

    def get_optimal_processing_params(self, total_reads, estimated_read_size=400):
        """
        Calculate optimal chunk size and worker count together to avoid memory issues.
        
        Args:
            total_reads: Total number of reads to process
            estimated_read_size: Estimated size per read in bytes
            
        Returns:
            tuple: (optimal_chunk_size, optimal_worker_count)
        """
        # Calculate memory per read with overhead
        memory_per_read = estimated_read_size * 3
        usable_ram = self.max_ram_bytes * 0.8
        
        # Start with CPU-based worker count
        max_workers_by_cpu = min(self.capabilities.cpu_count_logical, 16)
        
        # Calculate what chunk size we can afford with different worker counts
        best_chunk_size = 0
        best_worker_count = 1
        best_throughput = 0
        
        for workers in range(1, max_workers_by_cpu + 1):
            # Memory available per worker
            ram_per_worker = usable_ram / workers
            max_chunk_size = int(ram_per_worker / memory_per_read)
            
            # Apply reasonable limits
            max_chunk_size = min(max_chunk_size, 5000000)  # Max 5M reads per chunk
            max_chunk_size = max(max_chunk_size, 10000)    # Min 10k reads per chunk
            
            # Skip if chunk size is too small
            if max_chunk_size < 50000:  # Minimum viable chunk size
                continue
                
            # Estimate throughput (workers * chunk_size, but with diminishing returns)
            estimated_throughput = workers * max_chunk_size * (0.9 ** (workers - 1))
            
            if estimated_throughput > best_throughput:
                best_throughput = estimated_throughput
                best_chunk_size = max_chunk_size
                best_worker_count = workers
        
        # Ensure we have reasonable values
        if best_chunk_size == 0:
            best_chunk_size = min(100000, total_reads)
            best_worker_count = 1
        
        # Final validation - total memory usage should not exceed available RAM
        total_memory_needed = best_worker_count * best_chunk_size * memory_per_read
        if total_memory_needed > usable_ram:
            # Scale down chunk size to fit
            best_chunk_size = int(usable_ram / (best_worker_count * memory_per_read))
            best_chunk_size = max(10000, best_chunk_size)
        
        self.logger.info(f"Optimal processing parameters:")
        self.logger.info(f"  Chunk size: {best_chunk_size:,} reads")
        self.logger.info(f"  Worker count: {best_worker_count}")
        self.logger.info(f"  Total memory usage: {self.capabilities.format_bytes(total_memory_needed)}")
        self.logger.info(f"  Memory per worker: {self.capabilities.format_bytes(best_chunk_size * memory_per_read)}")
        
        return best_chunk_size, best_worker_count

