from Crypto.Random import random as crypto_random
import csv
import glob
import gzip
import logging
import os
import shutil
import time
import tempfile
import multiprocessing
import numpy as np
import process.util as util


def encrypt(loglevel="ERROR", workingpath=None, zipup=None, outputdir=None, 
            chunk_size=None, num_workers=None, max_ram_gb=None, split_compress=False):
    """
    Encrypt FASTQ files using a memory-efficient streaming approach with external sorting.
    Now generates separate key files for each input file.
    
    Args:
        loglevel: Logging level
        workingpath: Directory containing input files
        zipup: Whether to compress output files
        outputdir: Directory for output files
        chunk_size: Number of reads to process at once (None for auto-detection)
        num_workers: Number of worker processes to use (None for auto-detection)
        max_ram_gb: Maximum RAM to use in GB (None for auto-detection)
        split_compress: Whether to use split compression (separate FASTQ components before compression)
    """
    # Set up logger functionality
    logging.basicConfig()
    logger = logging.getLogger()
    logger.setLevel(loglevel)
    start = time.perf_counter()

    print(f"Logging at level {loglevel}")
    
    # Initialize resource manager for dynamic resource allocation
    resource_manager = util.ResourceManager(max_ram_gb=max_ram_gb, logger=logger)
    
    try:
        # Resolve current working directory
        current_directory = workingpath if workingpath is not None else os.getcwd()
    except Exception as e:
        logging.error("Fatal error with working directory; please check permissions. Terminating.")
        logging.error(f"{e}")
        return

    # Validate and create output directory if needed
    if outputdir is not None:
        try:
            os.makedirs(outputdir, exist_ok=True)
            logging.info(f"Output directory validated/created: {outputdir}")
        except Exception as e:
            logging.error(f"Fatal error creating output directory {outputdir}. Error: {e}")
            return

    # Validate input directory exists
    if not os.path.exists(current_directory):
        logging.error(f"Fatal error: Input directory {current_directory} does not exist. Terminating.")
        return

    # Create temp directory for intermediate files
    try:
        temp_dir = tempfile.mkdtemp(prefix="readmixer_", dir=outputdir if outputdir else None)
    except Exception as e:
        logging.error(f"Fatal error creating temporary directory. Error: {e}")
        return
    logging.info(f"Processing files in top-level directory {current_directory}")
    logging.info(f"Using temporary directory: {temp_dir}")
    if outputdir is not None:
        logging.info(f"Outputting data to {outputdir}")

    try:
        # Find all FASTQ files (streaming approach to avoid storing all paths)
        fastq_files = find_fastq_files(current_directory)
        
        # Count reads and create read chunks (streaming)
        total_reads, read_info, estimated_input_size = count_reads_streaming(fastq_files)
        
        if total_reads == 0:
            logger.info("Fatal error: No reads found in provided working directory. Terminating.")
            cleanup_temp_dir(temp_dir)
            return
        
        logging.info(f"Total number of reads: {total_reads}")
        logging.info(f"Number of input files: {len(read_info)}")
        logging.info(f"Estimated input size: {resource_manager.capabilities.format_bytes(estimated_input_size)}")
        
        # Check disk space requirements
        if not resource_manager.check_disk_space_requirements(estimated_input_size, outputdir or current_directory):
            cleanup_temp_dir(temp_dir)
            return
        
        # Determine optimal processing parameters
        if chunk_size is None or num_workers is None:
            # Use the new method that calculates both parameters together to avoid memory issues
            optimal_chunk_size, optimal_num_workers = resource_manager.get_optimal_processing_params(total_reads)
            
            if chunk_size is None:
                chunk_size = optimal_chunk_size
            else:
                logging.info(f"Using manual chunk size: {chunk_size}")
                
            if num_workers is None:
                num_workers = optimal_num_workers
            else:
                logging.info(f"Using manual worker count: {num_workers}")
        else:
            logging.info(f"Using manual chunk size: {chunk_size}")
            logging.info(f"Using manual worker count: {num_workers}")
            
        # Validate that the combination won't cause memory issues
        memory_per_read = 400 * 3  # Estimated memory per read with overhead
        total_memory_needed = num_workers * chunk_size * memory_per_read
        usable_ram = resource_manager.max_ram_bytes * 0.8
        
        if total_memory_needed > usable_ram:
            logging.warning(f"WARNING: Current parameters may cause memory issues!")
            logging.warning(f"  Total memory needed: {resource_manager.capabilities.format_bytes(total_memory_needed)}")
            logging.warning(f"  Available RAM: {resource_manager.capabilities.format_bytes(usable_ram)}")
            logging.warning(f"  Consider reducing chunk size or worker count")
        else:
            logging.info(f"Memory validation passed:")
            logging.info(f"  Total memory needed: {resource_manager.capabilities.format_bytes(total_memory_needed)}")
            logging.info(f"  Available RAM: {resource_manager.capabilities.format_bytes(usable_ram)}")
        
        # Generate file chunks and permutations
        chunk_count = (total_reads + chunk_size - 1) // chunk_size
        logging.info(f"Processing in {chunk_count} chunks of up to {chunk_size} reads each")
        
        # Create output file names - generate unique identifier for this encryption session
        out_file_name = util.random_alphanumeric_string(40)
        fastq_name = 'ReadMixer.' + out_file_name + ".fastq"
        gzfastq_name = 'ReadMixer.' + out_file_name + ".fastq.gz"

        if outputdir is not None:
            fastq_name = os.path.join(outputdir, fastq_name)
            gzfastq_name = os.path.join(outputdir, gzfastq_name)
        
        # Process reads in chunks with external sorting - now generates separate key files and multiple output FASTQ files
        output_files_created = process_reads_external_sort(fastq_files, read_info, total_reads, 
                                    chunk_size, temp_dir, fastq_name, out_file_name,
                                    outputdir, num_workers, resource_manager)
        
        # Determine if compression should be used
        should_compress = zipup
        # Only compress if explicitly requested with -z flag
        # Auto-compression removed - user must explicitly request compression
        
        # Compress if needed - now handles multiple FASTQ output files and multiple key files
        if should_compress:
            if split_compress:
                logging.info("Using split compression for better compression ratios")
                compress_output_files_split(output_files_created, out_file_name, 
                                          outputdir, read_info, resource_manager)
            else:
                compress_output_files_multiple_parts(output_files_created, out_file_name, 
                                                   outputdir, read_info, resource_manager)
        
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        import traceback
        logging.error(traceback.format_exc())
    finally:
        # Clean up temporary files
        cleanup_temp_dir(temp_dir)
    
    end = time.perf_counter()
    logging.info(f"Process completed in {end - start:0.4f} seconds")


def find_fastq_files(current_directory):
    """Find all FASTQ files in the given directory and its subdirectories."""
    # Get all subdirectories
    data_directories = glob.glob(os.path.join(current_directory, "*/"))
    
    # Yield files in the current directory
    for file in glob.glob(os.path.join(current_directory, "*.gz")):
        yield file
    for file in glob.glob(os.path.join(current_directory, "*.fastq")):
        yield file

    # Yield files in subdirectories
    for working_directory in data_directories:
        for file in glob.glob(os.path.join(working_directory, "*.gz")):
            yield file
        for file in glob.glob(os.path.join(working_directory, "*.fastq")):
            yield file


def count_reads_streaming(fastq_files):
    """Count reads in files using a streaming approach. Returns total count, file info, and estimated size."""
    total_reads = 0
    read_info = []  # [(file_path, file_name, read_count, start_index), ...]
    estimated_size = 0
    
    for fastq_file in fastq_files:
        file_name = os.path.basename(fastq_file)
        file_size = os.path.getsize(fastq_file)
        estimated_size += file_size
        
        if file_name.endswith(".gz"):
            line_count = util.gzip_file_line_count(fastq_file)
        else:
            line_count = util.file_line_count(fastq_file)
            
        if line_count % 4 != 0:
            logging.error(f"Fatal error: File {fastq_file} is not proper FASTQ format (line count not divisible by 4). Terminating.")
            raise ValueError(f"Invalid FASTQ format in {fastq_file}")
            
        read_count = line_count // 4
        if read_count > 0:
            read_info.append((fastq_file, file_name, read_count, total_reads))
            total_reads += read_count
            
    return total_reads, read_info, estimated_size


def process_chunk(args):
    """Process a single chunk of reads for parallelization - now generates separate key files per input file."""
    chunk_idx, start_idx, end_idx, chunk_size, temp_dir, file_info, out_file_name = args
    
    # Import required modules inside the function to ensure they're available in the subprocess
    import os
    import csv
    from Crypto.Random import random as crypto_random
    import numpy as np
    import process.util as util
    
    # Generate permutation for this chunk
    perm = np.arange(chunk_size)
    crypto_random.shuffle(perm.tolist())  # Use crypto-secure shuffle
    
    # Create temp files for this chunk
    chunk_fastq = os.path.join(temp_dir, f"chunk_{chunk_idx}.fastq")
    
    # Create separate temp key files for each original input file
    chunk_key_files = {}
    key_writers = {}
    
    # Initialize key files for each unique input file
    unique_files = set()
    for file_path, file_name, file_read_count, file_start_idx in file_info:
        unique_files.add((file_path, file_name))
    
    for file_path, file_name in unique_files:
        chunk_key_file = os.path.join(temp_dir, f"chunk_{chunk_idx}_{file_name}_key.csv")
        chunk_key_files[file_name] = chunk_key_file
        key_file = open(chunk_key_file, 'w', newline='')
        key_writer = csv.writer(key_file)
        # Updated header to include output_file_index column
        key_writer.writerow(["file_name", "read_description", "secure_index", "original_index", "tag", "output_file_index"])
        key_writers[file_name] = (key_file, key_writer)
    
    try:
        # Process reads in this chunk
        with open(chunk_fastq, 'w') as fastq_out:
            row_counts = {}  # Track row counts per file
            for file_name in unique_files:
                row_counts[file_name[1]] = 0
            
            # Read data for this chunk from input files
            reads_data = []  # Will store (file_name, read_desc, read_data, global_idx, tag) for each read
            current_read = 0
            
            # For each file that contains reads in this chunk range
            for file_path, file_name, file_read_count, file_start_idx in file_info:
                file_end_idx = file_start_idx + file_read_count
                
                # Check if this file has any reads in our chunk
                if file_end_idx <= start_idx or file_start_idx >= end_idx:
                    continue
                    
                # Calculate which reads to extract from this file
                file_read_start = max(0, start_idx - file_start_idx)
                file_read_end = min(file_read_count, end_idx - file_start_idx)
                
                # Extract those reads
                file_reader = open_fastq_file(file_path)
                
                # Skip to our starting position
                for _ in range(file_read_start):
                    next(file_reader)
                
                # Read the relevant portion of this file
                for read_idx in range(file_read_start, file_read_end):
                    read_data = next(file_reader)
                    global_idx = file_start_idx + read_idx
                    read_desc = extract_description(read_data[0])
                    tag = util.random_tag(7)  # Generate tag directly here
                    
                    reads_data.append((file_name, read_desc, read_data, global_idx, tag))
                    current_read += 1
            
            # Apply permutation to reads_data
            reads_data = [reads_data[perm[i]] for i in range(len(reads_data))]
            
            # Write permuted reads to output files
            row_count_global = 0  # Global row count for secure indices
            for file_name, read_desc, read_data, orig_idx, tag in reads_data:
                # Write to FASTQ file with new header
                new_header = f"@ReadMixerSecured {tag}\n"
                fastq_out.write(new_header)
                fastq_out.write(read_data[1])  # sequence
                fastq_out.write(read_data[2])  # plus
                fastq_out.write(read_data[3])  # quality
                
                # Write to appropriate key file based on original file name
                perm_idx = start_idx + row_count_global
                # Compress read description if it's very long to save space
                compressed_desc = read_desc if len(read_desc) <= 100 else read_desc[:50] + "..." + read_desc[-47:]
                
                _, key_writer = key_writers[file_name]
                # Add placeholder for output_file_index - will be updated during merge
                key_writer.writerow([file_name, compressed_desc, perm_idx, orig_idx, tag, 0])
                row_count_global += 1
    
    finally:
        # Close all key file handles
        for file_name in key_writers:
            key_file, _ = key_writers[file_name]
            key_file.close()
    
    return chunk_fastq, chunk_key_files


def open_fastq_file(file_path):
    """Open a FASTQ file and yield reads (header, sequence, plus, quality)."""
    import gzip
    
    if file_path.endswith(".gz"):
        handle = gzip.open(file_path, "rt", encoding="utf-8")
    else:
        handle = open(file_path, "rt", encoding="utf-8")
    
    try:
        while True:
            header = handle.readline()
            if not header:
                break
            sequence = handle.readline()
            plus = handle.readline()
            quality = handle.readline()
            
            if not (sequence and plus and quality):
                break
                
            yield (header, sequence, plus, quality)
    finally:
        handle.close()


def extract_description(header_line):
    """Extract read description from header line."""
    desc_line = header_line.rstrip('\n')
    if desc_line.startswith('@'):
        desc_line = desc_line[1:]
    return desc_line


def process_reads_external_sort(fastq_files, read_info, total_reads, chunk_size, 
                               temp_dir, output_fastq, out_file_name, outputdir, num_workers, resource_manager):
    """Process reads using external sorting for memory efficiency - now generates separate key files."""
    start_time = time.perf_counter()
    
    # Monitor memory usage before starting
    resource_manager.monitor_memory_usage()
    
    # Create chunk files
    chunk_files = []
    chunk_key_files_by_input = {}  # Dictionary to organize key files by input file
    
    # Initialize tracking for each input file
    for file_path, file_name, _, _ in read_info:
        chunk_key_files_by_input[file_name] = []
    
    # Process data in chunks to handle datasets much larger than available memory
    chunk_boundaries = [(i, min(i + chunk_size, total_reads)) 
                      for i in range(0, total_reads, chunk_size)]
    
    logging.info(f"Starting processing of {len(chunk_boundaries)} chunks with {num_workers} workers")
    
    # Set up worker pool for parallel processing
    with multiprocessing.Pool(num_workers) as pool:
        # Prepare arguments for each chunk
        chunk_args = []
        for i, (start_idx, end_idx) in enumerate(chunk_boundaries):
            chunk_size_i = end_idx - start_idx
            chunk_args.append((i, start_idx, end_idx, chunk_size_i, temp_dir, read_info, out_file_name))
        
        # Process chunks
        try:
            logging.info("Starting chunk processing...")
            results = pool.map(process_chunk, chunk_args)
            logging.info(f"All {len(results)} chunks processed successfully")
            
        except Exception as e:
            logging.error(f"Error during chunk processing: {e}")
            raise
            
        logging.info(f"Successfully processed {len(results)} chunks")
        
        # Collect results - organize by input file
        for chunk_fastq, chunk_key_files_dict in results:
            chunk_files.append(chunk_fastq)
            
            # Organize key files by input file
            for file_name, chunk_key_file in chunk_key_files_dict.items():
                chunk_key_files_by_input[file_name].append(chunk_key_file)
    
    # Monitor memory usage after processing
    resource_manager.monitor_memory_usage()
    
    # Merge chunk files into multiple output files with size limits
    output_file_base = output_fastq.rsplit('.', 1)[0]  # Remove .fastq extension
    output_files_created, read_to_file_mapping = merge_chunk_files_with_splitting(
        chunk_files, output_file_base, resource_manager, max_file_size_gb=5)
    
    # Update key files with correct output file indices and merge them
    update_key_files_with_output_mapping(chunk_key_files_by_input, read_to_file_mapping,
                                       out_file_name, outputdir, resource_manager)
    
    end_time = time.perf_counter()
    logging.info(f"Processed all reads in {end_time - start_time:0.4f} seconds")
    
    return output_files_created


def merge_chunk_files_with_splitting(chunk_files, output_file_base, resource_manager, max_file_size_gb=5):
    """
    Merge FASTQ chunk files into multiple output files, each no larger than max_file_size_gb.
    Returns a mapping of read indices to output file indices.
    """
    max_file_size_bytes = max_file_size_gb * 1024 * 1024 * 1024  # Convert to bytes
    
    logging.info(f"Merging {len(chunk_files)} chunk files into multiple output files (max {max_file_size_gb}GB each)...")
    buffer_size = resource_manager.get_optimal_buffer_size()
    
    # Track read indices to output file mapping
    read_to_file_mapping = {}  # secure_index -> output_file_index
    
    current_file_index = 0
    current_file_size = 0
    current_read_index = 0
    
    # Generate output file name for first file
    output_file = f"{output_file_base}.part{current_file_index:03d}.fastq"
    
    out_file = open(output_file, 'wb')
    output_files_created = []
    
    try:
        for chunk_file in chunk_files:
            with open(chunk_file, 'rb') as in_file:
                # Process the chunk file line by line to track reads
                in_file.seek(0)  # Reset to beginning
                
                # Read as text to count reads properly
                with open(chunk_file, 'r') as text_in_file:
                    lines_in_read = 0
                    current_read_data = []
                    
                    for line in text_in_file:
                        current_read_data.append(line)
                        lines_in_read += 1
                        
                        # FASTQ format has 4 lines per read
                        if lines_in_read == 4:
                            # We have a complete read
                            read_data_str = ''.join(current_read_data)
                            read_data_bytes = read_data_str.encode('utf-8')
                            
                            # Check if adding this read would exceed file size limit
                            if current_file_size + len(read_data_bytes) > max_file_size_bytes and current_file_size > 0:
                                # Close current file and start a new one
                                out_file.close()
                                output_files_created.append(output_file)
                                current_file_index += 1
                                output_file = f"{output_file_base}.part{current_file_index:03d}.fastq"
                                out_file = open(output_file, 'wb')
                                current_file_size = 0
                                logging.info(f"Started new output file: {os.path.basename(output_file)}")
                            
                            # Write the read to current file
                            out_file.write(read_data_bytes)
                            current_file_size += len(read_data_bytes)
                            
                            # Map this read to the current output file
                            read_to_file_mapping[current_read_index] = current_file_index
                            current_read_index += 1
                            
                            # Reset for next read
                            lines_in_read = 0
                            current_read_data = []
        
        # Close the last file
        out_file.close()
        output_files_created.append(output_file)
        
        logging.info(f"Created {len(output_files_created)} output files:")
        for i, file_path in enumerate(output_files_created):
            file_size = os.path.getsize(file_path)
            logging.info(f"  Part {i}: {os.path.basename(file_path)} ({resource_manager.capabilities.format_bytes(file_size)})")
    
    except Exception as e:
        # Clean up on error
        if out_file and not out_file.closed:
            out_file.close()
        raise e
    
    return output_files_created, read_to_file_mapping


def update_key_files_with_output_mapping(chunk_key_files_by_input, read_to_file_mapping, 
                                       out_file_name, outputdir, resource_manager):
    """
    Update key files with the correct output_file_index values and merge them.
    """
    logging.info("Updating key files with output file mapping...")
    
    for file_name in chunk_key_files_by_input:
        output_keyfile = f"ReadMixerKeys.{out_file_name}.{file_name}.csv"
        if outputdir:
            output_keyfile = os.path.join(outputdir, output_keyfile)
        
        # Merge key files while updating the output_file_index column
        with open(output_keyfile, 'w', newline='') as out_file:
            writer = None
            first_file = True
            
            for key_file in chunk_key_files_by_input[file_name]:
                with open(key_file, 'r', newline='') as in_file:
                    reader = csv.reader(in_file)
                    if first_file:
                        # Write header from first file
                        header = next(reader)
                        writer = csv.writer(out_file)
                        writer.writerow(header)
                        first_file = False
                    else:
                        # Skip header in subsequent files
                        next(reader)
                    
                    # Copy all rows, updating output_file_index
                    for row in reader:
                        if len(row) >= 6:  # Ensure we have all columns including output_file_index
                            secure_index = int(row[2])  # secure_index column
                            if secure_index in read_to_file_mapping:
                                row[5] = str(read_to_file_mapping[secure_index])  # Update output_file_index
                            writer.writerow(row)
        
        logging.info(f"Updated key file for {file_name}: {os.path.basename(output_keyfile)}")


def compress_output_files_multiple_parts(output_files_created, out_file_name, 
                                       outputdir, read_info, resource_manager):
    """Compress output files using gzip for multiple FASTQ files and multiple key files."""
    try:
        logging.info("Compressing output files...")
        buffer_size = resource_manager.get_optimal_buffer_size()
        
        # Compress each key file
        for file_path, file_name, _, _ in read_info:
            keyfile_name = f"ReadMixerKeys.{out_file_name}.{file_name}.csv"
            gzkeyfile_name = f"ReadMixerKeys.{out_file_name}.{file_name}.csv.gz"
            
            if outputdir:
                keyfile_name = os.path.join(outputdir, keyfile_name)
                gzkeyfile_name = os.path.join(outputdir, gzkeyfile_name)
            
            # Compress key file if it exists
            if os.path.exists(keyfile_name):
                with open(keyfile_name, "rb") as key_in, gzip.open(gzkeyfile_name, "wb") as key_out:
                    shutil.copyfileobj(key_in, key_out, buffer_size)
                
                # Remove uncompressed key file
                os.remove(keyfile_name)
                logging.info(f"Compressed key file: {os.path.basename(gzkeyfile_name)}")

        # Compress all FASTQ output files
        for output_file in output_files_created:
            compressed_file = output_file + ".gz"
            with open(output_file, "rb") as fastq_in, gzip.open(compressed_file, "wb") as fastq_out:
                shutil.copyfileobj(fastq_in, fastq_out, buffer_size)
            
            # Remove uncompressed FASTQ file
            if os.path.exists(output_file):
                os.remove(output_file)
            
            logging.info(f"Compressed FASTQ file: {os.path.basename(compressed_file)}")

        logging.info("Successfully compressed all output files.")
    except Exception as e:
        logging.error(f"Error compressing files: {e}")


def compress_output_files_split(output_files_created, out_file_name, 
                               outputdir, read_info, resource_manager):
    """
    Compress output files using split compression for better ratios.
    Separates FASTQ components (headers, sequences, quality scores) into separate files.
    """
    try:
        logging.info("Starting split compression (separating FASTQ components)...")
        buffer_size = resource_manager.get_optimal_buffer_size()
        
        # Create a manifest file to track the split files
        manifest_name = f"ReadMixer.{out_file_name}.manifest.txt"
        if outputdir:
            manifest_name = os.path.join(outputdir, manifest_name)
        
        manifest_entries = []
        
        # Process each FASTQ output file
        for output_file in output_files_created:
            base_name = os.path.basename(output_file)
            logging.info(f"Split-compressing: {base_name}")
            
            # Create component files
            headers_file = output_file + ".headers"
            sequences_file = output_file + ".sequences"
            plus_file = output_file + ".plus"
            qualities_file = output_file + ".qualities"
            
            # Split the FASTQ file into components
            read_count = split_fastq_into_components(output_file, headers_file, 
                                                    sequences_file, plus_file, 
                                                    qualities_file, buffer_size)
            
            # Compress each component
            component_files = [
                (headers_file, headers_file + ".gz"),
                (sequences_file, sequences_file + ".gz"),
                (plus_file, plus_file + ".gz"),
                (qualities_file, qualities_file + ".gz")
            ]
            
            compressed_sizes = {}
            for source_file, compressed_file in component_files:
                with open(source_file, "rb") as f_in:
                    with gzip.open(compressed_file, "wb", compresslevel=9) as f_out:
                        shutil.copyfileobj(f_in, f_out, buffer_size)
                
                # Get compressed size
                compressed_size = os.path.getsize(compressed_file)
                compressed_sizes[os.path.basename(compressed_file)] = compressed_size
                
                # Remove uncompressed component file
                os.remove(source_file)
                logging.info(f"  Compressed: {os.path.basename(compressed_file)} "
                           f"({resource_manager.capabilities.format_bytes(compressed_size)})")
            
            # Remove original FASTQ file
            if os.path.exists(output_file):
                original_size = os.path.getsize(output_file)
                os.remove(output_file)
                
                # Calculate total compressed size and ratio
                total_compressed = sum(compressed_sizes.values())
                ratio = (1 - total_compressed / original_size) * 100 if original_size > 0 else 0
                logging.info(f"  Original size: {resource_manager.capabilities.format_bytes(original_size)}")
                logging.info(f"  Compressed size: {resource_manager.capabilities.format_bytes(total_compressed)}")
                logging.info(f"  Compression ratio: {ratio:.1f}% reduction")
            
            # Add to manifest
            manifest_entries.append({
                'fastq_file': base_name,
                'read_count': read_count,
                'headers': base_name + ".headers.gz",
                'sequences': base_name + ".sequences.gz",
                'plus': base_name + ".plus.gz",
                'qualities': base_name + ".qualities.gz"
            })
        
        # Compress key files (same as before)
        for file_path, file_name, _, _ in read_info:
            keyfile_name = f"ReadMixerKeys.{out_file_name}.{file_name}.csv"
            gzkeyfile_name = f"ReadMixerKeys.{out_file_name}.{file_name}.csv.gz"
            
            if outputdir:
                keyfile_name = os.path.join(outputdir, keyfile_name)
                gzkeyfile_name = os.path.join(outputdir, gzkeyfile_name)
            
            if os.path.exists(keyfile_name):
                with open(keyfile_name, "rb") as key_in:
                    with gzip.open(gzkeyfile_name, "wb", compresslevel=9) as key_out:
                        shutil.copyfileobj(key_in, key_out, buffer_size)
                
                os.remove(keyfile_name)
                logging.info(f"Compressed key file: {os.path.basename(gzkeyfile_name)}")
        
        # Write manifest file
        with open(manifest_name, 'w') as manifest_file:
            manifest_file.write("# ReadMixer Split Compression Manifest\n")
            manifest_file.write("# This file describes how FASTQ files were split and compressed\n")
            manifest_file.write("# Use the readmixer_reconstruct.py script to reconstruct original FASTQ files\n\n")
            
            for entry in manifest_entries:
                manifest_file.write(f"[FASTQ_FILE]\n")
                manifest_file.write(f"original_name={entry['fastq_file']}\n")
                manifest_file.write(f"read_count={entry['read_count']}\n")
                manifest_file.write(f"headers={entry['headers']}\n")
                manifest_file.write(f"sequences={entry['sequences']}\n")
                manifest_file.write(f"plus={entry['plus']}\n")
                manifest_file.write(f"qualities={entry['qualities']}\n\n")
        
        logging.info(f"Created manifest file: {os.path.basename(manifest_name)}")
        logging.info("Split compression completed successfully.")
        
    except Exception as e:
        logging.error(f"Error during split compression: {e}")
        import traceback
        logging.error(traceback.format_exc())
        raise


def split_fastq_into_components(fastq_file, headers_file, sequences_file, 
                                plus_file, qualities_file, buffer_size):
    """
    Split a FASTQ file into its component parts.
    Returns the number of reads processed.
    """
    read_count = 0
    
    with open(fastq_file, 'r', buffering=buffer_size) as f_in, \
         open(headers_file, 'w', buffering=buffer_size) as f_headers, \
         open(sequences_file, 'w', buffering=buffer_size) as f_sequences, \
         open(plus_file, 'w', buffering=buffer_size) as f_plus, \
         open(qualities_file, 'w', buffering=buffer_size) as f_qualities:
        
        while True:
            header = f_in.readline()
            if not header:
                break
            sequence = f_in.readline()
            plus = f_in.readline()
            quality = f_in.readline()
            
            if not (sequence and plus and quality):
                break
            
            # Write each component to its respective file
            f_headers.write(header)
            f_sequences.write(sequence)
            f_plus.write(plus)
            f_qualities.write(quality)
            
            read_count += 1
    
    return read_count


def cleanup_temp_dir(temp_dir):
    """Clean up temporary directory and its contents."""
    try:
        if os.path.exists(temp_dir):
            # Force removal of large directories with many files
            import subprocess
            result = subprocess.run(['rm', '-rf', temp_dir], capture_output=True, text=True, timeout=300)
            if result.returncode == 0:
                logging.info(f"Temporary directory {temp_dir} removed")
            else:
                logging.warning(f"System rm command failed: {result.stderr}")
                # Fallback to Python's rmtree
                shutil.rmtree(temp_dir)
                logging.info(f"Temporary directory {temp_dir} removed (fallback)")
        else:
            logging.info(f"Temporary directory {temp_dir} already removed")
    except subprocess.TimeoutExpired:
        logging.error(f"Timeout cleaning up temporary directory {temp_dir} - continuing anyway")
    except Exception as e:
        logging.error(f"Error cleaning up temporary directory: {e} - continuing anyway")