#!/usr/bin/env python3
"""
ReadMixer FASTQ Reconstruction Tool

This script reconstructs FASTQ files that were split-compressed using ReadMixer's
split compression feature (-s/--split-compress flag).

Usage:
    python readmixer_reconstruct.py -m manifest.txt -o output_directory

The script reads the manifest file created during split compression and reconstructs
the original FASTQ files by combining the separated components.
"""

import argparse
import gzip
import logging
import os
import shutil
import time


def parse_manifest(manifest_file):
    """Parse the ReadMixer split compression manifest file."""
    entries = []
    current_entry = {}
    
    with open(manifest_file, 'r') as f:
        for line in f:
            line = line.strip()
            
            # Skip comments and empty lines
            if line.startswith('#') or not line:
                continue
            
            if line == '[FASTQ_FILE]':
                # Start of a new entry
                if current_entry:
                    entries.append(current_entry)
                current_entry = {}
            elif '=' in line:
                key, value = line.split('=', 1)
                current_entry[key] = value
        
        # Add the last entry
        if current_entry:
            entries.append(current_entry)
    
    return entries


def reconstruct_fastq(manifest_entry, manifest_dir, output_dir, buffer_size=8192*1024):
    """
    Reconstruct a FASTQ file from its split compressed components.
    
    Args:
        manifest_entry: Dictionary with file information from manifest
        manifest_dir: Directory containing the manifest file (where component files are located)
        output_dir: Directory where reconstructed FASTQ should be written
        buffer_size: Buffer size for file I/O operations
    """
    original_name = manifest_entry['original_name']
    read_count = int(manifest_entry['read_count'])
    
    logging.info(f"Reconstructing: {original_name} ({read_count} reads)")
    
    # Get paths to compressed component files
    headers_gz = os.path.join(manifest_dir, manifest_entry['headers'])
    sequences_gz = os.path.join(manifest_dir, manifest_entry['sequences'])
    plus_gz = os.path.join(manifest_dir, manifest_entry['plus'])
    qualities_gz = os.path.join(manifest_dir, manifest_entry['qualities'])
    
    # Verify all component files exist
    for comp_file in [headers_gz, sequences_gz, plus_gz, qualities_gz]:
        if not os.path.exists(comp_file):
            raise FileNotFoundError(f"Component file not found: {comp_file}")
    
    # Create output FASTQ file
    output_file = os.path.join(output_dir, original_name)
    
    # Open all component files and reconstruct FASTQ
    reads_written = 0
    with gzip.open(headers_gz, 'rt', encoding='utf-8') as f_headers, \
         gzip.open(sequences_gz, 'rt', encoding='utf-8') as f_sequences, \
         gzip.open(plus_gz, 'rt', encoding='utf-8') as f_plus, \
         gzip.open(qualities_gz, 'rt', encoding='utf-8') as f_qualities, \
         open(output_file, 'w', buffering=buffer_size) as f_out:
        
        while True:
            header = f_headers.readline()
            if not header:
                break
            
            sequence = f_sequences.readline()
            plus = f_plus.readline()
            quality = f_qualities.readline()
            
            if not (sequence and plus and quality):
                logging.warning(f"Incomplete read at line {reads_written * 4}")
                break
            
            # Write the complete FASTQ read
            f_out.write(header)
            f_out.write(sequence)
            f_out.write(plus)
            f_out.write(quality)
            
            reads_written += 1
    
    if reads_written != read_count:
        logging.warning(f"Expected {read_count} reads but wrote {reads_written}")
    else:
        logging.info(f"Successfully reconstructed {reads_written} reads")
    
    return output_file, reads_written


def format_bytes(size):
    """Format bytes as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"


def main():
    """Main reconstruction function."""
    parser = argparse.ArgumentParser(
        description='Reconstruct FASTQ files from ReadMixer split-compressed components.'
    )
    parser.add_argument(
        "-m", "--manifest", 
        dest="manifest", 
        required=True,
        help="Path to the ReadMixer manifest file (ReadMixer.*.manifest.txt)"
    )
    parser.add_argument(
        "-o", "--output", 
        dest="output", 
        required=True,
        help="Output directory for reconstructed FASTQ files"
    )
    parser.add_argument(
        "-l", "--loglevel", 
        default="info", 
        choices=["info", "debug", "error", "warning", "critical"],
        dest="loglevel", 
        help="The logging level. Default is 'info'."
    )
    parser.add_argument(
        "-c", "--compress",
        dest="compress",
        action="store_true",
        help="Compress the reconstructed FASTQ files with gzip"
    )
    
    args = parser.parse_args()
    
    # Set up logging
    logging.basicConfig(
        level=getattr(logging, args.loglevel.upper()),
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    start_time = time.time()
    
    # Validate manifest file exists
    if not os.path.exists(args.manifest):
        logging.error(f"Manifest file not found: {args.manifest}")
        return 1
    
    # Create output directory if it doesn't exist
    try:
        os.makedirs(args.output, exist_ok=True)
        logging.info(f"Output directory: {args.output}")
    except Exception as e:
        logging.error(f"Failed to create output directory: {e}")
        return 1
    
    # Get the directory containing the manifest file
    manifest_dir = os.path.dirname(os.path.abspath(args.manifest))
    logging.info(f"Looking for component files in: {manifest_dir}")
    
    # Parse manifest
    try:
        entries = parse_manifest(args.manifest)
        logging.info(f"Found {len(entries)} FASTQ files to reconstruct")
    except Exception as e:
        logging.error(f"Failed to parse manifest file: {e}")
        return 1
    
    # Reconstruct each FASTQ file
    total_reads = 0
    reconstructed_files = []
    
    for entry in entries:
        try:
            output_file, read_count = reconstruct_fastq(
                entry, manifest_dir, args.output
            )
            total_reads += read_count
            reconstructed_files.append(output_file)
            
            # Get file size
            file_size = os.path.getsize(output_file)
            logging.info(f"  Output file size: {format_bytes(file_size)}")
            
        except Exception as e:
            logging.error(f"Failed to reconstruct {entry.get('original_name', 'unknown')}: {e}")
            import traceback
            logging.debug(traceback.format_exc())
            continue
    
    # Optionally compress the reconstructed files
    if args.compress:
        logging.info("Compressing reconstructed FASTQ files...")
        for fastq_file in reconstructed_files:
            try:
                gz_file = fastq_file + ".gz"
                with open(fastq_file, 'rb') as f_in:
                    with gzip.open(gz_file, 'wb', compresslevel=6) as f_out:
                        shutil.copyfileobj(f_in, f_out, 8192*1024)
                
                # Remove uncompressed file
                os.remove(fastq_file)
                
                original_size = os.path.getsize(fastq_file) if os.path.exists(fastq_file) else 0
                compressed_size = os.path.getsize(gz_file)
                
                logging.info(f"Compressed: {os.path.basename(gz_file)} "
                           f"({format_bytes(compressed_size)})")
            except Exception as e:
                logging.error(f"Failed to compress {fastq_file}: {e}")
    
    elapsed_time = time.time() - start_time
    logging.info(f"\nReconstruction complete!")
    logging.info(f"  Total files: {len(reconstructed_files)}")
    logging.info(f"  Total reads: {total_reads}")
    logging.info(f"  Time elapsed: {elapsed_time:.2f} seconds")
    
    return 0


if __name__ == "__main__":
    exit(main())

