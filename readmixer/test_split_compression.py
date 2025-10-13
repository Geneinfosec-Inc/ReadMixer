#!/usr/bin/env python3
"""
Test script for ReadMixer split compression feature.

This script creates sample FASTQ files, tests split compression,
and verifies reconstruction works correctly.
"""

import os
import sys
import shutil
import tempfile
import gzip
import subprocess


def create_sample_fastq(output_file, num_reads=1000):
    """Create a sample FASTQ file for testing."""
    print(f"Creating sample FASTQ file with {num_reads} reads...")
    
    bases = ['A', 'C', 'G', 'T']
    with open(output_file, 'w') as f:
        for i in range(num_reads):
            # Header
            f.write(f"@READ_{i:06d} Sample read for testing\n")
            # Sequence (100bp)
            seq = ''.join([bases[i % 4] for i in range(100)])
            f.write(f"{seq}\n")
            # Plus
            f.write("+\n")
            # Quality scores (all high quality 'I')
            f.write("I" * 100 + "\n")
    
    print(f"  Created: {output_file}")
    return output_file


def get_file_size(filepath):
    """Get file size in bytes."""
    if os.path.exists(filepath):
        return os.path.getsize(filepath)
    return 0


def format_bytes(size):
    """Format bytes as human-readable string."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} TB"


def test_split_compression():
    """Test the split compression feature."""
    
    # Create temporary directories
    test_dir = tempfile.mkdtemp(prefix="readmixer_test_")
    input_dir = os.path.join(test_dir, "input")
    output_dir = os.path.join(test_dir, "output")
    reconstruct_dir = os.path.join(test_dir, "reconstructed")
    
    os.makedirs(input_dir)
    os.makedirs(output_dir)
    os.makedirs(reconstruct_dir)
    
    print(f"\n{'='*70}")
    print("ReadMixer Split Compression Test")
    print(f"{'='*70}\n")
    print(f"Test directory: {test_dir}")
    
    try:
        # Step 1: Create sample FASTQ files
        print("\n[Step 1] Creating sample FASTQ files...")
        sample_file1 = create_sample_fastq(
            os.path.join(input_dir, "sample1.fastq"), 500
        )
        sample_file2 = create_sample_fastq(
            os.path.join(input_dir, "sample2.fastq"), 300
        )
        
        # Compress one of them to test .gz input support
        print("\nCompressing sample2.fastq...")
        with open(sample_file2, 'rb') as f_in:
            with gzip.open(sample_file2 + '.gz', 'wb') as f_out:
                f_out.write(f_in.read())
        os.remove(sample_file2)
        sample_file2 = sample_file2 + '.gz'
        print(f"  Created: {sample_file2}")
        
        input_size = get_file_size(sample_file1) + get_file_size(sample_file2)
        print(f"\nTotal input size: {format_bytes(input_size)}")
        
        # Step 2: Run ReadMixer with split compression
        print("\n[Step 2] Running ReadMixer with split compression...")
        readmixer_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            "readmixer.py"
        )
        
        cmd = [
            sys.executable,
            readmixer_script,
            "-e",
            "-p", input_dir,
            "-o", output_dir,
            "-z",
            "-s",
            "-l", "info"
        ]
        
        print(f"Command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"\nERROR: ReadMixer failed!")
            print(f"STDOUT:\n{result.stdout}")
            print(f"STDERR:\n{result.stderr}")
            return False
        
        print("\nReadMixer completed successfully!")
        
        # Step 3: Check output files
        print("\n[Step 3] Checking output files...")
        output_files = os.listdir(output_dir)
        
        # Look for component files
        headers_files = [f for f in output_files if '.headers.gz' in f]
        sequences_files = [f for f in output_files if '.sequences.gz' in f]
        qualities_files = [f for f in output_files if '.qualities.gz' in f]
        plus_files = [f for f in output_files if '.plus.gz' in f]
        manifest_files = [f for f in output_files if '.manifest.txt' in f]
        key_files = [f for f in output_files if 'ReadMixerKeys' in f]
        
        print(f"  Headers files: {len(headers_files)}")
        print(f"  Sequences files: {len(sequences_files)}")
        print(f"  Qualities files: {len(qualities_files)}")
        print(f"  Plus files: {len(plus_files)}")
        print(f"  Manifest files: {len(manifest_files)}")
        print(f"  Key files: {len(key_files)}")
        
        if not (headers_files and sequences_files and qualities_files and 
                plus_files and manifest_files):
            print("\nERROR: Missing expected output files!")
            return False
        
        # Calculate total output size
        total_output_size = sum(
            get_file_size(os.path.join(output_dir, f)) 
            for f in output_files
        )
        print(f"\nTotal output size: {format_bytes(total_output_size)}")
        
        if total_output_size > 0:
            compression_ratio = (1 - total_output_size / input_size) * 100
            print(f"Compression ratio: {compression_ratio:.1f}% reduction")
        
        # Step 4: Test reconstruction
        print("\n[Step 4] Testing reconstruction...")
        manifest_file = os.path.join(output_dir, manifest_files[0])
        reconstruct_script = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), 
            "readmixer_reconstruct.py"
        )
        
        cmd = [
            sys.executable,
            reconstruct_script,
            "-m", manifest_file,
            "-o", reconstruct_dir,
            "-l", "info"
        ]
        
        print(f"Command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"\nERROR: Reconstruction failed!")
            print(f"STDOUT:\n{result.stdout}")
            print(f"STDERR:\n{result.stderr}")
            return False
        
        print("\nReconstruction completed successfully!")
        
        # Step 5: Verify reconstructed files
        print("\n[Step 5] Verifying reconstructed files...")
        reconstructed_files = os.listdir(reconstruct_dir)
        fastq_files = [f for f in reconstructed_files if '.fastq' in f]
        
        print(f"  Reconstructed FASTQ files: {len(fastq_files)}")
        
        if not fastq_files:
            print("\nERROR: No reconstructed FASTQ files found!")
            return False
        
        # Count reads in reconstructed files
        total_reconstructed_reads = 0
        for fastq_file in fastq_files:
            filepath = os.path.join(reconstruct_dir, fastq_file)
            with open(filepath, 'r') as f:
                lines = f.readlines()
                reads = len(lines) // 4
                total_reconstructed_reads += reads
                print(f"  {fastq_file}: {reads} reads")
        
        expected_reads = 800  # 500 + 300
        if total_reconstructed_reads == expected_reads:
            print(f"\n✓ Read count matches! ({total_reconstructed_reads} reads)")
        else:
            print(f"\n✗ Read count mismatch!")
            print(f"  Expected: {expected_reads} reads")
            print(f"  Got: {total_reconstructed_reads} reads")
            return False
        
        # Success!
        print(f"\n{'='*70}")
        print("✓ All tests passed successfully!")
        print(f"{'='*70}\n")
        
        print("Summary:")
        print(f"  Input size: {format_bytes(input_size)}")
        print(f"  Output size: {format_bytes(total_output_size)}")
        print(f"  Compression: {compression_ratio:.1f}% reduction")
        print(f"  Reads processed: {total_reconstructed_reads}")
        print(f"  Files created: {len(output_files)}")
        
        return True
        
    except Exception as e:
        print(f"\n✗ Test failed with exception: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Cleanup
        print(f"\nCleaning up test directory: {test_dir}")
        try:
            shutil.rmtree(test_dir)
            print("✓ Cleanup complete")
        except Exception as e:
            print(f"✗ Cleanup failed: {e}")


if __name__ == "__main__":
    success = test_split_compression()
    sys.exit(0 if success else 1)

