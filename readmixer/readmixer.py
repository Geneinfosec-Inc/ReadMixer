import argparse
import process

# Copyright 2025 GeneInfoSec

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



if __name__ == "__main__":
    # set up command line arguments
    parser = argparse.ArgumentParser(description='Convert a set of fastq.gz files with sample indexes into secure fastq files that have unique indexes.')
    parser.add_argument("-l", "--loglevel", default="error", choices=["info", "debug", "error", "warning", "critical"], dest="loglevel", help="The logging level for the conversion. Default is 'Error'.")
    parser.add_argument("-p", "--pathname", dest="pathname", help="The data directory containing the subfolders to be processed (directory name should be surrounded by quotation marks). Default is the current directory.")
    parser.add_argument("-o", "--output", dest="output", help="Output file directory location.")
    parser.add_argument("-z", "-gz","-gzip","--zip", dest="zipup", action="store_true", help="Enable gzip compression of output files. By default, output files are NOT compressed.")
    parser.add_argument("-s", "--scramble", dest="scrambling", action="store_true", help="Scramble/obfuscate files in a given location.")
    parser.add_argument("-c", "--chunk_size", dest="chunk_size", help="The chunk size for processing (number of reads). If not specified, optimal size will be calculated based on available RAM.", type=int, default=None)
    parser.add_argument("-w", "--workers", dest="workers", help="The number of worker processes. If not specified, optimal count will be calculated based on CPU cores and available RAM.", type=int, default=None)
    parser.add_argument("-m", "--max-ram", dest="max_ram_gb", help="Maximum RAM to use in GB. If not specified, will automatically use available system RAM with safe margins.", type=float, default=None)
    args = parser.parse_args()
    
    if args.scrambling:
        # call the main routine
        print("Beginning scrambling and obfuscation process.")
        process.scramble(loglevel = args.loglevel.upper(), workingpath=args.pathname, zipup=args.zipup, outputdir=args.output, chunk_size=args.chunk_size, num_workers=args.workers, max_ram_gb=args.max_ram_gb)
    else:
        print("Please use the -s or --scramble flag to scramble files.")
        parser.print_help()

    print("Process Completed.")