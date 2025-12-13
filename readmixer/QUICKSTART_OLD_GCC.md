# Quick Start Guide for Systems with Older GCC

If you encountered this error:
```
../meson.build:28:4: ERROR: Problem encountered: NumPy requires GCC >= 9.3
```

**You're in the right place!** This guide will get you up and running in just a few minutes.

## The 60-Second Solution

```bash
# Navigate to the readmixer directory
cd readmixer

# Create conda environment (installs numpy and contourpy)
conda env create -f environment.yml

# Activate the environment
conda activate readmixer-env

# Test that it works
python readmixer.py --help
```

**That's it!** You're ready to use ReadMixer.

## Why Does This Work?

- **The Problem:** NumPy 2.x and contourpy require GCC >= 9.3 to compile from source
- **Your System:** Likely has GCC 4.8.5 or another older version (common on cluster systems)
- **The Solution:** Conda provides pre-compiled binaries, so no compilation needed!

## First Time Using ReadMixer?

Scramble your first FASTQ files:

```bash
# Make sure environment is activated
conda activate readmixer-env

# Basic obfuscation
python readmixer.py -s -p /path/to/your/fastq/files

# With custom output directory
python readmixer.py -s -p /path/to/input -o /path/to/output

# See all options
python readmixer.py --help
```

## Alternative Installation Method

If `conda env create -f environment.yml` doesn't work for any reason, try this manual approach:

```bash
# Create environment with just the problematic packages
conda create -y -n readmixer-env python=3.11 numpy=2.3 contourpy=1.3

# Activate it
conda activate readmixer-env

# Install remaining packages with pip
pip install -r requirements-pip.txt

# Test it
python readmixer.py --help
```

## Don't Have Conda?

Install Miniconda (lightweight, recommended for clusters):

```bash
# Download Miniconda installer
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

# Run installer
bash Miniconda3-latest-Linux-x86_64.sh

# Follow prompts, then restart terminal or run:
source ~/.bashrc

# Now return to "The 60-Second Solution" above
```

## Troubleshooting

### "conda: command not found"
You need to install conda first (see "Don't Have Conda?" section above).

### "conda activate" doesn't work
Try this instead:
```bash
source $HOME/anaconda3/bin/activate readmixer-env
# OR
source $HOME/miniconda3/bin/activate readmixer-env
```

### Still having issues?
1. Check the comprehensive [INSTALLATION.md](INSTALLATION.md) guide
2. Review the troubleshooting section in [README.md](readme.md)

## What's Next?

- Read the [README.md](readme.md) for full documentation
- Review performance benchmarks and optimization tips
- Check command-line options: `python readmixer.py --help`

## Environment Activation Reminder

Every time you open a new terminal to use ReadMixer, you need to activate the environment:

```bash
conda activate readmixer-env
```

To check if your environment is active, look for `(readmixer-env)` at the start of your command prompt.

## Summary

✅ Use: `conda env create -f environment.yml`  
✅ Activate: `conda activate readmixer-env`  
✅ Run: `python readmixer.py -s -p /path/to/data`

**You're all set!** Happy scrambling!

