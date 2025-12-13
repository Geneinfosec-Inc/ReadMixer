# ReadMixer Installation Guide

This guide provides detailed installation instructions for ReadMixer across different environments and system configurations.

## Quick Start

**For most users (recommended):**
```bash
cd readmixer
conda env create -f environment.yml
conda activate readmixer-env
```

## System Requirements

- **Python**: 3.11 (recommended), 3.10 or 3.7+ also supported
- **GCC**: >= 9.3 (for pip-only installation)
- **RAM**: Minimum 1GB (auto-adjusts for available resources)
- **Disk Space**: Sufficient for your dataset size (tool validates automatically)

## Installation Methods

### Method 1: Conda with environment.yml (Recommended) ⭐

**Best for:** All users, especially those on cluster systems or with older GCC versions

```bash
# Navigate to the readmixer directory
cd readmixer

# Create environment from environment.yml
conda env create -f environment.yml

# Activate the environment
conda activate readmixer-env
```

**Advantages:**
- Works on any system regardless of GCC version
- Installs all dependencies with compatible versions
- Most reliable installation method
- Simplest to use

---

### Method 2: Conda with Manual Package Installation

**Best for:** Users who need more control over package versions or are troubleshooting installation issues

```bash
# Create conda environment with numpy and contourpy pre-installed
conda create -y -n readmixer-env python=3.11 numpy=2.3 contourpy=1.3

# Activate the environment
conda activate readmixer-env

# Install remaining dependencies
cd readmixer
pip install -r requirements-pip.txt
```

**Why this approach?**
- NumPy and contourpy require GCC >= 9.3 to build from source
- Conda provides pre-compiled binaries, bypassing GCC requirements
- This is the workaround for older cluster systems (e.g., GCC 4.8.5)

---

### Method 3: pip with Virtual Environment

**Best for:** Systems with modern GCC (>= 9.3) where conda is not available

**Requirements:** GCC >= 9.3

```bash
# Check your GCC version first
gcc --version

# If GCC >= 9.3, proceed:
cd readmixer

# Create virtual environment
python3.11 -m venv readmixer-env

# Activate the environment
source readmixer-env/bin/activate  # On Linux/Mac
# OR
readmixer-env\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt
```

---

### Method 4: Docker (Standard)

**Best for:** Users familiar with Docker who want containerized deployment

```bash
cd readmixer

# Build the Docker image
docker build -t readmixer:latest .

# Run ReadMixer in a container
docker run -v /path/to/data:/data readmixer:latest -s -p /data
```

**Note:** The standard Dockerfile uses pip and requires the base image to have GCC >= 9.3 (which python:3.11-slim-buster does).

---

### Method 5: Docker with Conda

**Best for:** Docker users who want maximum compatibility

```bash
cd readmixer

# Build the Docker image using conda
docker build -f Dockerfile.conda -t readmixer-conda:latest .

# Run ReadMixer in a container
docker run -v /path/to/data:/data readmixer-conda:latest -s -p /data
```

**Advantages:**
- Works on any Docker-compatible system
- Uses conda for maximum compatibility
- Isolated environment

---

## Troubleshooting Installation

### Problem: "NumPy requires GCC >= 9.3" Error

**Error message:**
```
../meson.build:28:4: ERROR: Problem encountered: NumPy requires GCC >= 9.3
```

**Solution:**
Use conda to install numpy and contourpy (Method 1 or 2 above).

**Why this happens:**
- NumPy 2.x requires GCC >= 9.3 to compile from source
- Many cluster systems use older GCC versions (e.g., 4.8.5)
- Conda provides pre-compiled binaries that don't require compilation

---

### Problem: "contourpy" Build Error

**Solution:**
Same as above - use conda installation (Method 1 or 2).

**Why this happens:**
Contourpy has similar GCC requirements as NumPy.

---

### Problem: Checking Your GCC Version

```bash
gcc --version
```

**Example output:**
```
gcc (GCC) 4.8.5 20150623 (Red Hat 4.8.5-44)  # OLD - use conda
gcc (GCC) 9.3.0                               # OK - can use pip
gcc (GCC) 11.2.0                              # OK - can use pip
```

If your version is **less than 9.3**, use **Method 1 or 2** (conda-based).

---

### Problem: conda: command not found

**Solution:**
Install Anaconda or Miniconda first.

**Anaconda (full distribution, includes many packages):**
```bash
# Download and install from: https://www.anaconda.com/download
wget https://repo.anaconda.com/archive/Anaconda3-latest-Linux-x86_64.sh
bash Anaconda3-latest-Linux-x86_64.sh
```

**Miniconda (minimal installation, recommended for clusters):**
```bash
# Download and install from: https://docs.conda.io/en/latest/miniconda.html
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

After installation, restart your terminal or run:
```bash
source ~/.bashrc  # or ~/.bash_profile
```

---

### Problem: Conda environment activation fails

**If `conda activate` doesn't work, try:**
```bash
source $HOME/anaconda3/bin/activate readmixer-env
# OR
source $HOME/miniconda3/bin/activate readmixer-env
```

**Or initialize conda for your shell:**
```bash
conda init bash
# Then restart your terminal
```

---

## Verification

After installation, verify that ReadMixer is working:

```bash
# Activate your environment first
conda activate readmixer-env  # or source readmixer-env/bin/activate

# Check Python version
python --version

# Check NumPy installation
python -c "import numpy; print(f'NumPy version: {numpy.__version__}')"

# Check other dependencies
python -c "import Bio; import matplotlib; print('All dependencies OK')"

# Test ReadMixer
python readmixer.py --help
```

Expected output should show the help message with all available options.

---

## Environment-Specific Recommendations

### Older Cluster Systems (e.g., GCC 4.8.5)
**Recommended:** Method 1 or 2 (conda-based)

### Modern Linux Workstation (GCC 9.3+)
**Recommended:** Method 1 (conda) or Method 3 (pip)

### Windows
**Recommended:** Method 1 (conda) or WSL with conda

### macOS
**Recommended:** Method 1 (conda) or Method 3 (pip)

### HPC/Supercomputer Environments
**Recommended:** Method 1 or 2 (conda)
- Many HPC systems have conda modules available: `module load anaconda3`

### Docker/Container Environments
**Recommended:** Method 4 (standard Docker) or Method 5 (conda Docker)

---

## File Reference

After installation, you should have these key files:

- `readmixer.py` - Main application
- `environment.yml` - Conda environment specification (Method 1)
- `requirements-pip.txt` - Pip requirements without numpy/contourpy (Method 2)
- `requirements.txt` - Original pip requirements (Method 3)
- `Dockerfile` - Standard Docker configuration (Method 4)
- `Dockerfile.conda` - Conda-based Docker configuration (Method 5)

---

## Getting Help

If you encounter issues not covered here:

1. Check the main README.md for additional troubleshooting tips
2. Verify your system meets the requirements
3. Try the conda-based installation (Method 1) as it works on most systems
4. Check that all files were downloaded correctly

For support, please refer to the project's GitHub issues page or contact information in the main README.

---

## Next Steps

After successful installation:

1. Review the README.md for usage instructions
2. Try a basic obfuscation operation: `python readmixer.py -s -p /path/to/fastq/files`
3. Explore the command-line options: `python readmixer.py --help`
4. Check the performance benchmarks in the README for optimization tips

