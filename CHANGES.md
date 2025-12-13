# Installation Updates - Changes Summary

## Overview

This document summarizes the changes made to address installation issues on systems with older GCC versions (< 9.3), particularly on cluster environments with GCC 4.8.5.

## Problem Statement

Users reported installation failures when running `pip install -r requirements.txt` on systems with GCC < 9.3:
- NumPy 2.x requires GCC >= 9.3 to compile from source
- contourpy package has similar GCC requirements
- Error: "NumPy requires GCC >= 9.3"

## Solution Implemented

The solution uses conda to install pre-compiled binaries of numpy and contourpy, bypassing the GCC version requirement.

## Files Added

### 1. **environment.yml**
Location: `readmixer/environment.yml`

Conda environment specification file that includes:
- Python 3.11
- numpy 2.3 (pre-compiled via conda)
- contourpy 1.3 (pre-compiled via conda)
- All other dependencies via pip

**Usage:**
```bash
conda env create -f environment.yml
conda activate readmixer-env
```

### 2. **requirements-pip.txt**
Location: `readmixer/requirements-pip.txt`

Pip requirements file with numpy removed (for use with Method 2).

**Contents:** All original requirements except numpy (which is installed via conda).

### 3. **Dockerfile.conda**
Location: `readmixer/Dockerfile.conda`

Alternative Dockerfile using conda base image for maximum compatibility.

**Usage:**
```bash
docker build -f Dockerfile.conda -t readmixer-conda:latest .
docker run -v /path/to/data:/data readmixer-conda:latest -s -p /data
```

### 4. **INSTALLATION.md**
Location: `readmixer/INSTALLATION.md`

Comprehensive installation guide covering:
- 5 different installation methods
- Detailed troubleshooting section
- Environment-specific recommendations
- GCC version checking instructions
- Conda installation instructions
- Verification steps

### 5. **setup.sh**
Location: `readmixer/setup.sh`

Interactive installation helper script that:
- Detects available tools (conda, gcc)
- Checks GCC version
- Recommends appropriate installation method
- Can perform automatic installation
- Provides guidance for missing dependencies

**Usage:**
```bash
chmod +x setup.sh
./setup.sh
```

## Files Modified

### 1. **README.md**
Location: `README.md` (root)

**Changes:**
- Updated System Requirements section to mention GCC >= 9.3 requirement
- Reorganized Installation section with 4 methods:
  1. Conda with environment.yml (recommended)
  2. Conda with manual package installation
  3. pip (requires GCC >= 9.3)
  4. Docker (both standard and conda versions)
- Added explanation of why conda method is needed
- Added new "Installation Issues" section under Troubleshooting
- Added instructions for checking GCC version
- Added link to INSTALLATION.md

### 2. **readmixer/readme.md**
Location: `readmixer/readme.md`

**Changes:**
- Same updates as main README.md
- Updated to match the structure and content of root README.md

### 3. **Dockerfile**
Location: `readmixer/Dockerfile`

**Changes:**
- Added comments explaining GCC requirement
- Added reference to Dockerfile.conda alternative
- No functional changes to the build process

## Installation Methods

Users now have 5 installation methods to choose from:

### Method 1: Conda with environment.yml (Recommended)
```bash
conda env create -f environment.yml
conda activate readmixer-env
```
**Best for:** All users, especially cluster systems

### Method 2: Conda with Manual Installation
```bash
conda create -y -n readmixer-env python=3.11 numpy=2.3 contourpy=1.3
conda activate readmixer-env
pip install -r requirements-pip.txt
```
**Best for:** Users needing control over package versions

### Method 3: pip (Requires GCC >= 9.3)
```bash
python -m venv readmixer-env
source readmixer-env/bin/activate
pip install -r requirements.txt
```
**Best for:** Modern systems with GCC >= 9.3

### Method 4: Docker Standard
```bash
docker build -t readmixer:latest .
docker run -v /path/to/data:/data readmixer:latest -s -p /data
```
**Best for:** Docker users with compatible base images

### Method 5: Docker with Conda
```bash
docker build -f Dockerfile.conda -t readmixer-conda:latest .
docker run -v /path/to/data:/data readmixer-conda:latest -s -p /data
```
**Best for:** Docker users needing maximum compatibility

## Key Improvements

1. **Compatibility:** Works on systems with any GCC version
2. **User-Friendly:** Interactive setup script and comprehensive documentation
3. **Flexibility:** Multiple installation methods for different environments
4. **Troubleshooting:** Detailed error explanations and solutions
5. **Docker Support:** Both standard and conda-based Docker options
6. **Documentation:** Comprehensive INSTALLATION.md guide

## Testing Recommendations

To verify the changes work correctly:

1. **Test on system with GCC 4.8.5:**
   ```bash
   ./setup.sh
   # Should recommend conda method
   ```

2. **Test conda installation:**
   ```bash
   conda env create -f environment.yml
   conda activate readmixer-env
   python readmixer.py --help
   ```

3. **Test manual conda installation:**
   ```bash
   conda create -y -n readmixer-test python=3.11 numpy=2.3 contourpy=1.3
   conda activate readmixer-test
   pip install -r requirements-pip.txt
   python readmixer.py --help
   ```

4. **Test Docker conda build:**
   ```bash
   docker build -f Dockerfile.conda -t readmixer-conda:test .
   docker run readmixer-conda:test --help
   ```

## Migration Guide for Existing Users

Existing users who had working installations are not affected. However, if they want to adopt the new recommended method:

1. **Remove old environment:**
   ```bash
   conda deactivate
   conda env remove -n readmixer-env
   ```

2. **Create new environment:**
   ```bash
   conda env create -f environment.yml
   conda activate readmixer-env
   ```

## Backward Compatibility

All changes are backward compatible:
- Original `requirements.txt` still works on systems with GCC >= 9.3
- Original `Dockerfile` still works
- No changes to the main `readmixer.py` or processing code
- Existing installations continue to work

## References

- NumPy GCC requirements: https://numpy.org/devdocs/user/building.html
- Issue reported by user on cluster system with GCC 4.8.5
- Workaround provided: Pre-install numpy and contourpy via conda

---

**Date:** October 29, 2025  
**Affects:** Installation process only, no functional changes to ReadMixer

