#!/bin/bash

# ReadMixer Installation Helper Script
# This script helps users choose and execute the appropriate installation method

set -e

echo "=============================================="
echo "ReadMixer Installation Helper"
echo "=============================================="
echo ""

# Check if conda is available
if command -v conda &> /dev/null; then
    CONDA_AVAILABLE=true
    echo "✓ Conda is available"
else
    CONDA_AVAILABLE=false
    echo "✗ Conda is not available"
fi

# Check GCC version if gcc is available
if command -v gcc &> /dev/null; then
    GCC_VERSION=$(gcc -dumpversion)
    GCC_MAJOR=$(echo $GCC_VERSION | cut -d. -f1)
    GCC_MINOR=$(echo $GCC_VERSION | cut -d. -f2)
    
    echo "✓ GCC version: $GCC_VERSION"
    
    # Check if GCC >= 9.3
    if [ "$GCC_MAJOR" -gt 9 ] || ([ "$GCC_MAJOR" -eq 9 ] && [ "$GCC_MINOR" -ge 3 ]); then
        GCC_OK=true
        echo "✓ GCC version is sufficient for pip installation"
    else
        GCC_OK=false
        echo "⚠ GCC version is too old for pip installation (requires >= 9.3)"
    fi
else
    GCC_OK=false
    echo "✗ GCC is not available"
fi

echo ""
echo "=============================================="
echo "Recommended Installation Method:"
echo "=============================================="

# Recommend installation method
if [ "$CONDA_AVAILABLE" = true ]; then
    echo ""
    echo "📦 Method 1: Conda with environment.yml (RECOMMENDED)"
    echo ""
    echo "This is the most reliable method that works on all systems."
    echo ""
    echo "Commands:"
    echo "  conda env create -f environment.yml"
    echo "  conda activate readmixer-env"
    echo ""
    
    read -p "Would you like to install using this method? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Installing ReadMixer using conda..."
        conda env create -f environment.yml
        echo ""
        echo "=============================================="
        echo "✓ Installation complete!"
        echo "=============================================="
        echo ""
        echo "To activate the environment, run:"
        echo "  conda activate readmixer-env"
        echo ""
        echo "Then test ReadMixer with:"
        echo "  python readmixer.py --help"
        echo ""
        exit 0
    fi
elif [ "$GCC_OK" = true ]; then
    echo ""
    echo "📦 Method 3: pip with virtual environment"
    echo ""
    echo "Your system has GCC >= 9.3, so pip installation should work."
    echo ""
    echo "Commands:"
    echo "  python3 -m venv readmixer-env"
    echo "  source readmixer-env/bin/activate"
    echo "  pip install -r requirements.txt"
    echo ""
    
    read -p "Would you like to install using this method? (y/n) " -n 1 -r
    echo ""
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo ""
        echo "Installing ReadMixer using pip..."
        python3 -m venv readmixer-env
        source readmixer-env/bin/activate
        pip install --upgrade pip
        pip install -r requirements.txt
        echo ""
        echo "=============================================="
        echo "✓ Installation complete!"
        echo "=============================================="
        echo ""
        echo "The virtual environment is now active."
        echo ""
        echo "Test ReadMixer with:"
        echo "  python readmixer.py --help"
        echo ""
        echo "To activate the environment in future sessions:"
        echo "  source readmixer-env/bin/activate"
        echo ""
        exit 0
    fi
else
    echo ""
    echo "⚠ WARNING: Your system requires conda for installation"
    echo ""
    echo "Your system has GCC < 9.3 (or GCC is not available)."
    echo "NumPy and contourpy require GCC >= 9.3 to build from source."
    echo ""
    echo "Please install Anaconda or Miniconda:"
    echo ""
    echo "Miniconda (recommended for clusters):"
    echo "  wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh"
    echo "  bash Miniconda3-latest-Linux-x86_64.sh"
    echo ""
    echo "Anaconda (full distribution):"
    echo "  wget https://repo.anaconda.com/archive/Anaconda3-latest-Linux-x86_64.sh"
    echo "  bash Anaconda3-latest-Linux-x86_64.sh"
    echo ""
    echo "After installing conda, run this script again or use:"
    echo "  conda env create -f environment.yml"
    echo "  conda activate readmixer-env"
    echo ""
    exit 1
fi

echo ""
echo "=============================================="
echo "Alternative Installation Methods:"
echo "=============================================="
echo ""
echo "For detailed installation instructions, see:"
echo "  - INSTALLATION.md (comprehensive guide)"
echo "  - README.md (quick start guide)"
echo ""
echo "Available methods:"
echo "  1. Conda with environment.yml (recommended)"
echo "  2. Conda with manual package installation"
echo "  3. pip with virtual environment (requires GCC >= 9.3)"
echo "  4. Docker (standard)"
echo "  5. Docker with conda"
echo ""

