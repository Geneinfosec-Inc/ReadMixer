# ReadMixer Archival and Preservation Strategy

## Overview

This document outlines the archival and long-term preservation strategy for the ReadMixer project. We are committed to ensuring that this software remains accessible, citable, and usable for the scientific community in perpetuity.

## Archival Objectives

1. **Long-term Preservation**: Ensure the source code is preserved for future access
2. **Citability**: Provide persistent identifiers (DOIs) for academic citations
3. **Reproducibility**: Enable researchers to access specific versions used in published work
4. **Discoverability**: Make the software findable through academic databases and search engines
5. **Transparency**: Maintain clear documentation of archival locations and identifiers

## Archival Locations

### 1. GitHub Repository (Primary Development)

**Location**: https://github.com/Geneinfosec-Inc/ReadMixer

**Purpose**: Primary development, collaboration, and version control

**Features**:
- Full git history with commit-level traceability
- Issue tracking and community engagement
- Release management with tagged versions
- Documentation and wiki
- CI/CD integration

**Limitations**: 
- Not designed for long-term archival
- Platform stability depends on GitHub's continued operation
- No persistent identifiers (DOIs) for citations

**Status**: Active

### 2. Zenodo (Long-term Archival with DOI)

**Location**: https://zenodo.org/account/settings/github/repository/Geneinfosec-Inc/ReadMixer

**Purpose**: Long-term archival with Digital Object Identifiers (DOIs) for each release

**Features**:
- Persistent DOI for each software version
- CERN-backed infrastructure for long-term preservation
- Integration with academic citation systems
- Automatic GitHub integration via webhooks
- EU-funded, committed to 20+ years of preservation
- Metadata preservation (authors, license, description)

**How to cite**: After archival, each release will have a unique DOI that can be cited in academic publications.

**Status**: Active

### 3. Software Heritage (Universal Source Code Archive)

**Location**: https://archive.softwareheritage.org/browse/origin/directory/?origin_url=https://github.com/Geneinfosec-Inc/ReadMixer

**Purpose**: Universal, permanent archive of all publicly available source code

**Features**:
- Non-profit organization dedicated to preserving software
- UNESCO-backed initiative
- Intrinsic persistent identifiers (SWHIDs)
- Captures full git history, not just releases
- Part of the global digital heritage
- Mirrors across multiple geographic locations

**Status**: Active

## Version Control and Release Strategy

### Version Numbering

ReadMixer uses Semantic Versioning (SemVer):
- **MAJOR.MINOR.PATCH** (e.g., 1.0.0)
- **MAJOR**: Incompatible API changes
- **MINOR**: Backward-compatible functionality additions
- **PATCH**: Backward-compatible bug fixes

### Release Process

1. **Development**: All work done on GitHub with proper branching
2. **Version Tag**: Create git tag for release (e.g., `v1.0.0`)
3. **GitHub Release**: Create GitHub release with changelog and assets
4. **Zenodo Archive**: Automatic trigger via GitHub-Zenodo integration
5. **Software Heritage**: Automatic crawling or manual submission
6. **Documentation Update**: Update CITATION.cff with new DOI

## Citation Guidance

### How to Cite ReadMixer

#### General Citation (Latest Version)
Users should cite ReadMixer using the information in the `CITATION.cff` file:

```
Sawaya, S., & Parks, J. (2025). ReadMixer - Dynamic FASTQ Obfuscation Tool 
(Version 1.0.0) [Computer software]. https://github.com/Geneinfosec-Inc/ReadMixer
```

#### Citing a Specific Version (With Zenodo DOI)
When a specific version is used in research, cite with the version-specific DOI:

```
Sawaya, S., & Parks, J. (2025). ReadMixer - Dynamic FASTQ Obfuscation Tool 
(Version 1.0.0) [Computer software]. https://doi.org/10.5281/zenodo.18157104
```

#### BibTeX Format
```bibtex
@software{sawaya_parks_2025_readmixer,
  author       = {Sawaya, Sterling and Parks, Joseph},
  title        = {ReadMixer - Dynamic FASTQ Obfuscation Tool},
  year         = 2025,
  publisher    = {GitHub},
  url          = {https://github.com/Geneinfosec-Inc/ReadMixer},
  version      = {1.0.0}
}
```

After Zenodo DOI is available:
```bibtex
@software{sawaya_parks_2025_readmixer,
  author       = {Sawaya, Sterling and Parks, Joseph},
  title        = {ReadMixer - Dynamic FASTQ Obfuscation Tool},
  year         = 2025,
  publisher    = {Zenodo},
  doi          = {10.5281/zenodo.18157104},
  url          = {https://github.com/Geneinfosec-Inc/ReadMixer},
  version      = {1.0.0}
}
```

### CITATION.cff File

The project includes a `CITATION.cff` (Citation File Format) file in the root directory. This machine-readable file:
- Provides standardized citation information
- Is recognized by GitHub and other platforms
- Automatically generates citation text
- Includes all metadata needed for proper attribution


#### Future Releases

Once set up, Zenodo automatically archives each GitHub release:
1. Create a new GitHub release with appropriate version tag
2. Zenodo automatically creates a new archive with a new DOI
3. Update CITATION.cff with the latest DOI
4. Each version gets its own citable DOI, plus a concept DOI for all versions


## Maintenance and Updates

### Regular Tasks

**For Each Release:**
1. Update version number in appropriate files
2. Update CHANGES.md with release notes
3. Create GitHub release with proper tag
4. Verify Zenodo archival occurred
5. Update CITATION.cff with new DOI
6. Check Software Heritage has latest version

**Annually:**
1. Verify all archival locations are accessible
2. Update contact information if changed
3. Review and update this strategy document

**When Infrastructure Changes:**
1. Document any new archival locations
2. Update all citation guidance
3. Notify users via README and release notes

## Metadata Preservation

Key metadata preserved across all archival locations:
- **Authors**: Sterling Sawaya, Joseph Parks
- **License**: Apache-2.0
- **Description**: Full abstract and feature list
- **Keywords**: bioinformatics, genomics, FASTQ, obfuscation, privacy
- **Dependencies**: All requirements documented
- **System Requirements**: Documented in README.md
- **Usage Examples**: Comprehensive documentation

## Disaster Recovery

In the unlikely event that primary platforms become unavailable:

1. **GitHub Unavailable**: 
   - Code available on Zenodo (all releases)
   - Code available on Software Heritage (full history)
   - Local git clones by developers and users

2. **Zenodo Unavailable**:
   - CERN has commitment to long-term preservation
   - Multiple geographic backups
   - Partnership agreements for data migration if needed

3. **Software Heritage Unavailable**:
   - Multiple mirror locations worldwide
   - Partnerships with national libraries
   - Data replication agreements

## Contact and Updates

**Maintainers**: 
- Sterling Sawaya (GeneInfoSec)
- Joseph Parks (joe@geneinfosec.com)

**Where to Report Issues**:
- GitHub Issues: https://github.com/Geneinfosec-Inc/ReadMixer/issues
- Email: joe@geneinfosec.com

**This Document**:
- Last Updated: January 5, 2025
- Version: 1.0
- Next Review: January 2026

---

*This archival strategy follows best practices from the Software Sustainability Institute, FORCE11, and the Research Data Alliance (RDA).*

