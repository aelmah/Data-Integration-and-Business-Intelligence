<div align="center">

# Multilingual Bibliographic Data Integration for PMB

[![PHP Version](https://img.shields.io/badge/PHP-8.x%2B-8892BF.svg?logo=php)](https://www.php.net/)
[![PMB Compatibility](https://img.shields.io/badge/PMB-4.x%2B-green)](https://pmb.services/)

*A PHP solution for integrating French/Arabic library records into PMB*

</div>

---

## Overview
Transform bibliographic datasets from French (`buf.csv`) and Arabic (`bua.xls`) into PMB-compatible formats with:
- Standardized inventory IDs (`fsr_XXXXX`)
- Multilingual field mapping (French/Arabic)
- Structural compliance with PMB database schema

---

## Key Features
- **ETL Pipeline**: Automated data extraction, transformation, and loading
- **Encoding Handling**: Automatic UTF-8 conversion for Arabic text
- **Data Validation**:
  - Inventory ID format checks
  - Required field verification
- **Error Logging**: Detailed rejection tracking

---

## Requirements
- **PHP 8.x+** with extensions:
  - `pdo_mysql`
  - `mbstring`
- **MySQL** database (PMB-compatible)
- **PhpSpreadsheet** library ([installation guide](https://phpspreadsheet.readthedocs.io/))

---

## Installation
```bash
git clone https://github.com/aelmah/Data-integration.git
cd Data-integration
composer install
```
