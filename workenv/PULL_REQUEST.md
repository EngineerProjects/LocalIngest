feat(emissions): Add complete emissions pipeline with data generation

BREAKING CHANGE: Renamed MARKET.CONSTRUCTION to MARKET_CODE.MARKET

Added:
- Emissions data generator (rf_fr1_prm_dtl_midcorp_m) with 15 columns
- Emissions processor with business filters and segmentation
- Main notebook orchestrator (main.ipynb) for all test suites
- Tuple handling in BaseProcessor for multi-output processors

Fixed:
- Product code format in segmentation (01XXX instead of 0XXX)
- Config path issues using get_project_root() in emissions processor
- Emissions data generation with proper filters (26 excluded intermediaries)
- Segmentation join matching between LOB and SEGMENTPRDT tables

Refactored:
- MARKET → MARKET_CODE constant class
- CONSTRUCTION → MARKET attribute
- Updated imports in emissions_operations.py, emissions_processor.py, azec_capitaux_processor.py

Tests:
- Emissions pipeline now processes 20,000 records successfully
- All components (PTF_MVT, Capitaux, Emissions) validated

---
