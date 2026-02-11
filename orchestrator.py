"""
ISCA Data Pipeline Orchestrator
Runs the complete data extraction pipeline:
1. Scrapes the latest PDF from Illinois Treasurer website
2. Extracts data from PDF and maps to CSV format
"""

import os
import sys
import logging
from datetime import datetime
import subprocess

# ============================================================================
# DIRECTORY SETUP
# ============================================================================
os.makedirs('downloads', exist_ok=True)
os.makedirs('logs', exist_ok=True)
os.makedirs('output', exist_ok=True)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/orchestrator_{timestamp}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

def run_command(script_name, description):
    """Run a Python script and handle its output"""
    logging.info("=" * 80)
    logging.info(f"RUNNING: {description}")
    logging.info("=" * 80)

    try:
        # Run the script and capture output
        result = subprocess.run(
            [sys.executable, script_name],
            capture_output=True,
            text=True,
            encoding='utf-8'
        )

        # Print stdout
        if result.stdout:
            print(result.stdout)

        # Print stderr if there are errors
        if result.stderr:
            print(result.stderr, file=sys.stderr)

        # Check return code
        if result.returncode != 0:
            logging.error(f"✗ {description} failed with exit code {result.returncode}")
            return False

        logging.info(f"✓ {description} completed successfully")
        return True

    except Exception as e:
        logging.error(f"✗ Error running {description}: {e}")
        return False

def main():
    """Main orchestrator function"""

    try:
        logging.info("=" * 80)
        logging.info(f"ISCA DATA PIPELINE STARTED - {timestamp}")
        logging.info("=" * 80)

        # Step 1: Run scraper
        logging.info("\nStep 1: Running web scraper to download latest PDF...")
        if not run_command('scraper.py', 'Web Scraper'):
            logging.error("Pipeline failed at Step 1: Web Scraper")
            return 1

        # Step 2: Run PDF to CSV mapper
        logging.info("\nStep 2: Running PDF to CSV mapper...")
        if not run_command('pdf_to_csv_mapper.py', 'PDF to CSV Mapper'):
            logging.error("Pipeline failed at Step 2: PDF Mapper")
            return 1

        # Success
        logging.info("=" * 80)
        logging.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        logging.info("=" * 80)
        logging.info("\nSummary:")
        logging.info("  - Latest PDF downloaded to: downloads/")
        logging.info("  - Mapped CSV saved to: output/")
        logging.info(f"  - Full logs saved to: logs/orchestrator_{timestamp}.log")
        logging.info("=" * 80)

        return 0

    except Exception as e:
        logging.error(f"Pipeline error: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return 1

if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
