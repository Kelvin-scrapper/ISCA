import os
import logging
from datetime import datetime
import sys
import traceback
import re
import pandas as pd
import pdfplumber

# ============================================================================
# CONFIGURATION - Auto-detect paths based on script location
# ============================================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOADS_FOLDER = os.path.join(SCRIPT_DIR, 'downloads')
OUTPUT_FOLDER = os.path.join(SCRIPT_DIR, 'output')
timestamp_for_file = datetime.now().strftime('%Y%m%d_%H%M%S')

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
        logging.FileHandler(f'logs/mapping_{timestamp}.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Set console output encoding to UTF-8
if sys.stdout.encoding != 'utf-8':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ============================================================================
# DATA MAPPING CONFIGURATION
# ============================================================================

# CSV Header Row 1 - Column Codes
CSV_HEADER_ROW1 = [
    '',  # First column is empty/date
    'ISCA.TA.M',
    'ISCA.TC.M',
    'ISCA.TW.M',
    'ISCA.MCA30.M',
    'ISCA.MWA.M',
    'ISCA.TFA.M',
    'ISCA.TPCA.M',
    'ISCA.AWW.M',
    'ISCA.MEA.M',
    'ISCA.MCA.M',
    'ISCA.AMC.M',
    'ISCA.ADR.M',
    'ISCA.AFAB.M',
    'ISCA.EOOR.M',
    'ISCA.TR.M',
    'ISCA.TAED.M',
    'ISCA.TSPD.M',
    'ISCA.TSPD90.M',
    'ISCA.TEE.M'
]

# CSV Header Row 2 - Column Descriptions
CSV_HEADER_ROW2 = [
    '',  # First column is empty/date
    'Total Assets',
    'Total Contributions',
    'Total Withdrawals',
    'Monthly Contributions Amount (Past 30 Days)',
    'Monthly Withdrawals Amount (Past 30 Days)',
    'Total Funded Accounts',
    'Total Payroll Contributing Accounts',
    'Accounts with a Withdrawal',
    'Multiple Employer Accounts',
    'Max Contribution Accounts',
    'Average Monthly Contribution Amount',
    'Average Deferral Rate (Funded Accounts)',
    'Average Funded Account Balance',
    'Effective Opt-Out Rate',
    'Total Registered',
    'Total Added Employee Data',
    'Total Submitting Payroll Deductions (Since Inception)',
    'Total Submitting Payroll Deductions (Last 90 Days)',
    'Total Exempted Employers'
]

# Column mapping with intelligent keyword matching
# Each CSV column maps to: (keywords to search for, section, is_percentage)
# Keywords are matched flexibly - the first keyword is primary, others are optional refinements
FIELD_MAPPING_SMART = {
    'ISCA.TA.M': (['Total Assets'], 'Program', False),
    'ISCA.TC.M': (['Total Contributions'], 'Program', False),
    'ISCA.TW.M': (['Total Withdrawals'], 'Program', False),
    'ISCA.MCA30.M': (['Monthly Contributions'], 'Program', False),
    'ISCA.MWA.M': (['Monthly Withdrawals'], 'Program', False),
    'ISCA.TFA.M': (['Funded Accounts'], 'Saver', False),
    'ISCA.TPCA.M': (['Payroll Contributing Accounts'], 'Saver', False),
    'ISCA.AWW.M': (['Accounts', 'Withdrawal'], 'Saver', False),
    'ISCA.MEA.M': (['Multiple', 'Employer'], 'Saver', False),
    'ISCA.MCA.M': (['Max Contribution'], 'Saver', False),
    'ISCA.AMC.M': (['Average Monthly Contribution'], 'Saver', False),
    'ISCA.ADR.M': (['Average', 'Rate'], 'Saver', True),
    'ISCA.AFAB.M': (['Average', 'Account Balance'], 'Saver', False),
    'ISCA.EOOR.M': (['Opt-Out Rate'], 'Saver', True),
    'ISCA.TR.M': (['Total Registered'], 'Employer', False),
    'ISCA.TAED.M': (['Added Employee Data'], 'Employer', False),
    'ISCA.TSPD.M': (['Payroll Deductions', 'Since Inception'], 'Employer', False),
    'ISCA.TSPD90.M': (['Payroll Deductions', 'Last 90 Days'], 'Employer', False),
    'ISCA.TEE.M': (['Exempted'], 'Employer', False),
}

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def find_latest_pdf(folder_path):
    """Find the most recent PDF file in the downloads folder"""
    logging.info(f"Searching for PDF files in: {folder_path}")

    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"Downloads folder not found: {folder_path}")

    # Get all PDF files
    pdf_files = [f for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]

    if not pdf_files:
        raise FileNotFoundError(f"No PDF files found in: {folder_path}")

    # Get full paths and sort by modification time (most recent first)
    pdf_paths = [os.path.join(folder_path, f) for f in pdf_files]
    pdf_paths.sort(key=os.path.getmtime, reverse=True)

    latest_pdf = pdf_paths[0]
    logging.info(f"Found {len(pdf_files)} PDF file(s). Using latest: {os.path.basename(latest_pdf)}")

    return latest_pdf

def clean_and_format_value(value, is_percentage=False):
    """
    Clean and format values to match CSV format requirements.
    Preserves commas for large numbers, handles negatives, and formats consistently.
    """
    if not value or value == 'NA':
        return ''

    original_value = str(value).strip()

    # Remove $ sign
    cleaned = original_value.replace('$', '').strip()

    # Handle parentheses as negative
    is_negative = False
    if cleaned.startswith('(') and cleaned.endswith(')'):
        is_negative = True
        cleaned = cleaned[1:-1]

    # Remove commas temporarily for parsing
    cleaned_no_commas = cleaned.replace(',', '')

    # Handle percentage
    if '%' in cleaned_no_commas:
        cleaned_no_commas = cleaned_no_commas.replace('%', '').strip()

    try:
        # Parse as float
        num_value = float(cleaned_no_commas)

        # Apply negative if needed
        if is_negative:
            num_value = -num_value

        # Format based on type
        if is_percentage:
            # Percentages: no commas, keep decimals
            return str(num_value)
        elif '.' in cleaned_no_commas:
            # Has decimal - preserve it
            # Add commas for thousands
            int_part, dec_part = str(abs(num_value)).split('.')
            formatted_int = f"{int(int_part):,}"
            result = f"{formatted_int}.{dec_part}"
            if num_value < 0:
                result = f"-{result}"
            return result
        else:
            # Integer value
            result = f"{int(num_value):,}"
            return result

    except:
        # If parsing fails, return original
        return original_value

def extract_month_year_from_pdf(pdf_path):
    """
    Smart extraction of month and year from PDF.
    Tries multiple patterns and sources to find the date.
    """
    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]
        text = first_page.extract_text()

        # Pattern 1: "Monthly Dashboard – Month Year" (e.g., "Monthly Dashboard – January 2026")
        match = re.search(r'Monthly Dashboard\s*[–-]\s*([A-Za-z]+)\s+(\d{4})', text, re.IGNORECASE)
        if match:
            month_name = match.group(1)
            year = match.group(2)
            try:
                month_num = datetime.strptime(month_name, '%B').month
                logging.info(f"Extracted date from dashboard title: {month_name} {year}")
                return f"{year}-{month_num:02d}"
            except:
                pass

        # Pattern 2: "Data as of Month Day, Year" (e.g., "Data as of January 31, 2026")
        match = re.search(r'Data as of ([A-Za-z]+)\s+\d+,\s+(\d{4})', text, re.IGNORECASE)
        if match:
            month_name = match.group(1)
            year = match.group(2)
            try:
                month_num = datetime.strptime(month_name, '%B').month
                logging.info(f"Extracted date from 'Data as of': {month_name} {year}")
                return f"{year}-{month_num:02d}"
            except:
                pass

        # Pattern 3: PDF filename (e.g., "SecureChoice_Monthly_January_2026_...")
        filename = os.path.basename(pdf_path)
        match = re.search(r'([A-Za-z]+)[_\s]+(\d{4})', filename)
        if match:
            month_name = match.group(1)
            year = match.group(2)
            try:
                month_num = datetime.strptime(month_name, '%B').month
                logging.info(f"Extracted date from filename: {month_name} {year}")
                return f"{year}-{month_num:02d}"
            except:
                pass

        # Pattern 4: Just look for any "Month Year" pattern in text
        match = re.search(r'\b([A-Za-z]{3,})\s+(\d{4})\b', text)
        if match:
            month_name = match.group(1)
            year = match.group(2)
            # Verify it's a valid month name
            month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                          'July', 'August', 'September', 'October', 'November', 'December']
            if any(month_name.lower() == m.lower() for m in month_names):
                try:
                    month_num = datetime.strptime(month_name, '%B').month
                    logging.info(f"Extracted date from general pattern: {month_name} {year}")
                    return f"{year}-{month_num:02d}"
                except:
                    pass

        # Pattern 5: Fallback - use current month/year
        logging.warning("Could not extract date from PDF, using current date")
        now = datetime.now()
        return f"{now.year}-{now.month:02d}"

    return None

def _looks_like_value(text):
    """Check if text looks like a numeric value (currency, number, percentage)"""
    if not text:
        return False

    text = str(text).strip()

    # Check for currency, numbers, or percentages
    if text.startswith('$') or text.startswith('(') or text.endswith('%'):
        return True

    # Check if it's a number (with or without commas)
    text_clean = text.replace(',', '').replace('$', '').replace('(', '').replace(')', '')
    try:
        float(text_clean)
        return True
    except:
        return False

def extract_data_from_pdf(pdf_path):
    """Extract all relevant data from the PDF"""
    logging.info(f"Extracting data from PDF: {pdf_path}")

    data = {}

    with pdfplumber.open(pdf_path) as pdf:
        first_page = pdf.pages[0]

        # Try table extraction first
        tables = first_page.extract_tables()

        if tables:
            logging.info(f"Found {len(tables)} tables in PDF")

            # Process each table
            for table_idx, table in enumerate(tables):
                logging.info(f"Processing table {table_idx + 1}")

                # Determine section by looking at headers
                current_section = None

                for row in table:
                    if not row:
                        continue

                    # Check if this row defines a section
                    first_cell = str(row[0]).strip() if row[0] else ""

                    if 'Program' in first_cell:
                        current_section = 'Program'
                        logging.info("Found Program section in table")
                        continue
                    elif 'Saver' in first_cell:
                        current_section = 'Saver'
                        logging.info("Found Saver section in table")
                        continue
                    elif 'Employer' in first_cell:
                        current_section = 'Employer'
                        logging.info("Found Employer section in table")
                        continue

                    # Extract data rows
                    if current_section and row[0]:
                        field_name = str(row[0]).replace('•', '').strip()

                        # Skip header rows
                        if field_name in ['Current', 'December 2025', 'Change', '']:
                            continue

                        # Handle multi-line field names (e.g., field name split across cells)
                        # Check if the next cell contains additional text (not a number)
                        if len(row) > 1 and row[1] and not _looks_like_value(row[1]):
                            # This might be continuation of field name
                            additional_text = str(row[1]).strip()
                            if additional_text and additional_text not in ['Current', 'December 2025', 'Change']:
                                field_name = f"{field_name} {additional_text}"

                        # Get the current value (check multiple columns)
                        current_value = None
                        for col_idx in range(1, len(row)):
                            if row[col_idx]:
                                cell_value = str(row[col_idx]).strip()
                                if _looks_like_value(cell_value) and cell_value not in ['Current', 'December 2025', 'Change']:
                                    current_value = cell_value
                                    break

                        if current_value:
                            data[f"{current_section}:{field_name}"] = current_value
                            logging.debug(f"Extracted {current_section}:{field_name} = {current_value}")

        # Fallback: text extraction if table extraction didn't work
        if len(data) == 0:
            logging.warning("No data from tables, trying text extraction...")
            text = first_page.extract_text()
            lines = text.split('\n')

            current_section = None
            i = 0
            while i < len(lines):
                line = lines[i].strip()

                # Identify sections
                if 'Program' in line and 'Current' in line:
                    current_section = 'Program'
                    logging.info("Found Program section")
                    i += 1
                    continue
                elif 'Saver' in line and 'Current' in line:
                    current_section = 'Saver'
                    logging.info("Found Saver section")
                    i += 1
                    continue
                elif 'Employer' in line and 'Current' in line:
                    current_section = 'Employer'
                    logging.info("Found Employer section")
                    i += 1
                    continue

                # Extract data lines (they have bullet points • or �)
                if current_section and ('•' in line or '�' in line):
                    # Remove bullet and split
                    line = line.replace('•', '').replace('�', '').strip()
                    parts = line.split()

                    # Find field name and value
                    field_name_parts = []
                    values = []

                    for part in parts:
                        # Check if it's a value (starts with $ or ( or looks like a number)
                        if part.startswith('$') or part.startswith('(') or re.match(r'^[\d,]+\.?\d*%?$', part) or part.endswith('%'):
                            values.append(part)
                        else:
                            field_name_parts.append(part)

                    field_name = ' '.join(field_name_parts)

                    # Check if we have a value on this line
                    if values:
                        current_value = values[0]
                        data[f"{current_section}:{field_name}"] = current_value
                        logging.debug(f"Extracted {current_section}:{field_name} = {current_value}")
                    else:
                        # No value on this line - check next lines for value and parenthetical text
                        # Pattern: Line 1 has field name, Line 2 has values, Line 3 has (clarification)
                        if i + 2 < len(lines):
                            next_line_1 = lines[i + 1].strip()
                            next_line_2 = lines[i + 2].strip()

                            # Check if next line has values
                            next_parts_1 = next_line_1.split()
                            next_values = [p for p in next_parts_1 if re.match(r'^[\d,]+\.?\d*%?$', p) or p.startswith('$') or p.startswith('(')]

                            # Check if line after that has parenthetical text
                            if next_line_2.startswith('(') and next_line_2.endswith(')'):
                                # Multi-line field: field name + parenthetical + value
                                clarification = next_line_2.strip()
                                full_field_name = f"{field_name} {clarification}"

                                if next_values:
                                    current_value = next_values[0]
                                    data[f"{current_section}:{full_field_name}"] = current_value
                                    logging.debug(f"Extracted multi-line {current_section}:{full_field_name} = {current_value}")

                i += 1

    logging.info(f"Extracted {len(data)} data points from PDF")
    return data

def find_matching_field(keywords, section, pdf_data):
    """
    Intelligently find a matching field in PDF data using keywords.
    Context-aware matching that handles field variations.
    Returns the value if found, empty string otherwise.
    """
    # Strategy 1: Exact match - ALL keywords present
    for key in pdf_data.keys():
        if not key.startswith(f"{section}:"):
            continue

        field_name = key.split(':', 1)[1].lower()

        # Check if ALL keywords are present in the field name
        if all(keyword.lower() in field_name for keyword in keywords):
            return pdf_data[key]

    # Strategy 2: Primary keyword match with optional secondary keywords
    # This handles cases where PDF has "Total Monthly Contributions" and we search for "Monthly Contributions"
    primary_keyword = keywords[0].lower()
    best_match = None
    best_match_score = 0

    for key in pdf_data.keys():
        if not key.startswith(f"{section}:"):
            continue

        field_name = key.split(':', 1)[1].lower()

        # Must contain primary keyword
        if primary_keyword not in field_name:
            continue

        # Score based on how many keywords match
        match_score = 1  # Primary keyword matched

        if len(keywords) > 1:
            # Count additional keyword matches
            for kw in keywords[1:]:
                if kw.lower() in field_name:
                    match_score += 1

        # Prefer matches with higher scores
        if match_score > best_match_score:
            best_match = pdf_data[key]
            best_match_score = match_score

    if best_match:
        return best_match

    # Strategy 3: Word boundary matching (for single keywords that might be part of compound words)
    # This helps avoid false matches like "Contributions" matching "Contributing"
    if len(keywords) == 1:
        keyword_lower = keywords[0].lower()
        for key in pdf_data.keys():
            if not key.startswith(f"{section}:"):
                continue

            field_name = key.split(':', 1)[1].lower()
            # Check if keyword appears as a whole word (with word boundaries)
            words_in_field = field_name.split()
            for word in words_in_field:
                if keyword_lower in word:  # Partial word match
                    return pdf_data[key]

    return ''

def map_pdf_to_csv_row(pdf_data, date_str):
    """Map extracted PDF data to CSV row format using intelligent matching"""
    logging.info(f"Mapping PDF data to CSV format for date: {date_str}")

    row = {'': date_str}  # First column is the date

    for csv_col, (keywords, section, is_percentage) in FIELD_MAPPING_SMART.items():
        value = find_matching_field(keywords, section, pdf_data)

        if value:
            # Format the value appropriately
            formatted_value = clean_and_format_value(value, is_percentage)
            row[csv_col] = formatted_value
            logging.debug(f"Mapped {csv_col} = {formatted_value} (from keywords: {keywords})")
        else:
            row[csv_col] = ''
            logging.warning(f"No match found for {csv_col} (keywords: {keywords}, section: {section})")

    return row

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function with comprehensive error handling"""

    try:
        logging.info("="*60)
        logging.info(f"STARTING PDF TO CSV MAPPING - {timestamp}")
        logging.info("="*60)

        # Step 1: Find latest PDF in downloads folder
        logging.info("Step 1: Finding latest PDF in downloads folder...")
        pdf_path = find_latest_pdf(DOWNLOADS_FOLDER)
        logging.info(f"✓ Using PDF: {os.path.basename(pdf_path)}")

        # Step 2: Extract month/year from PDF
        logging.info("Step 2: Extracting month/year from PDF...")
        date_str = extract_month_year_from_pdf(pdf_path)
        if not date_str:
            raise ValueError("Could not extract month/year from PDF")
        logging.info(f"✓ Extracted date: {date_str}")

        # Step 3: Extract data from PDF
        logging.info("Step 3: Extracting data from PDF...")
        pdf_data = extract_data_from_pdf(pdf_path)
        logging.info(f"✓ Extracted {len(pdf_data)} data points")

        # Step 4: Map to CSV row
        logging.info("Step 4: Mapping data to CSV format...")
        new_row = map_pdf_to_csv_row(pdf_data, date_str)
        logging.info(f"✓ Created new row with {len(new_row)} columns")

        # Step 5: Find existing CSV or create new one
        csv_files = [f for f in os.listdir(SCRIPT_DIR) if f.endswith('.csv') and 'ISCA_DATA' in f and 'UPDATED' not in f]

        if csv_files:
            csv_path = os.path.join(SCRIPT_DIR, csv_files[0])
            logging.info(f"Step 5: Loading existing CSV: {csv_files[0]}...")
            df = pd.read_csv(csv_path)
            logging.info(f"✓ Loaded CSV with {len(df)} rows and {len(df.columns)} columns")
        else:
            logging.info("Step 5: No existing CSV found. Creating new CSV with headers...")
            # Create DataFrame with hardcoded headers
            df = pd.DataFrame(columns=CSV_HEADER_ROW1)
            # Insert header row 2 as first data row
            df.loc[0] = CSV_HEADER_ROW2
            logging.info(f"✓ Created new CSV with headers")

        # Step 6: Check if date already exists (skip header row)
        data_rows = df.iloc[1:] if len(df) > 1 and df.iloc[0, 0] == '' else df

        if date_str in data_rows.iloc[:, 0].values:
            logging.warning(f"⚠ Date {date_str} already exists in CSV. Updating existing row...")
            row_index = df[df.iloc[:, 0] == date_str].index[0]
            for col, value in new_row.items():
                if col in df.columns:
                    df.at[row_index, col] = value
        else:
            logging.info(f"✓ Adding new row for {date_str}")
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

        # Step 7: Save updated CSV to output folder
        logging.info("Step 7: Saving updated CSV to output folder...")
        output_csv_path = os.path.join(OUTPUT_FOLDER, f'ISCA_DATA_{timestamp_for_file}.csv')
        df.to_csv(output_csv_path, index=False)
        logging.info(f"✓ Saved updated CSV to: output/{os.path.basename(output_csv_path)}")

        # Step 8: Display summary
        logging.info("="*60)
        logging.info("DATA MAPPING SUMMARY")
        logging.info("="*60)
        logging.info(f"PDF File: {os.path.basename(pdf_path)}")
        logging.info(f"Date: {date_str}")
        logging.info(f"Total rows in updated CSV: {len(df)}")
        logging.info("\nExtracted Values:")
        for key, value in sorted(new_row.items()):
            if key and value:
                logging.info(f"  {key}: {value}")

        logging.info("="*60)
        logging.info("✓ SCRIPT COMPLETED SUCCESSFULLY")
        logging.info("="*60)
        return 0

    except Exception as e:
        logging.error("="*60)
        logging.error("✗ ERROR OCCURRED")
        logging.error(f"✗ Error Type: {type(e).__name__}")
        logging.error(f"✗ Error Message: {str(e)}")
        logging.error("="*60)
        logging.error("Full Traceback:")
        logging.error(traceback.format_exc())
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
