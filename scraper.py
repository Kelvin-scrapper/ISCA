import os
import logging
from datetime import datetime
import sys
import traceback
import time
import requests
from urllib.parse import urljoin

# ============================================================================
# CONFIGURATION
# ============================================================================
HEADLESS = True  # Set to True to run browser in headless mode
BROWSER_TYPE = "playwright"  # Options: "selenium" or "playwright"

# ============================================================================
# DIRECTORY SETUP
# ============================================================================
os.makedirs('downloads', exist_ok=True)
os.makedirs('logs', exist_ok=True)

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'logs/{timestamp}.log'),
        logging.StreamHandler()
    ]
)

# ============================================================================
# ASSERTION FUNCTIONS
# ============================================================================

def assert_with_log(condition, message):
    """Assert with logging - helps track exactly where failure occurred"""
    if not condition:
        logging.error(f"ASSERTION FAILED: {message}")
        raise AssertionError(message)
    logging.debug(f"Assertion passed: {message}")

def assert_element_exists(element, element_name, context=""):
    """Assert that a web element was found"""
    context_msg = f" in {context}" if context else ""
    if element is None:
        msg = f"Element '{element_name}' not found{context_msg}"
        logging.error(f"ASSERTION FAILED: {msg}")
        raise AssertionError(msg)
    logging.debug(f"Element '{element_name}' found successfully{context_msg}")
    return element

def assert_file_exists(filepath, file_description=""):
    """Assert that a file was created/downloaded"""
    desc = file_description or filepath
    if not os.path.exists(filepath):
        msg = f"File not found: {desc} at {filepath}"
        logging.error(f"ASSERTION FAILED: {msg}")
        raise AssertionError(msg)
    logging.info(f"File verified: {desc}")
    return filepath

def assert_data_not_empty(data, data_name):
    """Assert that scraped data is not empty"""
    if not data or len(data) == 0:
        msg = f"No data found for: {data_name}"
        logging.error(f"ASSERTION FAILED: {msg}")
        raise AssertionError(msg)
    logging.info(f"Data validated: {data_name} contains {len(data)} items")
    return data

def assert_url_valid(url):
    """Assert URL is properly formatted"""
    assert_with_log(url.startswith(('http://', 'https://')), f"Valid URL format: {url}")
    return url

def assert_file_downloaded(filepath, min_size_bytes=1000, max_wait_seconds=30):
    """Assert file downloaded with valid size"""
    waited = 0
    while waited < max_wait_seconds:
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            assert_with_log(size >= min_size_bytes, f"File {filepath} has valid size: {size} bytes")
            return filepath
        time.sleep(1)
        waited += 1
    raise AssertionError(f"File not downloaded after {max_wait_seconds}s: {filepath}")

# ============================================================================
# ERROR HANDLING
# ============================================================================

def save_error_screenshot(page_or_driver, error_context=""):
    """Save screenshot when error occurs"""
    if page_or_driver:
        screenshot_path = f'logs/error_{timestamp}_{error_context}.png'
        try:
            if BROWSER_TYPE == "playwright":
                page_or_driver.screenshot(path=screenshot_path)
            else:
                page_or_driver.save_screenshot(screenshot_path)
            logging.error(f"Screenshot saved: {screenshot_path}")
            return screenshot_path
        except Exception as e:
            logging.error(f"Failed to save screenshot: {e}")
    return None

def save_page_source(page_or_driver, error_context=""):
    """Save HTML source when error occurs"""
    if page_or_driver:
        html_path = f'logs/error_{timestamp}_{error_context}.html'
        try:
            if BROWSER_TYPE == "playwright":
                content = page_or_driver.content()
            else:
                content = page_or_driver.page_source
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(content)
            logging.error(f"Page source saved: {html_path}")
            return html_path
        except Exception as e:
            logging.error(f"Failed to save page source: {e}")
    return None

# ============================================================================
# PDF DOWNLOAD FUNCTION
# ============================================================================

def download_pdf_with_requests(url, filename):
    """Download PDF file using requests library"""
    try:
        logging.info(f"Downloading PDF from: {url}")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()

        # Verify it's a PDF
        content_type = response.headers.get('Content-Type', '')
        if 'pdf' not in content_type.lower():
            logging.warning(f"Warning: Content-Type is {content_type}, expected PDF")

        filepath = os.path.join('downloads', filename)
        with open(filepath, 'wb') as f:
            f.write(response.content)

        file_size = os.path.getsize(filepath)
        logging.info(f"PDF downloaded successfully: {filepath} ({file_size} bytes)")

        assert_file_exists(filepath, f"Downloaded PDF: {filename}")
        assert_with_log(file_size > 1000, f"PDF file size is valid: {file_size} bytes")

        return filepath

    except requests.RequestException as e:
        logging.error(f"Failed to download PDF: {e}")
        raise

# ============================================================================
# HELPER FUNCTION TO FIND LATEST MONTH/YEAR
# ============================================================================

def find_and_click_latest_month(page_or_driver, browser_type):
    """
    Find and click on the latest month/year filter on the page.
    Returns the selected month and year.
    """
    months = ['January', 'February', 'March', 'April', 'May', 'June',
              'July', 'August', 'September', 'October', 'November', 'December']

    current_date = datetime.now()
    current_year = current_date.year

    # Search strategy: Start from current month/year and go backwards
    # We'll look for combinations like "January 2025", "December 2024", etc.

    if browser_type == "playwright":
        logging.info("Searching for latest month/year filter...")

        # Try to find all month/year combinations on the page
        found_filters = []

        # Try current and previous 2 years
        for year in range(current_year, current_year - 3, -1):
            for month_num in range(12, 0, -1):
                month = months[month_num - 1]

                # Try different text patterns
                patterns = [
                    f"{month} {year}",
                    f"{month}, {year}",
                    f"{month[:3]} {year}",  # Abbreviated month
                ]

                for pattern in patterns:
                    try:
                        elements = page_or_driver.locator(f"text={pattern}").all()
                        if elements:
                            logging.info(f"Found filter: {pattern} ({len(elements)} elements)")
                            found_filters.append({
                                'year': year,
                                'month_num': month_num,
                                'month': month,
                                'pattern': pattern,
                                'element': elements[0]  # Take first occurrence
                            })
                            break  # Move to next month after finding first pattern match
                    except:
                        continue

        if not found_filters:
            # Fallback: just look for month names and try to find the latest
            logging.warning("No year-specific filters found, searching for month names only...")
            for month in reversed(months):
                try:
                    elements = page_or_driver.locator(f"div:has-text('{month}')").all()
                    if elements:
                        logging.info(f"Found month filter (no year): {month}")
                        elements[-1].click()  # Click the last occurrence
                        time.sleep(1)
                        return month, None
                except:
                    continue
            raise AssertionError("Could not find any month filters on the page")

        # Sort filters by year (desc) then month (desc) to get the latest
        found_filters.sort(key=lambda x: (x['year'], x['month_num']), reverse=True)
        latest = found_filters[0]

        logging.info(f"Latest month/year found: {latest['pattern']}")
        latest['element'].click()
        time.sleep(1)

        return latest['month'], latest['year']

    else:  # Selenium
        from selenium.webdriver.common.by import By

        logging.info("Searching for latest month/year filter...")

        found_filters = []

        # Try current and previous 2 years
        for year in range(current_year, current_year - 3, -1):
            for month_num in range(12, 0, -1):
                month = months[month_num - 1]

                patterns = [
                    f"{month} {year}",
                    f"{month}, {year}",
                    f"{month[:3]} {year}",
                ]

                for pattern in patterns:
                    try:
                        elements = page_or_driver.find_elements(By.XPATH, f"//*[contains(text(), '{pattern}')]")
                        if elements:
                            logging.info(f"Found filter: {pattern} ({len(elements)} elements)")
                            found_filters.append({
                                'year': year,
                                'month_num': month_num,
                                'month': month,
                                'pattern': pattern,
                                'element': elements[0]
                            })
                            break
                    except:
                        continue

        if not found_filters:
            # Fallback: just look for month names
            logging.warning("No year-specific filters found, searching for month names only...")
            for month in reversed(months):
                try:
                    elements = page_or_driver.find_elements(By.XPATH, f"//div[contains(text(), '{month}')]")
                    if elements:
                        logging.info(f"Found month filter (no year): {month}")
                        elements[-1].click()
                        time.sleep(1)
                        return month, None
                except:
                    continue
            raise AssertionError("Could not find any month filters on the page")

        # Sort to get latest
        found_filters.sort(key=lambda x: (x['year'], x['month_num']), reverse=True)
        latest = found_filters[0]

        logging.info(f"Latest month/year found: {latest['pattern']}")
        latest['element'].click()
        time.sleep(1)

        return latest['month'], latest['year']

# ============================================================================
# PLAYWRIGHT IMPLEMENTATION
# ============================================================================

def run_playwright_scraper():
    """Run scraper using Playwright"""
    from playwright.sync_api import sync_playwright

    playwright = None
    browser = None
    page = None

    try:
        logging.info("Initializing Playwright browser...")
        playwright = sync_playwright().start()

        browser = playwright.chromium.launch(headless=HEADLESS)
        context = browser.new_context()
        page = context.new_page()

        logging.info("Navigating to Illinois Treasurer website...")
        page.goto('https://illinoistreasurer.gov/')
        assert_with_log(page.url.startswith('https://illinoistreasurer.gov'), "Successfully loaded homepage")

        logging.info("Clicking on 'INDIVIDUALS' link...")
        page.get_by_role('link', name='INDIVIDUALS').click()
        time.sleep(1)

        logging.info("Clicking on 'Saving for Retirement' link...")
        page.get_by_role('link', name='Saving for Retirement').click()
        time.sleep(1)

        logging.info("Clicking on 'Secure Choice' link...")
        page.locator('#menu-item-2741').get_by_role('link', name='Secure Choice').click()
        time.sleep(1)

        logging.info("Clicking on 'Secure Choice Performance' link...")
        page.get_by_role('link', name='Secure Choice Performance').click()
        time.sleep(2)

        # Find and click the latest month/year filter
        selected_month, selected_year = find_and_click_latest_month(page, "playwright")
        year_str = f"_{selected_year}" if selected_year else ""
        logging.info(f"Selected filter: {selected_month}{year_str}")

        logging.info("Looking for 'Secure Choice Monthly' link...")
        # Get the PDF link
        pdf_link_element = page.get_by_role('link', name='Secure Choice Monthly')
        assert_element_exists(pdf_link_element, "Secure Choice Monthly link")

        # Extract the href attribute
        pdf_url = pdf_link_element.get_attribute('href')
        assert_with_log(pdf_url is not None, "PDF URL extracted successfully")
        logging.info(f"Found PDF URL: {pdf_url}")

        # Make URL absolute if it's relative
        if not pdf_url.startswith('http'):
            pdf_url = urljoin(page.url, pdf_url)
            logging.info(f"Converted to absolute URL: {pdf_url}")

        # Download PDF using requests
        filename = f"SecureChoice_Monthly_{selected_month}{year_str}_{timestamp}.pdf"
        download_pdf_with_requests(pdf_url, filename)

        logging.info("Scraping completed successfully!")
        return 0

    except Exception as e:
        logging.error(f"Error in Playwright scraper: {e}")
        save_error_screenshot(page, "playwright_error")
        save_page_source(page, "playwright_error")
        raise

    finally:
        if page:
            page.close()
        if browser:
            browser.close()
        if playwright:
            playwright.stop()
        logging.info("Playwright browser closed")

# ============================================================================
# SELENIUM IMPLEMENTATION
# ============================================================================

def run_selenium_scraper():
    """Run scraper using Selenium"""
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options

    driver = None

    try:
        logging.info("Initializing Selenium Chrome driver...")
        chrome_options = Options()
        if HEADLESS:
            chrome_options.add_argument('--headless')
            chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.add_argument('--window-size=1920,1080')

        driver = webdriver.Chrome(options=chrome_options)
        wait = WebDriverWait(driver, 10)

        logging.info("Navigating to Illinois Treasurer website...")
        driver.get('https://illinoistreasurer.gov/')
        assert_with_log(driver.current_url.startswith('https://illinoistreasurer.gov'), "Successfully loaded homepage")

        logging.info("Clicking on 'INDIVIDUALS' link...")
        individuals_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, 'INDIVIDUALS')))
        individuals_link.click()
        time.sleep(1)

        logging.info("Clicking on 'Saving for Retirement' link...")
        retirement_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, 'Saving for Retirement')))
        retirement_link.click()
        time.sleep(1)

        logging.info("Clicking on 'Secure Choice' link...")
        secure_choice_link = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, '#menu-item-2741 a[href*="Secure"]')))
        secure_choice_link.click()
        time.sleep(1)

        logging.info("Clicking on 'Secure Choice Performance' link...")
        performance_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, 'Secure Choice Performance')))
        performance_link.click()
        time.sleep(2)

        # Find and click the latest month/year filter
        selected_month, selected_year = find_and_click_latest_month(driver, "selenium")
        year_str = f"_{selected_year}" if selected_year else ""
        logging.info(f"Selected filter: {selected_month}{year_str}")

        logging.info("Looking for 'Secure Choice Monthly' link...")
        pdf_link = wait.until(EC.presence_of_element_located((By.LINK_TEXT, 'Secure Choice Monthly')))
        assert_element_exists(pdf_link, "Secure Choice Monthly link")

        # Extract the href attribute
        pdf_url = pdf_link.get_attribute('href')
        assert_with_log(pdf_url is not None, "PDF URL extracted successfully")
        logging.info(f"Found PDF URL: {pdf_url}")

        # Make URL absolute if it's relative
        if not pdf_url.startswith('http'):
            pdf_url = urljoin(driver.current_url, pdf_url)
            logging.info(f"Converted to absolute URL: {pdf_url}")

        # Download PDF using requests
        filename = f"SecureChoice_Monthly_{selected_month}{year_str}_{timestamp}.pdf"
        download_pdf_with_requests(pdf_url, filename)

        logging.info("Scraping completed successfully!")
        return 0

    except Exception as e:
        logging.error(f"Error in Selenium scraper: {e}")
        save_error_screenshot(driver, "selenium_error")
        save_page_source(driver, "selenium_error")
        raise

    finally:
        if driver:
            driver.quit()
            logging.info("Selenium browser closed")

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Main function with comprehensive error handling"""

    try:
        logging.info("="*60)
        logging.info(f"STARTING SCRIPT - {timestamp}")
        logging.info(f"Configuration: HEADLESS={HEADLESS}, BROWSER={BROWSER_TYPE}")
        logging.info("="*60)

        if BROWSER_TYPE == "playwright":
            return run_playwright_scraper()
        elif BROWSER_TYPE == "selenium":
            return run_selenium_scraper()
        else:
            raise ValueError(f"Invalid BROWSER_TYPE: {BROWSER_TYPE}. Use 'playwright' or 'selenium'")

    except AssertionError as e:
        logging.error("="*60)
        logging.error("✗ ASSERTION FAILED")
        logging.error(f"✗ Error: {str(e)}")
        logging.error("="*60)
        return 1

    except Exception as e:
        logging.error("="*60)
        logging.error("✗ UNEXPECTED ERROR OCCURRED")
        logging.error(f"✗ Error Type: {type(e).__name__}")
        logging.error(f"✗ Error Message: {str(e)}")
        logging.error("="*60)
        logging.error("Full Traceback:")
        logging.error(traceback.format_exc())
        return 1

    finally:
        logging.info("="*60)
        logging.info("Script execution finished")
        logging.info("="*60)

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
