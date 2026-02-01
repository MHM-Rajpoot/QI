import csv
"""
WebScraperHumanized.py
Scrapes Glassdoor job listings with human-like browser actions.
"""
import time
import random
import logging
import re
import os
import undetected_chromedriver as uc
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import InvalidSessionIdException, WebDriverException, StaleElementReferenceException

# ---------------- LOGGING ---------------- #
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# ---------------- LOGGING ---------------- #
logger = logging.getLogger(__name__)

# ---------------- DRIVER SETUP ---------------- #
def setup_driver():
        # Setup undetected Chrome driver with custom options and user profile
    options = uc.ChromeOptions()
    options.add_argument("--start-maximized")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--lang=en-GB")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36")

    profile_dir = os.path.join(os.getcwd(), "chrome_profile")
    options.add_argument(f"--user-data-dir={profile_dir}")

    driver = uc.Chrome(
        options=options,
        headless=False,
        use_subprocess=True,
        version_main=144  # Match your Chrome browser version
    )
    return driver

# ---------------- UTILS ---------------- #
def human_sleep(a=1.5, b=4.5):
        # Sleep for a random interval to mimic human pauses
    # Random pause
    time.sleep(random.uniform(a, b))
    # Occasionally do a longer pause
    if random.random() < 0.08:
        time.sleep(random.uniform(2, 5))

def random_mouse_move(driver):
        # Move mouse to random positions to simulate human behavior
    # Move mouse to random positions on the page
    try:
        width = driver.execute_script("return window.innerWidth")
        height = driver.execute_script("return window.innerHeight")
        for _ in range(random.randint(1, 3)):
            x = random.randint(0, width)
            y = random.randint(0, height)
            ActionChains(driver).move_by_offset(x, y).perform()
            time.sleep(random.uniform(0.1, 0.4))
    except Exception:
        pass

def random_tab_switch(driver):
        # Occasionally switch tabs to mimic user multitasking
    # Simulate tab switching
    if random.random() < 0.05:
        driver.execute_script("window.open('about:blank', '_blank');")
        time.sleep(random.uniform(0.5, 1.5))
        handles = driver.window_handles
        if len(handles) > 1:
            driver.switch_to.window(handles[-1])
            time.sleep(random.uniform(0.5, 1.2))
            driver.close()
            driver.switch_to.window(handles[0])

def random_resize(driver):
        # Occasionally resize browser window to mimic user adjustments
    # Occasionally resize window
    if random.random() < 0.07:
        w = random.randint(900, 1400)
        h = random.randint(600, 900)
        try:
            driver.set_window_size(w, h)
        except Exception:
            pass

def scroll_page(driver, times=3):
        # Scroll down the page several times, sometimes scroll up
    for _ in range(times):
        driver.execute_script("window.scrollBy(0, document.body.scrollHeight);")
        human_sleep(1, 2)
        # Occasionally scroll up
        if random.random() < 0.2:
            driver.execute_script("window.scrollBy(0, -document.body.scrollHeight/2);")
            human_sleep(0.5, 1.2)

def wait_for_page_ready(driver, timeout=12):
        # Wait for page to fully load and body to be present
    try:
        WebDriverWait(driver, timeout).until(
            lambda d: d.execute_script("return document.readyState") == "complete"
        )
    except Exception:
        pass
    try:
        WebDriverWait(driver, timeout).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
    except Exception:
        pass
    # Add humanized actions
    random_mouse_move(driver)
    random_tab_switch(driver)
    random_resize(driver)

# ---------------- FILE LOAD ---------------- #
def read_list(filename):
        # Read a list of items from a text file
    with open(filename, "r", encoding="utf-8") as f:
        return [x.strip() for x in f if x.strip()]

def slugify(value: str) -> str:
        # Convert string to URL-friendly slug
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")

def try_accept_cookies(driver):
        # Try to accept cookies using common selectors
    selectors = [
        "button#onetrust-accept-btn-handler",
        "button[data-test='acceptCookiesButton']",
        "button[title='Accept all']",
        "button[aria-label='Accept all']"
    ]
    for sel in selectors:
        try:
            btn = driver.find_element(By.CSS_SELECTOR, sel)
            btn.click()
            human_sleep(1, 2)
            break
        except Exception:
            continue

# ---------------- JOB CARD SCRAPER ---------------- #
def collect_job_cards(driver, city, title, max_pages=2):
        # Collect job cards and details for a given city and job title
    collected = []

    formatted_city = slugify(city)
    formatted_title = slugify(title)
    
    # Build proper Glassdoor URL with location and job title
    location_str = formatted_city
    title_str = formatted_title
    location_len = len(location_str)
    title_offset = location_len + 1
    title_end = title_offset + len(title_str)
    
    base_url = f"https://www.glassdoor.co.uk/Job/{location_str}-{title_str}-jobs-SRCH_IL.0,{location_len}_KO{title_offset},{title_end}.htm?sortBy=date_desc&fromAge=30"

    alt_url = f"https://www.glassdoor.co.uk/Job/jobs.htm?sc.keyword={title}&sc.location={city}&sortBy=date_desc&fromAge=30"

    def wait_for_cards(timeout=8):
            # Wait for job cards to appear on the page
        try:
            WebDriverWait(driver, timeout).until(
                EC.presence_of_element_located((
                    By.CSS_SELECTOR,
                    "ul[class*='JobsList_jobsList'] li[data-test='jobListing'], li.react-job-listing, div[data-test='jobListing'], article[data-test='jobListing'], div[data-test='job-card']"
                ))
            )
            # Humanized actions after cards appear
            random_mouse_move(driver)
            random_tab_switch(driver)
            random_resize(driver)
            return True
        except Exception:
            return False

    driver.get(base_url)
    human_sleep(4, 7)
    try_accept_cookies(driver)
    wait_for_page_ready(driver)

    if not wait_for_cards():
        logger.warning("No job cards detected at base URL for %s in %s; trying alternate URL", title, city)
        driver.get(alt_url)
        human_sleep(4, 7)
        try_accept_cookies(driver)
        wait_for_page_ready(driver)
        if not wait_for_cards():
            logger.warning("No job cards detected after alternate URL for %s in %s", title, city)
            os.makedirs("debug", exist_ok=True)
            try:
                driver.save_screenshot(os.path.join("debug", "no_cards_initial.png"))
            except Exception:
                pass
            try:
                with open(os.path.join("debug", "no_cards_initial.html"), "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
            except Exception:
                pass
            return collected

    def first_text_in(root, selectors, fallback="Not specified"):
            # Get first non-empty text from a list of selectors
        for sel in selectors:
            try:
                el = root.find_element(By.CSS_SELECTOR, sel)
                text = el.text.strip()
                if text:
                    return text
            except Exception:
                continue
        return fallback

    def extract_detail_info(job_id=None, job_title_text=""):
            # Extract job description and skills from job details panel
        # Wait for job details panel to be present and updated
        try:
            WebDriverWait(driver, 8).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='JobDetails_jobDescription']"))
            )
            human_sleep(0.5, 1.0)
        except Exception:
            pass

        # Verify we're looking at the correct job by checking title match
        if job_title_text:
            try:
                detail_title = driver.find_element(By.CSS_SELECTOR, "h1[data-test='job-title'], div[data-test='job-title'], h2[class*='JobDetails_jobTitle']").text.strip()
                if job_title_text.lower() not in detail_title.lower() and detail_title.lower() not in job_title_text.lower():
                    logger.warning("Title mismatch: card='%s' vs detail='%s'", job_title_text, detail_title)
            except Exception:
                pass

        description = "Not specified"
        try:
            desc_el = driver.find_element(By.CSS_SELECTOR, "div[class*='JobDetails_jobDescription']")
            description = desc_el.text.strip() if desc_el else "Not specified"
        except Exception:
            try:
                desc_el = driver.find_element(By.CSS_SELECTOR, "div[data-test='jobDescriptionText']")
                description = desc_el.text.strip() if desc_el else "Not specified"
            except Exception:
                pass

        if not description or description == "Not specified":
            try:
                desc_el = driver.find_element(By.CSS_SELECTOR, "section[data-test='jobDescription']")
                description = desc_el.text.strip() if desc_el else "Not specified"
            except Exception:
                pass

        skills = []
        try:
            for el in driver.find_elements(By.CSS_SELECTOR, "span[class*='PendingQualification_label']"):
                text = el.text.strip()
                if text:
                    skills.append(text)
        except Exception:
            pass

        skills_text = ", ".join(dict.fromkeys(skills)) if skills else "Not specified"

        return {
            "description": description if description else "Not specified",
            "skills": skills_text
        }

    for page_index in range(max_pages):
            # Loop through job listing pages
        scroll_page(driver, 4)
        # Humanized actions between pages
        random_mouse_move(driver)
        random_tab_switch(driver)
        random_resize(driver)

        cards = driver.find_elements(
            By.CSS_SELECTOR,
            "ul[class*='JobsList_jobsList'] li[data-test='jobListing']"
        )
        if not cards:
            cards = driver.find_elements(
                By.CSS_SELECTOR,
                "li.react-job-listing, div[data-test='jobListing'], article[data-test='jobListing'], div[data-test='job-card']"
            )

        logger.info("Found %d cards on page %s for %s in %s", len(cards), page_index + 1, title, city)

        if not cards:
            try:
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "ul[class*='JobsList_jobsList'] li[data-test='jobListing']"))
                )
                cards = driver.find_elements(By.CSS_SELECTOR, "ul[class*='JobsList_jobsList'] li[data-test='jobListing']")
            except Exception:
                pass

        if not cards:
            page_title = driver.title
            page_text = driver.page_source[:2000].lower()
            blocked = any(x in page_text for x in ["captcha", "access denied", "unusual traffic", "robot"])
            logger.warning("No job cards found on page %s (title=%s, blocked=%s)", page_index + 1, page_title, blocked)
            os.makedirs("debug", exist_ok=True)
            try:
                safe_city = slugify(city)
                safe_title = slugify(title)
                driver.save_screenshot(os.path.join("debug", f"no_cards_{safe_title}_{safe_city}_{page_index + 1}.png"))
            except Exception:
                pass
            try:
                with open(os.path.join("debug", f"no_cards_{safe_title}_{safe_city}_{page_index + 1}.html"), "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
            except Exception:
                pass
            if blocked:
                break

        seen_ids = set()

        for card in cards:
                        # For each job card, extract info and click to get details
            try:
                job_id = card.get_attribute("data-jobid") or ""
            except StaleElementReferenceException:
                continue

            if job_id and job_id in seen_ids:
                continue
            if job_id:
                seen_ids.add(job_id)

            job_title_selectors = [
                "a[data-test='job-title']",
                "a[data-test='job-link']",
                "a[class*='JobCard_jobTitle']",
                "a"
            ]
            job_title = first_text_in(card, job_title_selectors)
            
            job_url = "Not specified"
            for sel in job_title_selectors:
                try:
                    link_el = card.find_element(By.CSS_SELECTOR, sel)
                    href = link_el.get_attribute("href")
                    if href:
                        job_url = href
                        break
                except Exception:
                    continue
            company = first_text_in(card, [
                "span[class*='EmployerProfile_compactEmployerName']",
                "div[data-test='employerName']",
                "span[data-test='employerName']",
                "div.employerName",
                "span.employerName"
            ])
            location = first_text_in(card, [
                "div[data-test='emp-location']",
                "span[data-test='emp-location']",
                "div[data-test='location']",
                "span[data-test='location']",
                "div.location",
                "span.location"
            ])
            salary = first_text_in(card, [
                "div[data-test='detailSalary']",
                "span[data-test='detailSalary']",
                "span.salary-estimate",
                "span.salary"
            ])
            post_date = first_text_in(card, [
                "div[data-test='job-age']",
                "span[data-test='job-age']",
                "span.job-age",
                "div.job-age"
            ])

            # Humanized: hover over card before clicking
            try:
                ActionChains(driver).move_to_element(card).perform()
                            # Hover over job card before clicking
                time.sleep(random.uniform(0.2, 0.7))
            except Exception:
                pass

            # Click on the card to load job details
            details = {"description": "Not specified", "skills": "Not specified"}
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", card)
                                # Scroll job card into view
                human_sleep(0.4, 0.8)
                random_mouse_move(driver)
                try:
                    card.click()
                                # Click job card to load details
                except Exception:
                    driver.execute_script("arguments[0].click();", card)
                human_sleep(0.8, 1.5)
                random_tab_switch(driver)
                random_resize(driver)
                # Extract details after clicking
                details = extract_detail_info(job_id=job_id, job_title_text=job_title)
                            # Extract job details after clicking
            except StaleElementReferenceException:
                logger.warning("Stale element when clicking card for: %s", job_title)
            except Exception as e:
                logger.warning("Error clicking/extracting for '%s': %s", job_title, str(e))

            collected.append({
                "domain": "glassdoor.co.uk",
                "job_title": job_title,
                "company": company,
                "location": location,
                "salary": salary,
                "job_type": "Not specified",
                "experience_level": "Not specified",
                "education_level": "Not specified",
                "skills": details.get("skills", "Not specified"),
                "post_date": post_date,
                "description": details.get("description", "Not specified"),
                "job_url": job_url,
                "scraped_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

        # Try pagination (if exists)
        try:
            next_btn = driver.find_element(By.CSS_SELECTOR, "button[data-test='pagination-next']")
            ActionChains(driver).move_to_element(next_btn).perform()
                        # Hover over next page button before clicking
            time.sleep(random.uniform(0.2, 0.7))
            random_mouse_move(driver)
            ActionChains(driver).move_to_element(next_btn).click().perform()
                        # Click next page button
            human_sleep(4, 6)
            random_tab_switch(driver)
            random_resize(driver)
        except:
            break

    logger.info(f"Collected {len(collected)} jobs for {title} in {city}")
    return collected

# ---------------- MAIN SCRAPER ---------------- #
def scrape(cities, titles, output_file):
        # Main scraping loop for all cities and job titles
    driver = setup_driver()

    fieldnames = [
        "domain",
        "job_title",
        "company",
        "location",
        "salary",
        "job_type",
        "experience_level",
        "education_level",
        "skills",
        "post_date",
        "description",
        "job_url",
        "scraped_at"
    ]

    file_exists = os.path.exists(output_file)
    total_saved = 0

    try:
        for city in cities:
                # For each city and job title, collect job cards and save to CSV
            for title in titles:
                try:
                    cards = collect_job_cards(driver, city, title)
                except (InvalidSessionIdException, WebDriverException):
                    logger.warning("Driver session lost. Reinitializing and retrying for %s in %s", title, city)
                    try:
                        driver.quit()
                        # Quit browser driver at the end
                    except Exception:
                        pass
                    driver = setup_driver()
                    cards = collect_job_cards(driver, city, title)

                if cards:
                    mode = "a" if file_exists else "w"
                    with open(output_file, mode, newline="", encoding="utf-8") as f:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        if not file_exists:
                            writer.writeheader()
                            file_exists = True
                        writer.writerows(cards)
                    
                    total_saved += len(cards)
                    logger.info(f"Appended {len(cards)} jobs to {output_file} (total: {total_saved})")
                    
                    for data in cards:
                        logger.info(f"Saved: {data['job_title']}")

    finally:
        driver.quit()

    if total_saved > 0:
        logger.info(f"Total {total_saved} jobs saved → {output_file}")
    else:
        logger.warning("No data scraped")

# ---------------- RUN ---------------- #
if __name__ == "__main__":
        # Entry point: read input files and start scraping
    cities = read_list("cities.txt")
    titles = read_list("title.txt")

    output = f"glassdoor_jobs_{datetime.now().strftime('%Y%m%d')}.csv"
    scrape(cities, titles, output)
