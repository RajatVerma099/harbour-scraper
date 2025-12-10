#!/usr/bin/env python3
import os
import re
import asyncio
import json
from datetime import datetime, timedelta
import traceback

import requests
from bs4 import BeautifulSoup

import firebase_admin
from firebase_admin import credentials, firestore

from telethon import TelegramClient
from telethon.sessions import StringSession  # NEW

# ===================== PATHS / CONSTANTS =====================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# File that permanently stores already-processed job URLs (per environment)
PROCESSED_URLS_FILE = os.path.join(BASE_DIR, "processed_urls.txt")

# Log file
LOG_FILE = os.path.join(BASE_DIR, "scraper.log")

GROUP_NAME_HINT = "Fresher Jobs Openings"

# Regex for extracting URLs from messages
URL_REGEX = re.compile(r"https?://[^\s]+")

# Domains we care about
TARGET_DOMAINS = ("fresheropenings.com", "freshersrecruitment.co.in")

# ===================== LOGGER =====================

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

def pretty_log_job(job: dict):
    try:
        pretty = json.dumps(job, indent=2, ensure_ascii=False)
    except Exception:
        pretty = repr(job)
    log("----- JOB DATA START -----")
    for line in pretty.splitlines():
        log(line)
    log("----- JOB DATA END -----")

# ===================== FIREBASE SETUP =====================

FIREBASE_KEY_PATH = os.path.join(BASE_DIR, "harbour-final-firebase-private-key.json")
FIREBASE_KEY_JSON = os.environ.get("FIREBASE_KEY_JSON")

# On GitHub Actions: use FIREBASE_KEY_JSON from env (secret).
# Locally: fall back to the JSON file on disk.
try:
    if FIREBASE_KEY_JSON:
        # Load from environment (GitHub Actions)
        service_account_info = json.loads(FIREBASE_KEY_JSON)
        cred = credentials.Certificate(service_account_info)
    else:
        # Local dev: use the file
        cred = credentials.Certificate(FIREBASE_KEY_PATH)
except Exception as e:
    print(f"[FIREBASE] Failed to load credentials: {e}")
    raise

firebase_admin.initialize_app(cred)
db = firestore.client()

# ===================== DEDUP HELPERS =====================

def job_exists_for_url(url: str) -> bool:
    """
    Returns True if a Job with this moreInfoLink already exists in Firestore.
    This prevents reposting the same job across runs / days.
    """
    try:
        query = (
            db.collection("Jobs")
            .where("moreInfoLink", "==", url)
            .limit(1)
            .stream()
        )
        for _ in query:
            return True
        return False
    except Exception as e:
        log(f"[DEDUP] Error while checking existing job for url={url}: {e}")
        log(traceback.format_exc())
        # On error, treat as not existing so scraper can still function
        return False

# ===================== TELEGRAM API CONFIG =====================

API_ID = int(os.environ.get("TG_API_ID", "22275520"))
API_HASH = os.environ.get("TG_API_HASH", "2fa908c209c73b52096afb82a18342b2")

SESSION_NAME = os.path.join(BASE_DIR, "harbour_manual_session")

# Optional string session for GitHub / headless runs
TG_SESSION_STRING = os.environ.get("TG_SESSION_STRING")

# ===================== ONE SIGNAL NOTIFICATION FUNCTION =====================

ONESIGNAL_APP_ID = os.environ.get("ONESIGNAL_APP_ID", "56c94a7a-618b-41d6-8db3-955968baf359")
ONESIGNAL_REST_API_KEY = os.environ.get("ONESIGNAL_REST_API_KEY", "ZjJiNDZhNmUtOTZkYy00ZjYwLTgyZjQtNDAyYTAzOTljNzdk")
ONESIGNAL_API_URL = "https://onesignal.com/api/v1/notifications"

def send_onesignal_notification_for_job(job: dict) -> bool:
    """
    Sends a OneSignal notification for the provided job dict.
    Returns True on success, False otherwise.
    """
    try:
        company_name = job.get("company", "Unknown Company")
        title = job.get("job-title", "Job Opening")
        apply_link = job.get("apply-link") or job.get("moreInfoLink") or "#"

        notification_message = f"{company_name} has openings for {title}. Click now to apply!"

        payload = {
            "app_id": ONESIGNAL_APP_ID,
            "included_segments": ["All"],
            "headings": {"en": "Job Opening Notification"},
            "contents": {"en": notification_message},
            "url": apply_link
        }

        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Basic {ONESIGNAL_REST_API_KEY}"
        }

        resp = requests.post(ONESIGNAL_API_URL, headers=headers, json=payload, timeout=20)
        if resp.status_code in (200, 201, 202):
            log(f"[ONESIGNAL] Notification sent for job: {job.get('title')}")
            return True
        else:
            try:
                err = resp.json()
            except Exception:
                err = resp.text
            log(f"[ONESIGNAL] Failed to send notification (status={resp.status_code}): {err}")
            return False
    except Exception as e:
        log(f"[ONESIGNAL] Exception while sending notification: {e}")
        log(traceback.format_exc())
        return False

# ===================== HTTP FETCHER =====================

def fetch_page(url: str):
    session = requests.Session()

    headers_list = [
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        },
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) "
                "Gecko/20100101 Firefox/122.0"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Connection": "keep-alive",
        },
    ]

    for headers in headers_list:
        try:
            resp = session.get(url, headers=headers, timeout=25)
            if resp.status_code == 200:
                return resp
            else:
                log(f"[FETCH] {url} -> status {resp.status_code}, trying next UA...")
        except Exception as e:
            log(f"[FETCH] Error fetching {url} with UA {headers.get('User-Agent')}: {e}")

    log(f"[FETCH] All header strategies failed for {url}")
    return None

# ===================== DATE HELPERS (FreshersRecruitment) =====================

MONTH_RE = r"(January|February|March|April|May|June|July|August|September|October|November|December)"

def extract_post_date(soup: BeautifulSoup) -> str:
    date_str = None
    for text in soup.find_all(string=True):
        m = re.search(rf"{MONTH_RE}\s+\d{{1,2}},\s+\d{{4}}", text)
        if m:
            date_str = m.group(0)
            break

    if date_str:
        try:
            dt = datetime.strptime(date_str, "%B %d, %Y")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass

    return datetime.now().strftime("%Y-%m-%d")

# ===================== SCRAPER FUNCTIONS =====================

def scrape_job_data_fresheropenings(url: str):
    log(f"[SCRAPER][FresherOpenings] Fetching URL: {url}")
    response = fetch_page(url)
    if response is None or response.status_code != 200:
        log("[SCRAPER] Failed to fetch page (all attempts).")
        return None

    soup = BeautifulSoup(response.content, 'html.parser')
    job_data = {}
    job_data["date-posted"] = datetime.now().strftime("%Y-%m-%d")

    table = soup.find('table')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            cols = row.find_all('td')
            if len(cols) != 2:
                continue
            key = cols[0].get_text(strip=True)
            value = cols[1].get_text(strip=True)

            if key in [
                "Company Name", "Company Name:", "Company Name -", "Company Name :",
                "Recruitment Authority", "Recruitment Authority:", "Recruitment Authority -", "Recruitment Authority :",
                "Employer Name", "Employer Name:",
                "Company/Organization", "Company/Organization:",
                "Company Info", "Company Info:",
                "Institution", "Institution:", "Institution -Company",
                "Company", "Company:", "Company -", "Company :",
                "Hiring Company", "Hiring Company:", "Hiring Company -", "Hiring Company :",
                "Organisation", "Organisation:", "Organisation -", "Organisation :",
                "Organization", "Organization:", "Organization -", "Organization :",
                "Employer", "Employer:", "Employer -", "Employer :",
                "Firm", "Firm:", "Firm -", "Firm :",
                "Recruiter", "Recruiter:", "Recruiter -", "Recruiter :",
                "Hiring Organization", "Hiring Organization:", "Hiring Organization -", "Hiring Organization :"
            ]:
                job_data["company"] = value

            elif key in [
                "Job Role", "Job Role:", "Job Role -", "Job Role :",
                "Opening Title", "Opening Title:",
                "Vacancy", "Vacancy Title",
                "Position Name", "Position Name:",
                "Job Opening", "Hiring For", "Job Name",
                "Role", "Role:", "Role -", "Role :",
                "Position", "Position:", "Position -", "Position :",
                "Job Title", "Job Title:", "Job Title -", "Job Title :",
                "Title", "Title:", "Title -", "Title :",
                "Designation", "Designation:", "Designation -", "Designation :",
                "Post", "Post:", "Post -", "Post :",
                "Opening", "Opening:", "Opening -", "Opening :",
                "Position Title", "Position Title:", "Position Title -", "Position Title :",
                "Job Position", "Job Position:", "Job Position -", "Job Position :"
            ]:
                job_data["job-title"] = value

            elif key in [
                "Experience", "Experience:",
                "Experience Needed", "Experience Needed:",
                "Years Required", "Years Required:",
                "Required Work Experience",
                "Experience -", "Experience :",
                "Experienced", "Experienced:", "Experienced -", "Experienced :",
                "Experiences", "Experiences:", "Experiences -", "Experiences :",
                "Work Experience", "Work Experience:", "Work Experience -", "Work Experience :",
                "Required Experience", "Required Experience:", "Required Experience -", "Required Experience :",
                "Minimum Experience", "Minimum Experience:", "Minimum Experience -", "Minimum Experience :",
                "Experience Required", "Experience Required:", "Experience Required -", "Experience Required :",
                "Exp", "Exp:", "Exp -", "Exp :",
                "Exp.", "Exp.:", "Exp. -", "Exp. :",
                "Total Experience", "Total Experience:", "Total Experience -", "Total Experience :",
                "Years of Experience", "Years of Experience:", "Years of Experience -", "Years of Experience :",
                "Experience Level", "Experience Level:", "Experience Level -", "Experience Level :",
                "Prior Experience", "Prior Experience:", "Prior Experience -", "Prior Experience :",
                "Professional Experience", "Professional Experience:", "Professional Experience -", "Professional Experience :"
            ]:
                job_data["experience"] = value

            elif key in [
                "Job Location", "Job Posting Location", "Office", "Job Place",
                "Job Location:", "Job Location -", "Job Location :",
                "Location", "Location:", "Location -", "Location :",
                "Locations", "Locations:", "Locations -", "Locations :",
                "Work Location", "Work Location:", "Work Location -", "Work Location :",
                "Posting Location", "Posting Location:", "Posting Location -", "Posting Location :",
                "Place of Posting", "Place of Posting:", "Place of Posting -", "Place of Posting :",
                "Place", "Place:", "Place -", "Place :",
                "Job Locations", "Job Locations:", "Job Locations -", "Job Locations :",
                "Job Place", "Job Place:", "Job Place -", "Job Place :",
                "Workplace", "Workplace:", "Workplace -", "Workplace :",
                "Office Location", "Office Location:", "Office Location -", "Office Location :",
                "Duty Location", "Duty Location:", "Duty Location -", "Duty Location :"
            ]:
                job_data["location"] = value

    def normalize_label(txt: str) -> str:
        return re.sub(r'[\s:\-\u2013]+$', '', txt.strip(), flags=re.UNICODE).lower()

    label_to_field = {
        "job": "job-title",
        "job role": "job-title",
        "position": "job-title",
        "role": "job-title",
        "experience": "experience",
        "job location": "location",
        "location": "location",
    }

    for p in soup.find_all('p'):
        full_text = p.get_text(" ", strip=True)
        if not full_text:
            continue

        m = re.match(r'^([^:]+):\s*(.+)$', full_text)
        if m:
            label_raw, value = m.group(1), m.group(2)
        else:
            parts = full_text.split(None, 1)
            if len(parts) != 2:
                continue
            label_raw, value = parts[0], parts[1]

        label_norm = normalize_label(label_raw)
        if label_norm in label_to_field and value:
            field = label_to_field[label_norm]
            if not job_data.get(field):
                job_data[field] = value.strip()

    title_tag = soup.find(['h1', 'h2'])
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        m_company = re.match(
            r'^(.+?)\s+(Walk-?in|Off\s*Campus|Recruitment|Hiring|Jobs|Careers)\b',
            title_text,
            flags=re.IGNORECASE
        )
        if m_company and not job_data.get("company"):
            job_data["company"] = m_company.group(1).strip()

        m_role = re.search(
            r'\bas\s+([^|:]+?)(\s+with|\s+With|\s*\||$)',
            title_text,
            flags=re.IGNORECASE
        )
        if m_role and not job_data.get("job-title"):
            job_data["job-title"] = m_role.group(1).strip()

    description_parts = []
    about_label = soup.find(
        lambda tag: tag.name in ["strong", "b"]
        and "about company" in tag.get_text(strip=True).lower()
    )
    if about_label:
        about_p = about_label.find_next("p")
        if about_p:
            about_text = about_p.get_text(strip=True)
            if about_text:
                description_parts.append(about_text)

    description_section = soup.find(
        lambda tag: (
            tag.name == 'p'
            and tag.string
            and (
                any(
                    tag.string.strip().lower() == key.lower()
                    for key in [
                        "Key Responsibilities:", "job description", "Job Summary", "Job Summary:", "Opportunity Details",
                        "Details about Role", "Work Details", "Work Summary",
                        "job description:", "job description -", "description", "description:", "description -",
                        "about the job", "about the job:", "about the job -",
                        "about job", "about job:", "about job -",
                        "about the role", "about the role:", "about the role -",
                        "role description", "role description:", "role description -",
                        "position description", "position description:", "position description -",
                        "job overview", "job overview:", "job overview -",
                        "role overview", "role overview:", "role overview -",
                        "what you will do", "what you will do:", "what you will do -",
                        "responsibilities", "responsibilities:", "responsibilities -",
                        "duties", "duties:", "duties -",
                        "job responsibilities", "job responsibilities:", "job responsibilities -",
                        "position overview", "position overview:", "position overview -"
                    ]
                )
                or
                re.search(
                    r'\b('
                    r'job(s)?\s+description(s)?|'
                    r'job(s)?\s+summary|'
                    r'key\s+responsibilit(y|ies)|'
                    r'responsibilit(y|ies)|'
                    r'key\s+duties|'
                    r'duties\s+and\s+responsibilit(y|ies)|'
                    r'role\s+responsibilit(y|ies)|'
                    r'position\s+description|'
                    r'position\s+overview|'
                    r'about\s+(the\s+)?(job|role)|'
                    r'job\s+role|'
                    r'about\s+position|'
                    r'what\s+you(\'ll|\s+will)?\s+do|'
                    r'what\s+you\s+will\s+be\s+doing|'
                    r'your\s+role|'
                    r'overview\s+of\s+responsibilit(y|ies)|'
                    r'opportunity\s+details|'
                    r'details\s+about\s+(the\s+)?role|'
                    r'role\s+overview|'
                    r'work\s+(details|summary)|'
                    r'job\s+profile|'
                    r'position\s+profile|'
                    r'job\s+purpose|'
                    r'objective\s+of\s+the\s+role|'
                    r'mission\s+of\s+the\s+role|'
                    r'job\s+objective|'
                    r'position\s+objective|'
                    r'role\s+description|'
                    r'job\s+information|'
                    r'position\s+information|'
                    r'role\s+and\s+responsibilit(y|ies)|'
                    r'profile\s+description|'
                    r'functional\s+responsibilit(y|ies)|'
                    r'business\s+function\s+description|'
                    r'description\s+of\s+duties|'
                    r'description\s+of\s+role|'
                    r'career\s+summary|'
                    r'profile\s+summary|'
                    r'professional\s+summary|'
                    r'career\s+objective|'
                    r'job\s+functions'
                    r')',
                    tag.string.strip().lower()
                )
            )
        )
    )

    extra_desc = []
    if description_section:
        next_nodes = description_section.find_next_siblings()
        for node in next_nodes:
            if node.name == 'p':
                extra_desc.append(node.get_text(strip=True))
            elif node.name == 'ul':
                for li in node.find_all('li'):
                    extra_desc.append(li.get_text(strip=True))

    if extra_desc:
        description_parts.extend(extra_desc)

    if description_parts:
        job_data["desc"] = "\n\n".join(description_parts).split("Join our WhatsApp")[0]
    else:
        job_data["desc"] = "N/A"

    apply_link = None
    apply_label = soup.find(
        lambda tag: tag.name in ["strong", "b"]
        and "apply link" in tag.get_text(strip=True).lower()
    )
    if apply_label:
        candidate = apply_label.find_next("a")
        if candidate and candidate.has_attr("href"):
            apply_link = candidate

    if not apply_link:
        for a in soup.find_all("a"):
            if "click here to apply" in a.get_text(strip=True).lower():
                apply_link = a
                break

    job_data["apply-link"] = (
        apply_link["href"] if (apply_link and apply_link.has_attr("href")) else "N/A"
    )

    job_data["moreInfoLink"] = url

    defaults = {
        "company": "N/A",
        "job-title": "N/A",
        "experience": "N/A",
        "location": "N/A",
        "apply-link": "N/A",
        "desc": "N/A",
    }
    for field, default_value in defaults.items():
        if not job_data.get(field):
            job_data[field] = default_value

    job_data["title"] = f"{job_data.get('company', 'N/A')} | {job_data.get('job-title', 'N/A')}"
    pretty_log_job(job_data)
    return job_data

def scrape_job_data_freshers_recruitment(url: str):
    log(f"[SCRAPER][FreshersRecruitment] Fetching URL: {url}")
    response = fetch_page(url)
    if response is None or response.status_code != 200:
        log("[SCRAPER] Failed to fetch page (all attempts).")
        return None

    soup = BeautifulSoup(response.content, "html.parser")
    job_data = {}
    job_data["date-posted"] = extract_post_date(soup)

    table = soup.find('table')
    if table:
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all(['th', 'td'])
            if len(cells) < 2:
                continue
            key = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)

            if key in [
                "Company Name", "Company Name:", "Company Name -", "Company Name :",
                "Recruitment Authority", "Recruitment Authority:", "Recruitment Authority -", "Recruitment Authority :",
                "Employer Name", "Employer Name:",
                "Company/Organization", "Company/Organization:",
                "Company Info", "Company Info:",
                "Institution", "Institution:", "Institution -Company",
                "Company", "Company:", "Company -", "Company :",
                "Hiring Company", "Hiring Company:", "Hiring Company -", "Hiring Company :",
                "Organisation", "Organisation:", "Organisation -", "Organisation :",
                "Organization", "Organization:", "Organization -", "Organization :",
                "Employer", "Employer:", "Employer -", "Employer :",
                "Firm", "Firm:", "Firm -", "Firm :",
                "Recruiter", "Recruiter:", "Recruiter -", "Recruiter :",
                "Hiring Organization", "Hiring Organization:", "Hiring Organization -", "Hiring Organization :"
            ]:
                job_data["company"] = value

            elif key in [
                "Job Role", "Job Role:", "Job Role -", "Job Role :",
                "Opening Title", "Opening Title:",
                "Vacancy", "Vacancy Title",
                "Position Name", "Position Name:",
                "Job Opening", "Hiring For", "Job Name",
                "Role", "Role:", "Role -", "Role :",
                "Position", "Position:", "Position -", "Position :",
                "Job Title", "Job Title:", "Job Title -", "Job Title :",
                "Title", "Title:", "Title -", "Title :",
                "Designation", "Designation:", "Designation -", "Designation :",
                "Post", "Post:", "Post -", "Post :",
                "Opening", "Opening:", "Opening -", "Opening :",
                "Position Title", "Position Title:", "Position Title -", "Position Title :",
                "Job Position", "Job Position:", "Job Position -", "Job Position :"
            ]:
                job_data["job-title"] = value

            elif key in [
                "Experience", "Experience:",
                "Experience Needed", "Experience Needed:",
                "Years Required", "Years Required:",
                "Required Work Experience",
                "Experience -", "Experience :",
                "Experienced", "Experienced:", "Experienced -", "Experienced :",
                "Experiences", "Experiences:", "Experiences -", "Experiences :",
                "Work Experience", "Work Experience:", "Work Experience -", "Work Experience :",
                "Required Experience", "Required Experience:", "Required Experience -", "Required Experience :",
                "Minimum Experience", "Minimum Experience:", "Minimum Experience -", "Minimum Experience :",
                "Experience Required", "Experience Required:", "Experience Required -", "Experience Required :",
                "Exp", "Exp:", "Exp -", "Exp :",
                "Exp.", "Exp.:", "Exp. -", "Exp. :",
                "Total Experience", "Total Experience:", "Total Experience -", "Total Experience :",
                "Years of Experience", "Years of Experience:", "Years of Experience -", "Years of Experience :",
                "Experience Level", "Experience Level:", "Experience Level -", "Experience Level :",
                "Prior Experience", "Prior Experience:", "Prior Experience -", "Prior Experience :",
                "Professional Experience", "Professional Experience:", "Professional Experience -", "Professional Experience :"
            ]:
                job_data["experience"] = value

            elif key in [
                "Job Location", "Job Posting Location", "Office", "Job Place",
                "Job Location:", "Job Location -", "Job Location :",
                "Location", "Location:", "Location -", "Location :",
                "Locations", "Locations:", "Locations -", "Locations :",
                "Work Location", "Work Location:", "Work Location -", "Work Location :",
                "Posting Location", "Posting Location:", "Posting Location -", "Posting Location :",
                "Place of Posting", "Place of Posting:", "Place of Posting -", "Place of Posting :",
                "Place", "Place:", "Place -", "Place :",
                "Job Locations", "Job Locations:", "Job Locations -", "Job Locations :",
                "Job Place", "Job Place:", "Job Place -", "Job Place :",
                "Workplace", "Workplace:", "Workplace -", "Workplace :",
                "Office Location", "Office Location:", "Office Location -", "Office Location :",
                "Duty Location", "Duty Location:", "Duty Location -", "Duty Location :"
            ]:
                job_data["location"] = value

    def normalize_label(txt: str) -> str:
        return re.sub(r'[\s:\-\u2013]+$', '', txt.strip(), flags=re.UNICODE).lower()

    label_to_field = {
        "job": "job-title",
        "job role": "job-title",
        "position": "job-title",
        "role": "job-title",
        "experience": "experience",
        "job location": "location",
        "location": "location",
    }

    for p in soup.find_all('p'):
        full_text = p.get_text(" ", strip=True)
        if not full_text:
            continue

        m = re.match(r'^([^:]+):\s*(.+)$', full_text)
        if m:
            label_raw, value = m.group(1), m.group(2)
        else:
            parts = full_text.split(None, 1)
            if len(parts) != 2:
                continue
            label_raw, value = parts[0], parts[1]

        label_norm = normalize_label(label_raw)
        if label_norm in label_to_field and value:
            field = label_to_field[label_norm]
            if not job_data.get(field):
                job_data[field] = value.strip()

    title_tag = soup.find(['h1', 'h2'])
    if title_tag:
        title_text = title_tag.get_text(strip=True)
        m_company = re.match(
            r'^(.+?)\s+(Walk-?in|Off\s*Campus|Off-Campus|Recruitment|Hiring|Jobs|Careers)\b',
            title_text,
            flags=re.IGNORECASE
        )
        if m_company and not job_data.get("company"):
            job_data["company"] = m_company.group(1).strip()

        m_role = re.search(
            r'\bas\s+([^|:]+?)(\s+with|\s+With|\s*\||$)',
            title_text,
            flags=re.IGNORECASE
        )
        if m_role and not job_data.get("job-title"):
            job_data["job-title"] = m_role.group(1).strip()

    description_parts = []
    about_label = soup.find(
        lambda tag: tag.name in ["strong", "b"]
        and "about company" in tag.get_text(strip=True).lower()
    )
    if about_label:
        about_p = about_label.find_next("p")
        if about_p:
            about_text = about_p.get_text(strip=True)
            if about_text:
                description_parts.append(about_text)

    description_section = soup.find(
        lambda tag: (
            tag.name == 'p'
            and tag.string
            and (
                any(
                    tag.string.strip().lower() == key.lower()
                    for key in [
                        "Key Responsibilities:", "job description", "Job Summary", "Job Summary:", "Opportunity Details",
                        "Details about Role", "Work Details", "Work Summary",
                        "job description:", "job description -", "description", "description:", "description -",
                        "about the job", "about the job:", "about the job -",
                        "about job", "about job:", "about job -",
                        "about the role", "about the role:", "about the role -",
                        "role description", "role description:", "role description -",
                        "position description", "position description:", "position description -",
                        "job overview", "job overview:", "job overview -",
                        "role overview", "role overview:", "role overview -",
                        "what you will do", "what you will do:", "what you will do -",
                        "responsibilities", "responsibilities:", "responsibilities -",
                        "duties", "duties:", "duties -",
                        "job responsibilities", "job responsibilities:", "job responsibilities -",
                        "position overview", "position overview:", "position overview -"
                    ]
                )
                or
                re.search(
                    r'\b('
                    r'job(s)?\s+description(s)?|'
                    r'job(s)?\s+summary|'
                    r'key\s+responsibilit(y|ies)|'
                    r'responsibilit(y|ies)|'
                    r'key\s+duties|'
                    r'duties\s+and\s+responsibilit(y|ies)|'
                    r'role\s+responsibilit(y|ies)|'
                    r'position\s+description|'
                    r'position\s+overview|'
                    r'about\s+(the\s+)?(job|role)|'
                    r'job\s+role|'
                    r'about\s+position|'
                    r'what\s+you(\'ll|\s+will)?\s+do|'
                    r'what\s+you\s+will\s+be\s+doing|'
                    r'your\s+role|'
                    r'overview\s+of\s+responsibilit(y|ies)|'
                    r'opportunity\s+details|'
                    r'details\s+about\s+(the\s+)?role|'
                    r'role\s+overview|'
                    r'work\s+(details|summary)|'
                    r'job\s+profile|'
                    r'position\s+profile|'
                    r'job\s+purpose|'
                    r'objective\s+of\s+the\s+role|'
                    r'mission\s+of\s+the\s+role|'
                    r'job\s+objective|'
                    r'position\s+objective|'
                    r'role\s+description|'
                    r'job\s+information|'
                    r'position\s+information|'
                    r'role\s+and\s+responsibilit(y|ies)|'
                    r'profile\s+description|'
                    r'functional\s+responsibilit(y|ies)|'
                    r'business\s+function\s+description|'
                    r'description\s+of\s+duties|'
                    r'description\s+of\s+role|'
                    r'career\s+summary|'
                    r'profile\s+summary|'
                    r'professional\s+summary|'
                    r'career\s+objective|'
                    r'job\s+functions'
                    r')',
                    tag.string.strip().lower()
                )
            )
        )
    )

    extra_desc = []
    if description_section:
        next_nodes = description_section.find_next_siblings()
        for node in next_nodes:
            if node.name == 'p':
                extra_desc.append(node.get_text(strip=True))
            elif node.name == 'ul':
                for li in node.find_all('li'):
                    extra_desc.append(li.get_text(strip=True))

    if extra_desc:
        description_parts.extend(extra_desc)

    if description_parts:
        job_data["desc"] = "\n\n".join(description_parts).split("Join our WhatsApp")[0]
    else:
        job_data["desc"] = "N/A"

    apply_link = None
    apply_label = soup.find(
        lambda tag: tag.name in ["strong", "b"]
        and "apply link" in tag.get_text(strip=True).lower()
    )
    if apply_label:
        a = apply_label.find_next("a")
        if a and a.has_attr("href"):
            apply_link = a["href"]

    if not apply_link:
        for a in soup.find_all("a"):
            if "click here" in a.get_text(strip=True).lower() and a.has_attr("href"):
                apply_link = a["href"]
                break

    job_data["apply-link"] = apply_link if apply_link else "N/A"

    job_data["moreInfoLink"] = url

    defaults = {
        "company": "N/A",
        "job-title": "N/A",
        "experience": "N/A",
        "location": "N/A",
        "apply-link": "N/A",
        "desc": "N/A",
    }
    for field, default_value in defaults.items():
        if not job_data.get(field):
            job_data[field] = default_value

    job_data["title"] = f"{job_data.get('company', 'N/A')} | {job_data.get('job-title', 'N/A')}"
    pretty_log_job(job_data)
    return job_data

# ===================== PROCESSED URL STORAGE =====================

def load_processed_urls() -> set:
    urls = set()
    if os.path.exists(PROCESSED_URLS_FILE):
        with open(PROCESSED_URLS_FILE, "r", encoding="utf-8") as f:
            for line in f:
                u = line.strip()
                if u:
                    urls.add(u)
    return urls

def append_processed_url(url: str):
    with open(PROCESSED_URLS_FILE, "a", encoding="utf-8") as f:
        f.write(url.strip() + "\n")

# ===================== TELEGRAM HELPERS =====================

async def resolve_target_dialog(client: TelegramClient):
    log("[TG] Resolving target dialog...")
    dialogs = await client.get_dialogs(limit=200)

    strong_match = None
    fuzzy_candidates = []

    for d in dialogs:
        name = d.name or ""
        if not name:
            continue

        lname = name.lower()
        if "fresher" in lname or "job" in lname:
            log(f"[TG] Dialog candidate: {name!r} (id={d.id}, is_group={d.is_group}, is_channel={d.is_channel})")

        if "fresher" in lname and "job" in lname and ("opening" in lname or "openings" in lname):
            strong_match = d
            break

        if "fresher" in lname and "job" in lname:
            fuzzy_candidates.append(d)

    if strong_match:
        log(f"[TG] Using strong match dialog: {strong_match.name!r} (id={strong_match.id})")
        return strong_match

    if len(fuzzy_candidates) == 1:
        chosen = fuzzy_candidates[0]
        log(f"[TG] Using single fuzzy candidate dialog: {chosen.name!r} (id={chosen.id})")
        return chosen

    if not fuzzy_candidates:
        log("[TG] No dialogs found containing both 'fresher' and 'job' in the title.")
    else:
        log("[TG] Multiple fuzzy candidates found. Please check scraper.log and adjust logic if needed.")

    return None

async def fetch_job_urls_from_group(client: TelegramClient, entity) -> list[str]:
    urls = set()
    N = 200

    log(f"[TG] Scanning last {N} messages from dialog: {getattr(entity, 'name', repr(entity))!r}")

    try:
        async for msg in client.iter_messages(entity, limit=N):
            if not msg.message:
                continue

            text = msg.message
            preview = text.replace("\n", " ")[:120]
            log(f"[TG] msg.id={msg.id}, date={msg.date}, preview={preview!r}")

            for match in URL_REGEX.findall(text):
                url = match.strip()
                log(f"[TG]   found URL: {url}")
                if any(domain in url for domain in TARGET_DOMAINS):
                    log(f"[TG]   -> accepted (matches target domains)")
                    urls.add(url)
                else:
                    log(f"[TG]   -> ignored (domain not in TARGET_DOMAINS)")
    except Exception as e:
        log(f"[TG] ERROR while iterating messages: {e}")
        log(traceback.format_exc())
        return []

    log(f"[TG] Total candidate URLs from group: {len(urls)}")
    return list(urls)

# ===================== DELETE OLD JOBS (>= 3 months) =====================

def delete_old_jobs(months: int = 1.5):
    """
    Delete jobs whose 'date-posted' is older than `months` months.
    Implementation uses 90 days as approximate 3 months.
    """
    cutoff = datetime.now() - timedelta(days=90 * months // 3 if months != 3 else 90)
    deleted = 0
    checked = 0
    try:
        log(f"[CLEANUP] Starting deletion of jobs older than {months} months (cutoff: {cutoff.strftime('%Y-%m-%d')})")
        docs = db.collection("Jobs").stream()
        for doc in docs:
            checked += 1
            data = doc.to_dict() or {}
            dp = data.get("date-posted", "").strip()
            if not dp:
                continue
            try:
                dt = datetime.strptime(dp, "%Y-%m-%d")
            except Exception:
                log(f"[CLEANUP] Skipping doc {doc.id}: unparsable date-posted='{dp}'")
                continue

            if dt < cutoff:
                try:
                    doc.reference.delete()
                    deleted += 1
                    log(f"[CLEANUP] Deleted doc {doc.id} date-posted={dp}")
                except Exception as e:
                    log(f"[CLEANUP] Failed to delete doc {doc.id}: {e}")
        log(f"[CLEANUP] Completed. Checked {checked} docs, deleted {deleted} old jobs.")
    except Exception as e:
        log(f"[CLEANUP] Exception during cleanup: {e}")
        log(traceback.format_exc())

# ===================== MAIN FLOW =====================

async def main():
    log("=== Telegram Session Setup ===")

    # If TG_SESSION_STRING is provided (GitHub / non-interactive)
    if TG_SESSION_STRING:
        log("Using TG_SESSION_STRING from environment (no OTP needed).")
        client = TelegramClient(StringSession(TG_SESSION_STRING), API_ID, API_HASH)
        await client.start()
    else:
        # Local behaviour with saved session file
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)

        if os.path.exists(SESSION_NAME + ".session") or os.path.exists(SESSION_NAME):
            log("Existing Telegram session found. Reusing it (no OTP needed).")
            await client.start()
        else:
            phone = input("Enter your Telegram phone number with country code (e.g. +91XXXXXXXXXX): ").strip()
            await client.start(phone=phone)

    me = await client.get_me()
    log(f"[TG] Logged in as: {me.first_name} (id={me.id})")

    # Run daily cleanup: delete jobs older than ~3 months (90 days)
    await asyncio.to_thread(delete_old_jobs, 3)

    # Resolve the correct dialog (group/channel)
    target_dialog = await resolve_target_dialog(client)
    if not target_dialog:
        log("[MAIN] Could not resolve target dialog. Check scraper.log for dialog list.")
        await client.disconnect()
        return

    # Read recent job URLs from group
    all_urls = await fetch_job_urls_from_group(client, target_dialog)
    await client.disconnect()

    if not all_urls:
        log("[MAIN] No job URLs found in recent messages.")
        return

    # Filter out already-processed URLs (per environment)
    processed = load_processed_urls()
    new_urls = [u for u in all_urls if u not in processed]

    log(f"[MAIN] {len(all_urls)} URLs found, {len(new_urls)} are new (not in processed_urls.txt).")

    if not new_urls:
        log("[MAIN] Nothing new to process. Exiting.")
        return

    # Scrape each new URL and upload to Firestore
    for url in new_urls:
        log(f"[MAIN] Processing URL: {url}")

        # 🔁 Firestore-based deduplication to avoid reposting
        if job_exists_for_url(url):
            log("  🔁 Job for this URL already exists in Firestore. Skipping.")
            append_processed_url(url)
            continue

        if "fresheropenings.com" in url:
            job_data = scrape_job_data_fresheropenings(url)
        elif "freshersrecruitment.co.in" in url:
            job_data = scrape_job_data_freshers_recruitment(url)
        else:
            log("  Skipping URL – unsupported domain (should not happen).")
            append_processed_url(url)
            continue

        if not job_data:
            log("  Scraping failed for this URL.")
            append_processed_url(url)
            continue

        company_val = (job_data.get("company") or "").strip()
        if not company_val or company_val.upper() == "N/A":
            log(f"  ⛔ Skipping posting because company is missing or 'N/A' (company='{company_val}').")
            append_processed_url(url)
            continue

        try:
            # Add to Firestore (this is when a new job is created)
            db.collection("Jobs").add(job_data)
            log("  ✅ Job data successfully added to Firestore.")

            # Trigger OneSignal notification only after successful Firestore write
            try:
                sent = send_onesignal_notification_for_job(job_data)
                if sent:
                    log("  🔔 Notification triggered for this new job.")
                else:
                    log("  ⚠️ Notification failed (see logs).")
            except Exception as e:
                log(f"  ⚠️ Exception while sending notification: {e}")
                log(traceback.format_exc())

            # Only mark URL as processed AFTER successful Firestore write
            append_processed_url(url)
        except Exception as e:
            log(f"  ❌ Failed to write to Firestore: {e}")
            log(traceback.format_exc())

# ===================== ENTRY POINT =====================

if __name__ == "__main__":
    try:
        log("=== Run started ===")
        asyncio.run(main())
        log("=== Run finished ===")
    except Exception as e:
        log(f"[MAIN] Unhandled exception: {e}")
        log(traceback.format_exc())
