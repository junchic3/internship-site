"""
Cloud scraper for GitHub Actions.
Fetches Summer 2026 internships and writes data/jobs.json
"""

import json
import re
import time
import os
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

OUT_FILE = Path(__file__).parent.parent / "data" / "jobs.json"
OUT_FILE.parent.mkdir(exist_ok=True)

# ── Known CPT/OPT sponsors ───────────────────────────────
KNOWN_SPONSORS = {
    "amazon","google","microsoft","meta","apple","netflix",
    "salesforce","oracle","adobe","nvidia","intel","qualcomm","ibm",
    "cisco","vmware","servicenow","workday","splunk","databricks",
    "snowflake","palantir","uber","lyft","airbnb","stripe","square",
    "goldman sachs","jp morgan","jpmorgan","morgan stanley",
    "bank of america","citigroup","citi","wells fargo","blackrock",
    "bloomberg","two sigma","jane street","citadel","jump trading",
    "de shaw","bridgewater","blackstone","apollo","american express",
    "visa","mastercard","capital one","deloitte","mckinsey","bain",
    "accenture","kpmg","pwc","ernst & young","ey","bcg","booz allen",
    "johnson & johnson","pfizer","merck","unitedhealth","cvs",
    "tesla","ford","general motors","boeing","lockheed martin",
    "att","verizon","comcast","disney","tiktok","bytedance",
}

USER_SKILLS = {
    "python":4,"pandas":3,"numpy":2,"sql":4,"mysql":3,"r":2,
    "tableau":4,"powerbi":4,"power bi":4,"excel":2,"aws":3,
    "sagemaker":3,"machine learning":4,"xgboost":3,"random forest":3,
    "time series":3,"forecasting":3,"mlflow":2,"data visualization":3,
    "statistics":2,"regression":2,"business analytics":4,
    "business intelligence":3,"data analysis":4,
}

TARGET_ROLES = [
    "data analyst","business analyst","data scientist","data science",
    "analytics","machine learning","ml engineer","business intelligence",
    "bi analyst","quantitative analyst","quant analyst","data engineer",
]

CPT_OPT_KEYWORDS = [
    "cpt","opt","f-1","f1 visa","visa sponsor","international student",
    "stem opt","work authorization",
]

US_SIGNALS = [
    " al"," ak"," az"," ar"," ca"," co"," ct"," de"," fl"," ga",
    " hi"," id"," il"," in"," ia"," ks"," ky"," la"," me"," md",
    " ma"," mi"," mn"," ms"," mo"," mt"," ne"," nv"," nh"," nj",
    " nm"," ny"," nc"," nd"," oh"," ok"," or"," pa"," ri"," sc",
    " sd"," tn"," tx"," ut"," vt"," va"," wa"," wv"," wi"," wy",
    "united states","usa","u.s.","remote","hybrid","new york",
    "san francisco","seattle","chicago","boston","austin","atlanta",
    "los angeles","san jose","denver","washington dc",
]

# ── Fetch ─────────────────────────────────────────────────
def fetch_simplify():
    urls = [
        "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/.github/scripts/listings.json",
        "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/main/.github/scripts/listings.json",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                jobs = []
                for item in r.json():
                    j = parse_item(item)
                    if j:
                        jobs.append(j)
                print(f"SimplifyJobs JSON: {len(jobs)} active")
                return jobs
        except Exception as e:
            print(f"JSON fetch failed: {e}")

    # Fallback: README
    return fetch_readme()

def parse_item(item):
    title   = item.get("title") or item.get("role") or ""
    company = item.get("company_name") or item.get("company") or ""
    locs    = item.get("locations") or item.get("location") or ""
    loc     = ", ".join(locs) if isinstance(locs, list) else str(locs)
    url     = item.get("url") or item.get("link") or ""
    active  = item.get("active", item.get("is_visible", True))
    sponsor = item.get("sponsorship", "")

    if not active or not title or not company:
        return None

    is_sp = (
        str(sponsor).lower() in ("sponsors", "yes", "true") or
        company.lower().strip() in KNOWN_SPONSORS
    )
    return {
        "title": title.strip(), "company": company.strip(),
        "location": loc.strip(), "url": url,
        "source": "SimplifyJobs", "is_sponsor": is_sp,
        "description": item.get("description", ""),
        "date_posted": str(item.get("date_posted", "")),
    }

def fetch_readme():
    jobs = []
    try:
        r = requests.get(
            "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/README.md",
            timeout=20,
        )
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line.startswith("|") or "---" in line:
                continue
            if "🔒" in line:
                continue
            cells = [c.strip() for c in line.split("|")[1:-1]]
            if len(cells) < 3:
                continue

            def strip_md(s):
                return re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", s).strip()

            company  = strip_md(cells[0])
            role     = strip_md(cells[1]) if len(cells) > 1 else ""
            location = strip_md(cells[2]) if len(cells) > 2 else ""
            if not company or not role or company.lower() in ("company",""):
                continue

            url_m = re.search(r"\((https?://[^)]+)\)", cells[1] if len(cells) > 1 else "")
            jobs.append({
                "title": role, "company": company, "location": location,
                "url": url_m.group(1) if url_m else "",
                "source": "SimplifyJobs",
                "is_sponsor": company.lower().strip() in KNOWN_SPONSORS,
                "description": "", "date_posted": "",
            })
    except Exception as e:
        print(f"README parse failed: {e}")
    print(f"SimplifyJobs README: {len(jobs)}")
    return jobs

def fetch_jsearch():
    api_key = os.environ.get("RAPIDAPI_KEY", "")
    if not api_key:
        return []
    jobs = []
    queries = ["data analyst intern summer 2026", "business analyst intern summer 2026"]
    for q in queries:
        try:
            r = requests.get(
                "https://jsearch.p.rapidapi.com/search",
                headers={"x-rapidapi-key": api_key, "x-rapidapi-host": "jsearch.p.rapidapi.com"},
                params={"query": f"{q} CPT OPT", "country": "us",
                        "employment_types": "INTERN", "date_posted": "month", "num_pages": "1"},
                timeout=20,
            )
            if r.status_code == 200:
                for item in r.json().get("data", []):
                    company = item.get("employer_name", "")
                    jobs.append({
                        "title": item.get("job_title", ""),
                        "company": company,
                        "location": f"{item.get('job_city','')}, {item.get('job_state','')}",
                        "url": item.get("job_apply_link", ""),
                        "source": "JSearch", "date_posted": "",
                        "is_sponsor": company.lower().strip() in KNOWN_SPONSORS,
                        "description": (item.get("job_description") or "")[:500],
                    })
            time.sleep(0.5)
        except Exception as e:
            print(f"JSearch error: {e}")
    return jobs

# ── Filter & Score ────────────────────────────────────────
def is_us(loc):
    l = loc.lower()
    return (not loc) or any(s.strip() in l for s in US_SIGNALS)

def is_relevant(title):
    t = title.lower()
    return any(k in t for k in [
        "data","analyst","analytics","science","machine learning","ml",
        "ai","intelligence","quantitative","quant","statistical",
        "engineer","business","intern","bi",
    ])

def score(job):
    s    = 0
    text = (job.get("title","") + " " + job.get("description","")).lower()
    for role in TARGET_ROLES:
        if role in text: s += 15
    for skill, w in USER_SKILLS.items():
        if skill in text: s += w
    for kw in CPT_OPT_KEYWORDS:
        if kw in text: s += 8; break
    if job.get("is_sponsor"): s += 20
    if "summer" in text or "2026" in text: s += 5
    return min(s, 100)

def filter_rank(jobs, n=50):
    jobs = [j for j in jobs if is_us(j.get("location",""))]
    jobs = [j for j in jobs if is_relevant(j.get("title",""))]
    seen, unique = set(), []
    for j in jobs:
        k = (j["company"].lower()[:20], j["title"].lower()[:20])
        if k not in seen:
            seen.add(k); unique.append(j)
    for j in unique:
        j["score"] = score(j)
    # Take top 100 candidates, validate URLs, then return top n
    candidates = sorted(unique, key=lambda x: x["score"], reverse=True)[:100]
    valid = validate_urls(candidates)
    return valid[:n]

# ── URL Validation ────────────────────────────────────────
def check_url(url, timeout=6):
    """Return True if URL is valid (not 404). Keep jobs with no URL."""
    if not url:
        return True
    try:
        r = requests.head(
            url, timeout=timeout, allow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        )
        if r.status_code == 405:
            # HEAD not allowed — try GET with stream to avoid downloading body
            r = requests.get(
                url, timeout=timeout, allow_redirects=True, stream=True,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
            )
            r.close()
        return r.status_code != 404
    except Exception:
        return True  # Network/timeout error ≠ 404, keep the job

def validate_urls(jobs, max_workers=15):
    """Parallel URL validation. Filters confirmed 404s, keeps everything else."""
    print(f"Validating {len(jobs)} URLs (parallel, {max_workers} workers)…")
    valid = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(check_url, j.get("url","")): j for j in jobs}
        for future in as_completed(future_map):
            job = future_map[future]
            if future.result():
                valid.append(job)
            else:
                print(f"  [404 removed] {job.get('company')} - {job.get('title')}")
    # Re-sort by score (threading loses order)
    valid.sort(key=lambda x: x.get("score", 0), reverse=True)
    print(f"  {len(valid)} valid jobs after URL check")
    return valid

# ── Main ──────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Starting fetch: {datetime.now().isoformat()}")
    all_jobs = fetch_simplify() + fetch_jsearch()
    print(f"Total fetched: {len(all_jobs)}")

    top = filter_rank(all_jobs, n=50)
    print(f"Top {len(top)} selected")

    OUT_FILE.write_text(json.dumps(top, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved to {OUT_FILE}")
