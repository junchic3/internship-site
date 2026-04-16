"""
Cloud scraper for GitHub Actions.
Sources: SimplifyJobs + LinkedIn + Indeed (via python-jobspy) + JSearch (optional)
Features: URL validation, PhD filter, Canada filter, new-job tagging
"""

import json, re, time, os, requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

OUT_FILE  = Path(__file__).parent.parent / "data" / "jobs.json"
PREV_FILE = Path(__file__).parent.parent / "data" / "jobs_prev.json"
OUT_FILE.parent.mkdir(exist_ok=True)

# ── Known CPT/OPT sponsors ────────────────────────────────
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
    "att","verizon","comcast","disney","linkedin","indeed",
    "jpmorgan chase","wells fargo","synchrony","ge","general electric",
}

# ── Excluded companies ────────────────────────────────────
EXCLUDED_COMPANIES = {
    "tiktok",
    "bytedance",
}

# ── User skills for scoring ───────────────────────────────
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
    "los angeles","san jose","denver","washington dc","raleigh",
    "charlotte","dallas","houston","phoenix","minneapolis","new jersey",
]

CANADA_SIGNALS = [
    "canada"," on"," bc"," qc"," ab"," mb"," sk"," ns"," nb",
    "ontario","british columbia","quebec","alberta","manitoba",
    "toronto","vancouver","montreal","calgary","ottawa","edmonton",
    "winnipeg","waterloo","kitchener","mississauga","brampton",
]

PHD_EXCLUDE = [
    "phd","ph.d","ph d","doctoral","postdoc","post-doc",
    "research scientist","principal scientist","staff scientist",
    "- phd","phd -","(phd)","phd)",
]

# ── Dead-page phrases (content check) ────────────────────
DEAD_PHRASES = [
    "page you are looking for doesn't exist",
    "page you're looking for doesn't exist",
    "this job is no longer available",
    "this position is no longer available",
    "job listing is no longer","job posting is no longer",
    "posting has been removed","posting has expired",
    "position has been filled","no longer accepting applications",
    "application is closed","this job has expired",
    "job has been filled","requisition is no longer",
    "this role is no longer","page not found","404 not found",
    "job is closed","posting is closed","position is closed",
    "this job has been closed","this position has been closed",
    "job opportunity is no longer","sorry, this job",
    "listing has expired","this listing is no longer",
    "opening is no longer","opening has been filled",
    "we're sorry, this position","this role has been filled",
]

JS_RENDERED_PLATFORMS = [
    "myworkdayjobs.com","taleo.net","successfactors.com",
    "icims.com","smartrecruiters.com","jobvite.com",
    "brassring.com","oraclecloud.com","ultipro.com",
    "adp.com","kronos.com","lever.co",
]

SEARCH_QUERIES = [
    "data analyst intern summer 2026",
    "business analyst intern summer 2026",
    "data science intern summer 2026",
    "analytics intern summer 2026",
    "machine learning intern summer 2026",
    "business intelligence intern summer 2026",
    "quantitative analyst intern summer 2026",
]

# ── Source 1: SimplifyJobs ────────────────────────────────
def fetch_simplify():
    urls = [
        "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/.github/scripts/listings.json",
        "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/main/.github/scripts/listings.json",
    ]
    for url in urls:
        try:
            r = requests.get(url, timeout=20)
            if r.status_code == 200:
                jobs = [j for item in r.json() if (j := _parse_simplify(item))]
                print(f"SimplifyJobs: {len(jobs)} active")
                return jobs
        except Exception as e:
            print(f"SimplifyJobs JSON failed: {e}")
    return _fetch_simplify_readme()

def _parse_simplify(item):
    title   = item.get("title") or item.get("role") or ""
    company = item.get("company_name") or item.get("company") or ""
    locs    = item.get("locations") or item.get("location") or ""
    loc     = ", ".join(locs) if isinstance(locs, list) else str(locs)
    url     = item.get("url") or item.get("link") or ""
    active  = item.get("active", item.get("is_visible", True))
    sponsor = item.get("sponsorship", "")
    if not active or not title or not company:
        return None
    is_sp = str(sponsor).lower() in ("sponsors","yes","true") or \
            company.lower().strip() in KNOWN_SPONSORS
    return {
        "title": title.strip(), "company": company.strip(),
        "location": loc.strip(), "url": url,
        "source": "SimplifyJobs", "is_sponsor": is_sp,
        "description": item.get("description",""),
        "date_posted": str(item.get("date_posted","")),
    }

def _fetch_simplify_readme():
    jobs = []
    try:
        r = requests.get(
            "https://raw.githubusercontent.com/SimplifyJobs/Summer2026-Internships/dev/README.md",
            timeout=20)
        r.raise_for_status()
        for line in r.text.splitlines():
            if not line.startswith("|") or "---" in line or "🔒" in line:
                continue
            cells = [c.strip() for c in line.split("|")[1:-1]]
            if len(cells) < 3:
                continue
            def strip_md(s):
                return re.sub(r"\[([^\]]+)\]\([^\)]+\)", r"\1", s).strip()
            company = strip_md(cells[0])
            role    = strip_md(cells[1]) if len(cells) > 1 else ""
            loc     = strip_md(cells[2]) if len(cells) > 2 else ""
            if not company or not role or company.lower() == "company":
                continue
            url_m = re.search(r"\((https?://[^)]+)\)", cells[1] if len(cells) > 1 else "")
            jobs.append({
                "title": role, "company": company, "location": loc,
                "url": url_m.group(1) if url_m else "",
                "source": "SimplifyJobs",
                "is_sponsor": company.lower().strip() in KNOWN_SPONSORS,
                "description": "", "date_posted": "",
            })
    except Exception as e:
        print(f"SimplifyJobs README failed: {e}")
    print(f"SimplifyJobs README: {len(jobs)}")
    return jobs

# ── Source 2: LinkedIn + Indeed via python-jobspy ─────────
def fetch_jobspy():
    try:
        from jobspy import scrape_jobs
    except ImportError:
        print("python-jobspy not installed, skipping LinkedIn/Indeed")
        return []

    jobs = []
    queries = [
        "data analyst intern 2026 CPT OPT",
        "business analyst intern 2026 CPT OPT",
        "data science intern 2026 CPT OPT",
        "analytics intern 2026 CPT OPT",
    ]
    sites = ["linkedin", "indeed", "glassdoor"]

    for query in queries:
        try:
            df = scrape_jobs(
                site_name=sites,
                search_term=query,
                location="United States",
                results_wanted=15,
                job_type="internship",
                hours_old=168,         # last 7 days only
                country_indeed="USA",
            )
            if df is not None and not df.empty:
                for _, row in df.iterrows():
                    company = str(row.get("company") or "")
                    desc    = str(row.get("description") or "")[:600]
                    jobs.append({
                        "title":       str(row.get("title") or ""),
                        "company":     company,
                        "location":    str(row.get("location") or ""),
                        "url":         str(row.get("job_url") or ""),
                        "source":      str(row.get("site") or "jobspy").title(),
                        "is_sponsor":  company.lower().strip() in KNOWN_SPONSORS,
                        "description": desc,
                        "date_posted": str(row.get("date_posted") or ""),
                    })
            time.sleep(2)   # be polite
        except Exception as e:
            print(f"jobspy error for '{query}': {e}")

    print(f"LinkedIn/Indeed/Glassdoor (jobspy): {len(jobs)}")
    return jobs

# ── Source 3: JSearch (optional) ──────────────────────────
def fetch_jsearch():
    api_key = os.environ.get("RAPIDAPI_KEY","")
    if not api_key:
        return []
    jobs = []
    for query in SEARCH_QUERIES[:3]:
        try:
            r = requests.get(
                "https://jsearch.p.rapidapi.com/search",
                headers={"x-rapidapi-key": api_key,
                         "x-rapidapi-host": "jsearch.p.rapidapi.com"},
                params={"query": f"{query} CPT OPT", "country":"us",
                        "employment_types":"INTERN","date_posted":"month","num_pages":"1"},
                timeout=20)
            if r.status_code == 200:
                for item in r.json().get("data",[]):
                    company = item.get("employer_name","")
                    jobs.append({
                        "title":    item.get("job_title",""),
                        "company":  company,
                        "location": f"{item.get('job_city','')}, {item.get('job_state','')}",
                        "url":      item.get("job_apply_link",""),
                        "source":   "JSearch",
                        "is_sponsor": company.lower().strip() in KNOWN_SPONSORS,
                        "description": (item.get("job_description") or "")[:500],
                        "date_posted": str(item.get("job_posted_at_datetime_utc","")),
                    })
            time.sleep(0.5)
        except Exception as e:
            print(f"JSearch error: {e}")
    print(f"JSearch: {len(jobs)}")
    return jobs

# ── Filter & Score ────────────────────────────────────────
def is_us(loc):
    if not loc: return True
    l = loc.lower()
    if any(c in l for c in CANADA_SIGNALS): return False
    return any(s.strip() in l for s in US_SIGNALS)

def is_relevant(title):
    t = title.lower()
    return any(k in t for k in [
        "data","analyst","analytics","science","machine learning","ml",
        "ai","intelligence","quantitative","quant","statistical",
        "engineer","business","intern","bi",
    ])

def is_not_phd(title):
    t = title.lower()
    return not any(k in t for k in PHD_EXCLUDE)

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
    jobs = [j for j in jobs if is_not_phd(j.get("title",""))]
    jobs = [j for j in jobs if j.get("company","").lower().strip() not in EXCLUDED_COMPANIES]
    # Deduplicate
    seen, unique = set(), []
    for j in jobs:
        k = (j["company"].lower()[:20], j["title"].lower()[:20])
        if k not in seen:
            seen.add(k); unique.append(j)
    for j in unique:
        j["score"] = score(j)
    candidates = sorted(unique, key=lambda x: x["score"], reverse=True)[:120]
    valid = validate_urls(candidates)
    return valid[:n]

# ── URL Validation ────────────────────────────────────────
def _check_workday(url, timeout=10):
    m = re.match(
        r"https?://([^.]+)(\.wd\d+\.myworkdayjobs\.com)(?:/[a-z-]+)?/([^/]+)/job/(.+)",
        url)
    if not m: return None
    company, domain, site, rest = m.groups()
    api = f"https://{company}{domain}/wday/cxs/{company}/{site}/job/{rest}"
    try:
        r = requests.get(api, timeout=timeout,
                         headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"})
        if r.status_code == 404: return False
        if r.status_code == 200:
            try:
                d = r.json()
                if "jobPostingInfo" in d or "title" in d: return True
                if "error" in d: return False
            except: pass
        return None
    except: return None

def _check_greenhouse(url, timeout=8):
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True,
                          headers={"User-Agent":"Mozilla/5.0"})
        return False if r.status_code == 404 else True
    except: return None

def check_url(url, timeout=10):
    if not url: return True, False
    hdrs = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

    # JS-rendered platforms
    for platform in JS_RENDERED_PLATFORMS:
        if platform in url:
            if "myworkdayjobs.com" in url:
                result = _check_workday(url, timeout)
                if result is False: return False, True
            return True, False   # assume live, unverified

    # Greenhouse
    if "greenhouse.io" in url:
        result = _check_greenhouse(url, timeout)
        if result is False: return False, True
        return True, True

    # Lever — returns 404 properly
    if "lever.co" in url:
        try:
            r = requests.head(url, timeout=timeout, allow_redirects=True, headers=hdrs)
            return (False, True) if r.status_code == 404 else (True, True)
        except: return True, False

    # Generic: HTTP + content check (read up to 30 KB)
    try:
        with requests.get(url, timeout=timeout, allow_redirects=True,
                          stream=True, headers=hdrs) as r:
            if r.status_code == 404: return False, True
            chunk = b""
            for block in r.iter_content(chunk_size=2048):
                chunk += block
                if len(chunk) >= 30720: break
        text = chunk.decode("utf-8", errors="ignore").lower()
        for phrase in DEAD_PHRASES:
            if phrase in text: return False, True
        return True, True
    except: return True, False

def validate_urls(jobs, max_workers=15):
    print(f"Validating {len(jobs)} URLs (parallel, {max_workers} workers)...")
    valid = []
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_map = {ex.submit(check_url, j.get("url","")): j for j in jobs}
        for future in as_completed(future_map):
            job = future_map[future]
            is_live, is_verified = future.result()
            if is_live:
                job["url_verified"] = is_verified
                valid.append(job)
            else:
                print(f"  [DEAD] {job.get('company')} - {job.get('title')}")
    valid.sort(key=lambda x: x.get("score",0), reverse=True)
    v = sum(1 for j in valid if j.get("url_verified"))
    print(f"  {len(valid)} valid ({v} verified, {len(valid)-v} unverified)")
    return valid

# ── New-job tagging ───────────────────────────────────────
def tag_new_jobs(jobs):
    """Compare with previous run; mark jobs that are newly appearing today."""
    prev_keys = set()
    if PREV_FILE.exists():
        try:
            prev = json.loads(PREV_FILE.read_text(encoding="utf-8"))
            prev_keys = {(j.get("company","").lower(), j.get("title","").lower())
                         for j in prev}
        except Exception:
            pass

    new_count = 0
    for j in jobs:
        key = (j.get("company","").lower(), j.get("title","").lower())
        j["is_new"] = key not in prev_keys
        if j["is_new"]:
            new_count += 1

    print(f"New jobs today: {new_count}")
    return jobs

# ── Main ──────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Starting fetch: {datetime.now().isoformat()}")

    all_jobs = []
    all_jobs += fetch_simplify()
    all_jobs += fetch_jobspy()
    all_jobs += fetch_jsearch()
    print(f"Total raw: {len(all_jobs)}")

    top = filter_rank(all_jobs, n=50)
    top = tag_new_jobs(top)
    print(f"Final: {len(top)} jobs, {sum(1 for j in top if j['is_new'])} new")

    # Rotate: save current as prev before overwriting
    if OUT_FILE.exists():
        PREV_FILE.write_text(OUT_FILE.read_text(encoding="utf-8"), encoding="utf-8")

    OUT_FILE.write_text(json.dumps(top, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Saved to {OUT_FILE}")
