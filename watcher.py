"""Job watcher: polls job sources and sends ntfy push alerts for new matching roles.

Sources:
  1. SimplifyJobs new-grad + Spring 2027 internship lists (true posted time, sponsorship flag)
  2. Company career boards straight from their ATS (Greenhouse, Lever, Ashby, Workday, Amazon) - see companies.json
  3. Optional: JSearch API (Google for Jobs, which aggregates Indeed, Glassdoor, ZipRecruiter, LinkedIn...)
     Enabled only if JSEARCH_KEY is set.

Env vars:
  NTFY_TOPIC      required to send alerts (keep secret)
  NTFY_SERVER     default https://ntfy.sh
  MAX_AGE_H       hard cap on posting age for every source (default 12)
  ATS_STRICT      1 (default): career-board roles must look early-career (new grad, intern, university, I/1, junior...)
                  0: alert on any non-senior SWE/ML role from the boards (noisier)
  JSEARCH_KEY     optional RapidAPI key for JSearch
  JSEARCH_EVERY_H how often to call JSearch (default 4, keeps a free tier alive)
  DRY_RUN=1       print alerts instead of sending
Auto-discovery: every company that has EVER appeared on the Simplify lists with a Greenhouse, Lever,
Ashby or Workday link is added to the watch list automatically (thousands of boards), so a company not
in companies.json is still watched directly once it shows up anywhere in Simplify's data.
  - "hot" boards (companies.json + companies with a tech posting in the last HOT_DAYS days) every run
  - all other discovered boards (and all Workday boards) in rotating slices, each checked about hourly
State: state.json (kept in the GitHub Actions cache). The first time a board is seen, its current
jobs are recorded silently so you only get alerts for roles posted after that.
"""
import json, os, re, sys, time, urllib.parse, urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, "state.json")
CONF = os.path.join(HERE, "companies.json")
MAX_AGE_H = float(os.environ.get("MAX_AGE_H", "12"))   # never alert on anything older than this
ATS_STRICT = os.environ.get("ATS_STRICT", "1") != "0"
COOP_TERMS = set(t.strip() for t in os.environ.get("COOP_TERMS", "Spring 2027,Winter 2027").split(","))
# career-page internship titles that name a term other than spring/winter get skipped
OFF_TERM = ("summer", "fall", "autumn")
HOT_DAYS = float(os.environ.get("HOT_DAYS", "60"))
SLICES = int(os.environ.get("SLICES", "6"))          # cold boards: each checked once every SLICES runs
DISCOVER_DAYS = float(os.environ.get("DISCOVER_DAYS", "365"))
THREADS = int(os.environ.get("THREADS", "32"))

NG = "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/.github/scripts/listings.json"
IN = "https://raw.githubusercontent.com/SimplifyJobs/Summer2027-Internships/dev/.github/scripts/listings.json"
CATS = {"Software", "Software Engineering", "AI/ML/Data", "Data Science, AI & Machine Learning"}
BAD_SPONSOR = {"Does Not Offer Sponsorship", "U.S. Citizenship is Required"}

SENIOR = ("senior", "sr.", "sr ", "staff", "principal", "lead", "manager", " ii", " iii", " iv", "director",
          "phd", "head of", "architect", "distinguished", "level 2", "vp")
SENIOR_RE = re.compile(r"\b(ii|iii|iv|2|3|4|l[4-9]|e[4-9])\b")
BLOCK = ("analyst", "technician", "annotat", "operator", "evaluator", "coordinator", "generalist", "curation",
         "bilingual", "language", "clearance", "ts/sci", "secret", "us person", "u.s. person", "recruit",
         "sales", "account", "support engineer", "solutions engineer", "customer", "marketing")
ROLE = ("engineer", "developer", "software", "scientist", "machine learning", "ml ", "ai ", "sde", "swe", "data")
EARLY = ("new grad", "new graduate", "graduate", "university", "intern", "co-op", "coop", "early career",
         "entry", "junior", "associate", "apprentice", "resident", "fellow", "2026", "2027", "level 1")
EARLY_RE = re.compile(r"\b(i|1|l3|e3)\b")
NO_SPONSOR_COS = ("lockheed", "northrop", "raytheon", "rtx", "general dynamics", "huntington ingalls",
                  "l3harris", "bae systems", "leidos", "saic", "booz allen", "caci", "anduril", "palantir",
                  "mitre", "sandia", "lawrence livermore", "los alamos", "boeing", "textron", "peraton",
                  "parsons", "mantech", "johns hopkins applied physics", "aerospace corporation",
                  "sierra nevada", "shield ai", "noblis", "kbr", "amentum", "spacex", "u.s. army", "u.s. navy")
HOT = ("machine learning", "ml ", "ai ", "llm", "infra", "gpu", "cuda", "research", "vision", "backend",
       "platform", "genai", "applied", "inference", "training", "kernel")

STATES = set("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH "
             "NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split())
US_CITIES = ("san francisco", "new york", "nyc", "seattle", "bellevue", "mountain view", "palo alto",
             "sunnyvale", "san jose", "los angeles", "austin", "boston", "chicago", "denver", "atlanta",
             "redmond", "menlo park", "santa clara", "san diego", "washington", "pittsburgh", "cambridge, ma")
NON_US = ("canada", "toronto", "vancouver", "london", "united kingdom", "uk", "india", "bangalore", "bengaluru",
          "hyderabad", "dublin", "ireland", "germany", "berlin", "munich", "paris", "france", "amsterdam",
          "netherlands", "singapore", "tokyo", "japan", "sydney", "australia", "israel", "tel aviv", "brazil",
          "mexico", "poland", "warsaw", "spain", "madrid", "zurich", "switzerland", "korea", "seoul", "china",
          "emea", "apac", "latam", "costa rica", "argentina", "romania", "serbia", "czech", "taiwan")


def http_json(url, data=None, headers=None, timeout=30):
    h = {"User-Agent": "Mozilla/5.0 job-watcher", "Accept": "application/json"}
    h.update(headers or {})
    body = json.dumps(data).encode() if data is not None else None
    if body is not None:
        h["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=body, headers=h)
    return json.load(urllib.request.urlopen(req, timeout=timeout))


def is_us(loc):
    low = (loc or "").lower()
    if not low.strip():
        return True  # unknown location: let it through
    parts = [p.strip() for chunk in re.split(r"[;|/]", loc) for p in [chunk]]
    for p in parts:
        pl = p.lower()
        if any(re.search(rf"\b{re.escape(n)}\b", pl) for n in NON_US) and "united states" not in pl:
            continue
        if re.search(r"\b(united states|usa|u\.s\.|us)\b", pl) or any(c in pl for c in US_CITIES):
            return True
        if re.search(r",\s*([A-Z]{2})\b", p) and re.search(r",\s*([A-Z]{2})\b", p).group(1) in STATES:
            return True
        if pl.startswith("remote") and not any(re.search(rf"\b{re.escape(n)}\b", pl) for n in NON_US):
            return True
    return False


def title_ok(title, strict):
    t = " " + title.lower() + " "
    if any(s in t for s in SENIOR) or (SENIOR_RE.search(t) and "intern" not in t) or any(b in t for b in BLOCK) or not any(k in t for k in ROLE):
        return False
    return (not strict) or any(e in t for e in EARLY) or bool(EARLY_RE.search(t))


# ---------- discovery from Simplify urls ----------

ATS_PATTERNS = [
    ("gh", re.compile(r"greenhouse\.io/(?:embed/job_app\?for=)?([A-Za-z0-9_-]+)")),
    ("lv", re.compile(r"jobs\.lever\.co/([A-Za-z0-9_.-]+)")),
    ("ab", re.compile(r"jobs\.ashbyhq\.com/([A-Za-z0-9_.%-]+)")),
    ("wd", re.compile(r"https?://([a-z0-9-]+)\.(wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?([A-Za-z0-9_-]+)")),
]
GH_SKIP = {"embed", "v1", "job_app"}


def board_from_url(url):
    for kind, pat in ATS_PATTERNS:
        m = pat.search(url or "")
        if not m:
            continue
        if kind == "wd":
            tenant, wd, site = m.groups()
            if site.lower() in ("job", "jobs", "details"):
                return None
            return f"wd:{tenant}:{site}", {"host": f"{tenant}.{wd}.myworkdayjobs.com", "tenant": tenant, "site": site}
        slug = m.group(1)
        if kind == "gh" and slug in GH_SKIP:
            return None
        return f"{kind}:{slug.lower()}", slug
    return None


def boards_from(conf, listings, now):
    """Return {key: (kind, arg, company_name, hot)}."""
    boards = {}
    for kind in ("greenhouse", "lever", "ashby"):
        k2 = {"greenhouse": "gh", "lever": "lv", "ashby": "ab"}[kind]
        for slug in conf.get(kind, []):
            boards[f"{k2}:{slug.lower()}"] = (k2, slug, slug.title(), True)
    for w in conf.get("workday", []):
        boards[f"wd:{w['tenant']}:{w['site']}"] = ("wd", w, w["name"], True)
    for x in listings:
        age_d = (now - x.get("date_posted", 0)) / 86400
        if age_d > DISCOVER_DAYS or x.get("category") not in CATS:
            continue
        b = board_from_url(x.get("url"))
        if not b:
            continue
        key, arg = b
        hot = age_d <= HOT_DAYS and not key.startswith("wd:")
        if key in boards:
            k, a, n, h = boards[key]
            boards[key] = (k, a, n, h or hot)
        else:
            boards[key] = (key[:2], arg, x["company_name"], hot)
    return boards


# ---------- sources ----------

def fetch_simplify():
    out = []
    for track, url in (("New grad", NG), ("Intern", IN)):
        for x in http_json(url, timeout=60):
            x["_track"] = track
            out.append(x)
    return out


def simplify_matches(listings, now):
    out = []
    for x in listings:
        track = x["_track"]
        if not x.get("active") or not x.get("is_visible", True):
            continue
        if (now - x.get("date_posted", 0)) / 3600 > MAX_AGE_H:
            continue
        if x.get("category") not in CATS or x.get("sponsorship") in BAD_SPONSOR:
            continue
        if track == "Intern":
            terms = set(x.get("terms") or [])
            t = x.get("title", "").lower()
            # spring co-ops are listed as "Spring 2027" or "Winter 2027" (Jan start); also co-ops with no term
            if not (terms & COOP_TERMS or (("co-op" in t or "coop" in t) and terms <= {"N/A"})):
                continue
        locs = x.get("locations") or []
        if not title_ok(x.get("title", ""), strict=False) or not any(is_us(l) for l in locs):
            continue
        if any(c in x["company_name"].lower() for c in NO_SPONSOR_COS):
            continue
        out.append({"id": x["id"], "company": x["company_name"], "role": x["title"],
                    "location": "; ".join(locs), "url": x["url"], "track": track + " (Simplify)",
                    "note": f"posted {int((now - x['date_posted']) / 60)} min ago · sponsorship: {x.get('sponsorship') or 'unknown'}"})
    return out


def iso_ts(v):
    if not v:
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def day_ts(v):
    """'November 1, 2026' -> end of that day (UTC), so a same-day posting counts as fresh."""
    try:
        from datetime import datetime, timezone
        return datetime.strptime(v.strip(), "%B %d, %Y").replace(tzinfo=timezone.utc).timestamp() + 86399
    except Exception:
        return None


def age_note(ts, now, found):
    if ts:
        return f"posted {int(max(0, now - ts) / 60)} min ago"
    return f"appeared within the last {found}"


def src_greenhouse(slug):
    d = http_json(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs")
    return [{"id": str(j["id"]), "role": j["title"], "location": (j.get("location") or {}).get("name", ""),
             "url": j["absolute_url"], "ts": iso_ts(j.get("first_published") or j.get("updated_at"))}
            for j in d.get("jobs", [])]


def src_lever(slug):
    d = http_json(f"https://api.lever.co/v0/postings/{slug}?mode=json")
    out = []
    for j in d:
        c = j.get("categories") or {}
        out.append({"id": j["id"], "role": j["text"], "url": j["hostedUrl"],
                    "ts": (j.get("createdAt") or 0) / 1000 or None,
                    "location": "; ".join(c.get("allLocations") or [c.get("location", "")])})
    return out


def src_ashby(slug):
    d = http_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
    out = []
    for j in d.get("jobs", []):
        if j.get("isListed") is False:
            continue
        locs = [j.get("location", "")] + [s.get("location", "") for s in j.get("secondaryLocations") or []]
        out.append({"id": j["id"], "role": j["title"], "location": "; ".join(l for l in locs if l),
                    "url": j.get("jobUrl") or j.get("applyUrl"), "ts": iso_ts(j.get("publishedAt"))})
    return out


def src_workday(w):
    base = f"https://{w['host']}/wday/cxs/{w['tenant']}/{w['site']}"
    out, seen = [], set()
    for q in ("software engineer", "machine learning", "intern", "new grad"):
        d = http_json(base + "/jobs", data={"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": q})
        for j in d.get("jobPostings", []):
            p = j.get("externalPath", "")
            if not p or p in seen:
                continue
            seen.add(p)
            posted = (j.get("postedOn") or "").lower()
            out.append({"id": p.rsplit("_", 1)[-1], "role": j.get("title", ""), "location": j.get("locationsText", ""),
                        "url": f"https://{w['host']}/en-US/{w['site']}{p}",
                        # Workday only says "Posted Today" / "Posted Yesterday" / "Posted 3 Days Ago"
                        "ts": None if ("today" in posted or "just" in posted or not posted) else 0})
    return out


def src_amazon(_):
    out = []
    for q in ("software development engineer", "machine learning", "applied scientist intern", "new grad"):
        u = "https://www.amazon.jobs/en/search.json?" + urllib.parse.urlencode(
            {"base_query": q, "sort": "recent", "country": "USA", "result_limit": 50})
        for j in http_json(u).get("jobs", []):
            out.append({"id": str(j.get("id_icims") or j.get("id")), "role": j.get("title", ""),
                        "location": j.get("normalized_location") or j.get("location", ""),
                        "url": "https://www.amazon.jobs" + j.get("job_path", ""),
                        "ts": day_ts(j.get("posted_date"))})
    return out


FETCH = {"gh": src_greenhouse, "lv": src_lever, "ab": src_ashby, "wd": src_workday, "am": src_amazon}


def src_jsearch(now, key):
    out = []
    for q in ("new grad software engineer", "machine learning engineer intern", "entry level machine learning engineer"):
        u = "https://jsearch.p.rapidapi.com/search?" + urllib.parse.urlencode(
            {"query": q + " in United States", "date_posted": "today", "num_pages": 1})
        d = http_json(u, headers={"X-RapidAPI-Key": key, "X-RapidAPI-Host": "jsearch.p.rapidapi.com"})
        for j in d.get("data", []):
            ts = j.get("job_posted_at_timestamp") or 0
            if ts and (now - ts) / 3600 > MAX_AGE_H:
                continue
            loc = ", ".join(x for x in (j.get("job_city"), j.get("job_state"), j.get("job_country")) if x)
            out.append({"id": j["job_id"], "company": j.get("employer_name", ""), "role": j.get("job_title", ""),
                        "location": loc, "url": j.get("job_apply_link") or j.get("job_google_link", ""),
                        "track": f"via {j.get('job_publisher', 'job site')}",
                        "note": f"posted {int((now - ts) / 60)} min ago" if ts else ""})
    return out


# ---------- main ----------

def notify(job):
    topic = os.environ.get("NTFY_TOPIC")
    hot = any(k in " " + job["role"].lower() + " " for k in HOT)
    msg = {"topic": topic, "title": f"{'🔥 ' if hot else ''}{job['company']}: {job['role']}"[:250],
           "message": f"{job.get('track', '')} · {job['location'][:100]}\n{job.get('note', '')}".strip(),
           "click": job["url"], "priority": 5 if hot else 4, "tags": ["briefcase"],
           "actions": [{"action": "view", "label": "Open posting", "url": job["url"]}]}
    if os.environ.get("DRY_RUN") == "1" or not topic:
        print("[dry-run]", msg["title"], "|", msg["message"].replace("\n", " | "))
        return
    server = os.environ.get("NTFY_SERVER", "https://ntfy.sh").rstrip("/")
    urllib.request.urlopen(urllib.request.Request(server, data=json.dumps(msg).encode(),
                                                  headers={"Content-Type": "application/json"}), timeout=30).read()


def load_state():
    try:
        s = json.load(open(STATE))
        if isinstance(s, dict) and "boards" in s and isinstance(s["boards"], dict):
            return s
    except Exception:
        pass
    return {"boards": {}, "simplify": [], "jsearch": [], "last_jsearch": 0}


def slice_of(key):
    return int.from_bytes(key.encode()[-4:].rjust(4, b"\0"), "big") % SLICES


def main():
    now = time.time()
    st = load_state()
    conf = json.load(open(CONF))
    alerts = []

    listings = []
    try:
        listings = fetch_simplify()
        seen_sj = st["simplify"]
        for j in simplify_matches(listings, now):
            if j["id"] not in seen_sj:
                seen_sj.append(j["id"])
                alerts.append(j)
        st["simplify"] = seen_sj[-3000:]
    except Exception as e:
        print(f"[warn] Simplify failed: {e}", file=sys.stderr)

    manual = set(boards_from(conf, [], now))
    boards = boards_from(conf, listings, now)
    if conf.get("amazon"):
        boards["am:amazon"] = ("am", None, "Amazon", True)
    this_slice = int(now // 600) % SLICES
    todo = {k: v for k, v in boards.items() if v[3] or slice_of(k) == this_slice}
    print(f"boards known: {len(boards)} (hot {sum(v[3] for v in boards.values())}), checking {len(todo)} this run")

    def run(item):
        key, (kind, arg, name, hot) = item
        try:
            return key, name, FETCH[kind](arg), None
        except Exception as e:
            return key, name, None, e

    t0 = time.time()
    with ThreadPoolExecutor(THREADS) as ex:
        results = list(ex.map(run, todo.items()))
    errs = 0
    for key, name, jobs, err in results:
        if err is not None:
            errs += 1
            if key in manual:  # only complain about boards you listed yourself
                print(f"[warn] {key} (from companies.json): {err}", file=sys.stderr)
            continue
        prev = st["boards"].get(key)
        ids = [j["id"] for j in jobs]
        st["boards"][key] = ids
        if prev is None:
            continue  # first sighting: record silently
        prev = set(prev)
        for j in jobs:
            if j["id"] in prev:
                continue
            ts = j.get("ts")
            if ts is not None and now - ts > MAX_AGE_H * 3600:
                continue  # an old job that just resurfaced (relisted / search reshuffle), not a fresh posting
            rl = j["role"].lower()
            if ("intern" in rl or "co-op" in rl) and any(o in rl for o in OFF_TERM) and "spring" not in rl and "winter" not in rl:
                continue
            if title_ok(j["role"], ATS_STRICT) and is_us(j["location"]) and not any(c in name.lower() for c in NO_SPONSOR_COS):
                found = "10 min" if boards[key][3] else "hour"
                j.update(company=name, track="Career page",
                         note=f"{age_note(ts, now, found)} · sponsorship unknown")
                alerts.append(j)
    print(f"fetched {len(todo) - errs} boards in {time.time() - t0:.0f}s ({errs} errors)")

    key = os.environ.get("JSEARCH_KEY")
    if key and now - st.get("last_jsearch", 0) >= float(os.environ.get("JSEARCH_EVERY_H", "4")) * 3600 - 300:
        try:
            seen_js = st.get("jsearch", [])
            for j in src_jsearch(now, key):
                if j["id"] not in seen_js and title_ok(j["role"], True) and is_us(j["location"]) \
                        and not any(c in j["company"].lower() for c in NO_SPONSOR_COS):
                    seen_js.append(j["id"])
                    alerts.append(j)
            st["jsearch"] = seen_js[-2000:]
            st["last_jsearch"] = now
        except Exception as e:
            print(f"[warn] JSearch failed: {e}", file=sys.stderr)

    # the same role often shows up on Simplify and the career page; alert once
    alerted = [tuple(x) for x in st.get("alerted", [])]
    done = set(alerted)
    sent = 0
    for j in alerts:
        k = (j["company"].lower()[:20], j["role"].lower())
        if k in done:
            continue
        done.add(k)
        alerted.append(k)
        try:
            notify(j)
            sent += 1
            print(f"ALERT: {j['company']} | {j['role']} | {j['location'][:60]}")
        except Exception as e:
            print(f"[warn] notify failed: {e}", file=sys.stderr)
    st["alerted"] = [list(x) for x in alerted[-3000:]]

    tmp = STATE + ".tmp"
    json.dump(st, open(tmp, "w"), separators=(",", ":"))
    os.replace(tmp, STATE)
    print(f"{sent} alerts sent; state {os.path.getsize(STATE) // 1024} KB")


if __name__ == "__main__":
    main()
