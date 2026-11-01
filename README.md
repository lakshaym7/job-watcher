# job-watcher

Every 10 minutes this checks three kinds of sources and pushes new matching roles to your phone via ntfy. Tap an alert to open the posting.

1. **SimplifyJobs** new-grad and Spring 2027 internship lists (these include sponsorship flags).
2. **Company career pages, read directly** from the public job-board feeds behind them: Greenhouse, Lever, Ashby, Workday, plus Amazon. These usually show a role before Indeed, Glassdoor, ZipRecruiter or Jobright pick it up, because those sites mostly re-list from the same feeds.
3. **Optional: JSearch** (Google for Jobs data, which pulls from Indeed, Glassdoor, ZipRecruiter, LinkedIn and others). It only runs if you add a `JSEARCH_KEY` secret.

## How it knows a posting is fresh

Every check compares each career page with what it looked like on the last check. A job ID that wasn't there before is a new posting, so you hear about it within about 10 minutes of it going live (about an hour for less-active companies). On top of that, every alert must pass a hard age cap, `MAX_AGE_H` (default 12 hours; set it to "6" in the workflow for stricter):

- **Greenhouse, Lever, Ashby:** the job's own publish timestamp must be within the cap. This catches old jobs that were reposted or relisted.
- **Workday:** only jobs marked "Posted Today".
- **Amazon:** only jobs posted today.
- **Simplify:** the time Simplify added the job must be within the cap.

Each alert shows the age, for example "posted 25 min ago" or "appeared within the last 10 min".

## How the company list grows by itself

You don't have to maintain the company list. On every run the watcher reads Simplify's full history (about 36,000 postings). Any company that has posted a tech role there in the past year with a Greenhouse, Lever, Ashby or Workday link gets added to the watch list automatically. That's about 3,200 career boards today, and it grows as Simplify adds new companies. From then on, that company's career page is checked directly, so you hear about its next role even if Simplify never lists it.

- **Every 10 minutes:** companies in `companies.json`, plus any company with a tech posting in the last 60 days (about 700 boards).
- **About hourly:** everything else, including all Workday boards, checked in rotating slices.

`companies.json` is only for extras: companies you care about that Simplify hasn't listed yet.

Filters: US locations, software/ML/data titles, no senior/staff/II+ titles, nothing flagged "no sponsorship" or citizenship-only, and no defense or clearance-heavy companies. Career-page roles must also look early-career (new grad, intern, university, I/1, junior, associate, 2026/2027…). ML/AI/LLM/GPU/infra/backend titles get a 🔥 and urgent priority.

## Setup (about 5 minutes)

1. **Phone:** install **ntfy** (iOS/Android) and subscribe to a random topic name, for example `lakshay-jobs-7f3k9q2x`. Anyone who knows the name can read your alerts.
2. **GitHub:** create a **public** repo (unlimited free Actions minutes; a private repo's free minutes only cover a check every ~30 min) and upload all of these files, including the `.github` folder.
3. **Secret:** Settings → Secrets and variables → Actions → New repository secret: `NTFY_TOPIC` = your topic name.
4. **Permissions:** Settings → Actions → General → Workflow permissions → **Read and write** → Save.
5. **First run:** Actions tab → job-watcher → **Run workflow**. The first time each board is checked, its current jobs are recorded silently, so you only hear about roles posted after that. The log shows how many boards were checked and how long it took. A line like `[warn] gh:something (from companies.json): HTTP Error 404` means a company name in `companies.json` is wrong; fix it or delete it.

## Adding companies by hand

Open a company's careers page and look at where "Apply" goes:

- `boards.greenhouse.io/SLUG` or `job-boards.greenhouse.io/SLUG` → add SLUG to `greenhouse`
- `jobs.lever.co/SLUG` → `lever`
- `jobs.ashbyhq.com/SLUG` → `ashby`
- `COMPANY.wd5.myworkdayjobs.com/SITE` → add `{"name", "host", "tenant", "site"}` to `workday` (tenant is usually the first part of the host)

## Optional: Indeed / Glassdoor / ZipRecruiter / LinkedIn coverage

Sign up for **JSearch** on RapidAPI (it has a free tier) and add your key as a repo secret named `JSEARCH_KEY`. It runs every 4 hours by default to stay inside the free quota. You can change that with `JSEARCH_EVERY_H` in the workflow if you upgrade.

## Tuning

- `ATS_STRICT: "0"` in the workflow alerts on any non-senior SWE/ML role from career pages, not only ones labelled early-career. You'll get more alerts, and more noise.
- `HOT_DAYS` sets how recently a company must have posted to be checked every 10 minutes (default 60). `SLICES` sets how the rest are spread out (6 slices × 10 min = hourly).
- Keyword lists are at the top of `watcher.py`.
- Memory of seen jobs is kept in the Actions cache (`state.json`), not committed to git. If GitHub ever clears that cache, the watcher re-records everything silently instead of spamming you. A tiny commit once a month stops GitHub from pausing the schedule for inactivity.
