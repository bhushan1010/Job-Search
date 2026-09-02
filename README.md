# Job Search Pipeline Deduplicator & Slack Deliverer

An Apify Actor that sits downstream of your job scraper tasks (`curious_coder/linkedin-jobs-search-scraper` and `borderline/indeed-scraper`).

## Features
- **Senior Role Exclusion**: Filters out Senior, Lead, Principal, Manager, Architect, and 5+ YOE positions automatically.
- **Entry-Level/Fresher Highlight**: Flags fresher-friendly, internship, and associate postings.
- **Persistent Deduplication**: Uses a shared named Apify Key-Value Store (`JOB_PIPELINE_SEEN_STORE`) to remember previously seen job URLs/IDs across runs.
- **Clean Slack Digest**: Posts a clean, scannable Block Kit digest to Slack via Incoming Webhook when new jobs are found, and silently suppresses empty runs.

## Deployment Options

### Option 1: Apify Web Console (Easiest, No CLI needed)
1. Go to [Apify Actors Console](https://console.apify.com/actors/new).
2. Click **Create new actor** -> Choose **Node.js** template.
3. In the **Source** tab:
   - Paste `package.json` into `package.json`
   - Paste `src/main.js` into `src/main.js` (or `main.js`)
   - Paste `.actor/input_schema.json` into the Input Schema editor.
4. Click **Build & Save**.

### Option 2: Apify CLI
```bash
npm install -g apify-cli
apify login
apify push
```

## Chaining with Scraper Tasks
In your scraper Task's **Integrations** tab:
1. Add a **Webhook** on event `Run succeeded` (`ACTOR.RUN.SUCCEEDED`).
2. Target: `Run an Actor` -> Select this `job-pipeline-dedup-filter` Actor.
3. Pass payload with `{"datasetId": "{{resource.defaultDatasetId}}", "sourceLabel": "AI ML Engineer - Pune", "slackWebhookUrl": "https://hooks.slack.com/services/..."}`.
