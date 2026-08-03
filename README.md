# Baja Blast Tracker

Tracks MLB home runs hit **420+ feet**, styled around the "Baja Blast"
regular-season home-run-distance promotion.

## How it works

- **`fetch_stats.py`** pulls home run data starting from `START_DATE`
  (2026-03-20) with a minimum distance cutoff (`MIN_DISTANCE = 420` ft),
  writing results to `data.json`. It blends two sources: the MLB Stats
  API for very recent/live games, and Baseball Savant (Statcast) once it
  has had time to refresh (Savant typically finishes its daily refresh by
  ~4 AM ET, so the script keeps yesterday's games in the "live"/Stats API
  bucket until 08:00 UTC the next day before switching over to Savant).
- **`index.html`** is a static dark-themed page that reads `data.json` and
  displays the qualifying home runs.

## Automation

`.github/workflows/update.yml` runs `fetch_stats.py` on a cron schedule
(hourly, 10am–1am CDT, March–October — the MLB regular season window)
and commits the refreshed `data.json` back to the repo. Also runnable
manually via `workflow_dispatch`.

## Running locally

```bash
pip install requests
python fetch_stats.py
```

Then open `index.html` (or serve the folder with
`python -m http.server`) to view the tracker.

## Deploying

Push to a GitHub repo, enable GitHub Pages (branch `main`, root), and the
Actions workflow keeps `data.json` fresh automatically during the season.

## Structure

```
baja-blast-tracker-main/
├── fetch_stats.py   # MLB Stats API + Baseball Savant -> data.json
├── data.json         # generated home run data (≥420 ft)
├── index.html        # front-end
└── .github/workflows/update.yml
```
