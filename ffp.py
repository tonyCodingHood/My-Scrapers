import time
import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from dateutil.parser import parse, ParserError

# ---------- CONFIG ----------
INPUT_FILE = r"C:\Users\CagedBird\Desktop\Work\Tom\rib.xlsx"
OUTPUT_FILE = r"C:\Users\CagedBird\Desktop\Work\Tom\Fantasy.xlsx"

BACK_YEAR_LIMIT = 5
FORWARD_YEAR_CAP = 2025
REQUEST_TIMEOUT = 10
SLEEP_BETWEEN_REQUESTS = 0.35
MAX_MISSED_WEEKS_BEFORE = 4  # New limit to prevent jumping too far back after a minimum is met
MIN_PRIOR_GAMES = 4

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36"
}

WEEK_RE = re.compile(r"Week\s*(\d+)\s*,\s*(\d{4})", re.IGNORECASE)

# ---------- Utilities ----------
def name_to_slug(name):
    base = re.sub(r"[^\w\s-]", "", name.lower()).replace(".", "").replace("'", "")
    return "-".join(base.split())

def base_games_url(identifier_or_url):
    ident = str(identifier_or_url).strip()
    if ident.lower().startswith("http"):
        return ident.split("?")[0]
    return f"https://www.fantasypros.com/nfl/games/{ident}.php"

def attach_query(base_url, season, scoring=None):
    base = base_url.split("?")[0]
    if scoring:
        return f"{base}?scoring={scoring}&season={season}"
    return f"{base}?season={season}"

def get_nfl_week(date_obj, year):
    nfl_week_1_dates = {
        2016: datetime(2016, 9, 8),
        2017: datetime(2017, 9, 7),
        2018: datetime(2018, 9, 6),
        2019: datetime(2019, 9, 5),
        2020: datetime(2020, 9, 10),
        2021: datetime(2021, 9, 9),
        2022: datetime(2022, 9, 8),
        2023: datetime(2023, 9, 7),
        2024: datetime(2024, 9, 5),
        2025: datetime(2025, 9, 4),
    }

    if date_obj.year != year:
        return 0, 'Pre-Season'

    week_1_date = nfl_week_1_dates.get(year)
    if not week_1_date:
        return 0, 'Unknown'
    
    if date_obj < week_1_date:
        return 0, 'Pre-Season'
    
    diff_days = (date_obj - week_1_date).days
    week = (diff_days // 7) + 1
    return week, 'Regular Season'

def parse_flexible_injury_week(s):
    s = str(s).strip()
    m = WEEK_RE.search(s)
    if m:
        return int(m.group(1)), int(m.group(2))
    
    try:
        date_obj = parse(s)
        year = date_obj.year
        week, _ = get_nfl_week(date_obj, year)
        return week, year
    except (ParserError, ValueError, TypeError):
        return None, None

# ---------- Fetching & parsing ----------
def fetch_url(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200:
            return None, r.status_code
        return r.text, 200
    except requests.RequestException:
        return None, None

def find_game_table_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    el = soup.select_one("div.mobile-table table, table.mobile-table, table.table")
    if el:
        rows = el.find_all("tr")
        for tr in rows[:6]:
            if "Week" in tr.get_text():
                return el
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        for tr in rows[:6]:
            if "Week" in tr.get_text():
                return tbl
    return None

def parse_table_rows(table, season):
    rows_out = []
    header_cells = []
    thead = table.find("thead")
    if thead:
        hdr_row = thead.find("tr")
        if hdr_row:
            header_cells = [th.get_text(strip=True).lower() for th in hdr_row.find_all(["th", "td"])]
    else:
        first_row = table.find("tr")
        if first_row and first_row.find("th"):
            header_cells = [th.get_text(strip=True).lower() for th in first_row.find_all(["th", "td"])]

    fp_col_index = None
    for i, txt in enumerate(header_cells):
        if any(k in txt for k in ("fpts", "fantasy", "fp", "fantasy points", "fpts (half)")):
            fp_col_index = i
            break

    tbody = table.find("tbody")
    row_tags = tbody.find_all("tr") if tbody else table.find_all("tr")

    for tr in row_tags:
        text = tr.get_text(" ", strip=True)
        if not text:
            continue

        if "bye" in text.lower():
            tds = tr.find_all("td")
            weeknum = None
            if tds:
                first = tds[0].get_text(strip=True)
                if first.lower().startswith("week"):
                    try:
                        weeknum = int(first.split()[1])
                    except:
                        weeknum = None
            if weeknum is None:
                continue
            rows_out.append({
                "week": weeknum,
                "season": season,
                "status": "BYE",
                "fantasy_points": None
            })
            continue

        tds = tr.find_all("td")
        if not tds:
            continue

        first_txt = tds[0].get_text(strip=True)
        if not first_txt.lower().startswith("week"):
            continue
        try:
            weeknum = int(first_txt.split()[1])
        except:
            continue

        fp_val = None
        if len(tds) >= 2:
            s = tds[-2].get_text(strip=True).replace(",", "")
            if s and s not in ("-", ""):
                try:
                    fp_val = float(s)
                except:
                    fp_val = None

        if fp_val is None:
            for s_td in reversed(tds[-4:]):
                s = s_td.get_text(strip=True).replace(",", "")
                if s and s not in ("-", ""):
                    try:
                        fp_val = float(s)
                        break
                    except:
                        fp_val = None

        status = "Played" if fp_val is not None else "Skipped"
        rows_out.append({
            "week": weeknum,
            "season": season,
            "status": status,
            "fantasy_points": fp_val
        })
    return rows_out

def scrape_season_with_fallback(base_url, year):
    half_url = attach_query(base_url, year, scoring="HALF")
    html, code = fetch_url(half_url)
    scoring_used = "Standard"
    if html:
        tbl = find_game_table_from_html(html)
        if tbl:
            rows = parse_table_rows(tbl, year)
            if any(r["status"] == "Played" and r["fantasy_points"] is not None for r in rows):
                scoring_used = "Half-PPR"
                return rows, scoring_used
    default_url = attach_query(base_url, year, scoring=None)
    html2, code2 = fetch_url(default_url)
    if html2:
        tbl2 = find_game_table_from_html(html2)
        if tbl2:
            rows2 = parse_table_rows(tbl2, year)
            if any(r["status"] == "Played" and r["fantasy_points"] is not None for r in rows2):
                return rows2, scoring_used
            if html:
                if tbl:
                    return rows, scoring_used
            return rows2, scoring_used
    return [], "N/A"

# ---------- Aggregation ----------
def collect_all_rows(base_url, injury_year):
    start_year = max(injury_year - BACK_YEAR_LIMIT, 2000)
    end_year = FORWARD_YEAR_CAP if injury_year <= FORWARD_YEAR_CAP else injury_year + 1
    
    all_rows = []
    scoring_used = "N/A"
    for y in range(start_year, end_year + 1):
        rows, current_scoring = scrape_season_with_fallback(base_url, y)
        if rows:
            all_rows.extend(rows)
            scoring_used = current_scoring
        time.sleep(SLEEP_BETWEEN_REQUESTS)
    
    all_rows = sorted(all_rows, key=lambda x: (x["season"], x["week"]))
    return all_rows, scoring_used

# ---------- Selection helpers ----------
def select_prior_played(all_rows, iw, iy, max_games=6):
    prior_games = []
    weeks_skipped = 0
    
    # Sort all rows in reverse order (most recent first)
    all_rows_rev = sorted(all_rows, key=lambda x: (x["season"], x["week"]), reverse=True)
    
    # Find the last game before the injury week
    start_index = -1
    for i, game in enumerate(all_rows_rev):
        if game["season"] < iy or (game["season"] == iy and game["week"] < iw):
            start_index = i
            break
            
    if start_index == -1:
        return []

    # Iterate backwards from the injury week
    for i in range(start_index, len(all_rows_rev)):
        current_game = all_rows_rev[i]
        
        # Check if the current game is 'Played'
        if current_game["status"] == "Played":
            prior_games.append(current_game)
            weeks_skipped = 0
        
        # Count skipped weeks (excluding byes)
        if current_game["status"] not in ("Played", "BYE"):
            weeks_skipped += 1
            
        # Apply the gap limit only AFTER the minimum game count is met
        if len(prior_games) >= MIN_PRIOR_GAMES and weeks_skipped > MAX_MISSED_WEEKS_BEFORE:
            break
        
        # Stop if we have reached the max number of games
        if len(prior_games) >= max_games:
            break
            
    # Return games in chronological order
    return sorted(prior_games, key=lambda x: (x["season"], x["week"]))

def select_after_played(all_rows, iw, iy, max_games=6):
    after_candidates = [r for r in all_rows if r["status"] == "Played" and ((r["season"] > iy) or (r["season"] == iy and r["week"] > iw))]
    after_candidates = sorted(after_candidates, key=lambda x: (x["season"], x["week"]))
    return after_candidates[:max_games]

def compute_weeks_missed_excluding_byes(all_rows, iw, iy, return_game):
    if not return_game:
        return None
    rw, ry = return_game["week"], return_game["season"]
    count = 0
    for r in all_rows:
        if (r["season"], r["week"]) <= (iy, iw):
            continue
        if (r["season"], r["week"]) >= (ry, rw):
            break
        if r["status"] == "BYE":
            continue
        if r["status"] != "Played":
            count += 1
    return count

# ---------- Output builders ----------
def calculate_average(games):
    pts = [g["fantasy_points"] for g in games if g.get("fantasy_points") is not None]
    if not pts:
        return 0.0, 0, 0.0
    total = round(sum(pts), 2)
    count = len(pts)
    avg = round(total / count, 2)
    return total, count, avg

def build_excel_row(name, iw_str, all_rows, prior_played, after_played):
    row = {"PLAYER NAME": name, "Injury Week": iw_str}
    prior_sorted = sorted(prior_played, key=lambda x: (x["season"], x["week"]))
    for i in range(6):
        if i < len(prior_sorted):
            val = prior_sorted[i]["fantasy_points"]
            row[f"Before_{i+1}"] = val if val is not None else 0.0
        else:
            row[f"Before_{i+1}"] = 0.0
    if not after_played:
        for i in range(1, 7):
            row[f"After_{i}"] = "N/A"
        row["Return Week"] = ""
        row["Weeks Missed Until Return"] = ""
        return row
    row["After_1"] = after_played[0]["fantasy_points"] if after_played[0]["fantasy_points"] is not None else "N/A"
    row["Return Week"] = f"Week {after_played[0]['week']}, {after_played[0]['season']}"
    
    parsed_iw, parsed_iy = parse_flexible_injury_week(iw_str)
    missed = compute_weeks_missed_excluding_byes(all_rows, parsed_iw, parsed_iy, after_played[0])
    row["Weeks Missed Until Return"] = missed if missed is not None else ""
    
    for idx in range(1, 6):
        key = f"After_{idx+1}"
        if len(after_played) > idx:
            val = after_played[idx]["fantasy_points"]
            row[key] = val if val is not None else "N/A"
        else:
            row[key] = "N/A"
    return row

# ---------- Interactive prompt for URL ----------
def prompt_for_url_until_valid(name):
    while True:
        user = input(f"Enter the full FantasyPros GAMES URL for {name} (or press Enter to skip): ").strip()
        if user == "":
            return None
        if user.lower().startswith("http"):
            return base_games_url(user)
        print("Please paste a full URL starting with 'http' or press Enter to skip.")

# ---------- Main run ----------
def interactive_run():
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Failed to read input Excel '{INPUT_FILE}': {e}")
        return

    results = []
    errors = []

    for idx, rec in df.iterrows():
        name = str(rec.get("PLAYER NAME", "")).strip()
        iw_str = str(rec.get("Injury Week", "")).strip()
        if not name or not iw_str:
            print(f"Skipping row {idx} - missing name or injury week.")
            continue

        print(f"\nProcessing {name} (Injured {iw_str})...")

        iw, iy = parse_flexible_injury_week(iw_str)
        if iw is None or iy is None:
            print(f"Skipping {name} - bad injury week format: {iw_str}")
            placeholder = {"PLAYER NAME": name, "Injury Week": iw_str}
            for i in range(1, 7):
                placeholder[f"Before_{i}"] = 0.0
            for i in range(1, 7):
                placeholder[f"After_{i}"] = "N/A"
            placeholder["Return Week"] = ""
            placeholder["Weeks Missed Until Return"] = ""
            results.append(placeholder)
            errors.append((name, f"Bad injury week format: {iw_str}"))
            continue

        base_url = base_games_url(name_to_slug(name))

        while True:
            try:
                all_rows, scoring_used = collect_all_rows(base_url, iy)
            except Exception as e:
                print(f"\nError fetching for {name}: {type(e).__name__}: {e}")
                user_url = prompt_for_url_until_valid(name)
                if user_url is None:
                    na_row = {"PLAYER NAME": name, "Injury Week": iw_str}
                    for i in range(1, 7):
                        na_row[f"Before_{i}"] = 0.0
                    for i in range(1, 7):
                        na_row[f"After_{i}"] = "N/A"
                    na_row["Return Week"] = ""
                    na_row["Weeks Missed Until Return"] = ""
                    results.append(na_row)
                    errors.append((name, f"Failed to fetch data (user skipped)"))
                    break
                else:
                    base_url = user_url
                    continue
            
            prior_played = select_prior_played(all_rows, iw, iy, max_games=6)
            after_played = select_after_played(all_rows, iw, iy, max_games=6)

            if not prior_played and not after_played:
                print(f"\nNo data found for {name} using slug-based URL: {base_url}")
                user_url = prompt_for_url_until_valid(name)
                if user_url is None:
                    na_row = {"PLAYER NAME": name, "Injury Week": iw_str}
                    for i in range(1, 7):
                        na_row[f"Before_{i}"] = 0.0
                    for i in range(1, 7):
                        na_row[f"After_{i}"] = "N/A"
                    na_row["Return Week"] = ""
                    na_row["Weeks Missed Until Return"] = ""
                    results.append(na_row)
                    errors.append((name, "No game data found (user skipped)"))
                    break
                else:
                    base_url = user_url
                    continue
            
            print("\n" + "=" * 60)
            print(f"Analyzing {name} (Injured {iw_str})")
            print(f"Scoring Used: {scoring_used}")
            
            print("\n--- Previous games (min 4, up to 6, with a max 4-week gap) ---")
            if prior_played:
                for g in prior_played:
                    print(f"Week {g['week']}, {g['season']}: {g['fantasy_points']} pts (Status: {g['status']})")
                t, c, a = calculate_average(prior_played)
                print(f"Average: {t} / {c} = {a}")
            else:
                print("Not enough data for previous games.")
            
            print("\n--- First game after injury ---")
            if after_played:
                g1 = after_played[0]
                print(f"Week {g1['week']}, {g1['season']}: {g1['fantasy_points']} pts (Status: {g1['status']})")
                missed = compute_weeks_missed_excluding_byes(all_rows, iw, iy, g1)
                print(f"Weeks missed until return: {missed if missed is not None else 'N/A'}")
            else:
                print("Not enough data for first game after injury.")
            
            print("\n--- Games 2-3 after injury ---")
            seg23 = after_played[1:3] if len(after_played) >= 2 else []
            if len(seg23) < 2:
                print("Not enough data for games 2-3 after injury.")
            else:
                for g in seg23:
                    print(f"Week {g['week']}, {g['season']}: {g['fantasy_points']} pts (Status: {g['status']})")
                t23, c23, a23 = calculate_average(seg23)
                print(f"Average: {t23} / {c23} = {a23}")
            
            print("\n--- Games 4-6 after injury ---")
            seg46 = after_played[3:6] if len(after_played) >= 4 else []
            if len(seg46) < 3:
                print("Not enough data for games 4-6 after injury.")
            else:
                for g in seg46:
                    print(f"Week {g['week']}, {g['season']}: {g['fantasy_points']} pts (Status: {g['status']})")
                t46, c46, a46 = calculate_average(seg46)
                print(f"Average: {t46} / {c46} = {a46}")
            
            excel_row = build_excel_row(name, iw_str, all_rows, prior_played, after_played)
            results.append(excel_row)
            break
    
    try:
        out_df = pd.DataFrame(results)
        out_df.to_excel(OUTPUT_FILE, index=False)
        print(f"\nResults saved to {OUTPUT_FILE}")
    except Exception as e:
        print(f"Failed to save Excel: {e}")
    
    if errors:
        print("\n--- Players with errors ---")
        for nm, msg in errors:
            print(f"- {nm}: {msg}")
    else:
        print("\nNo player errors encountered.")

if __name__ == "__main__":
    interactive_run()