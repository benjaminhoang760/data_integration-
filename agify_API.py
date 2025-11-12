import requests, argparse, pycountry, json, os, time, csv, sqlite3

SCHEMA = ['name', 'age', 'count', 'country_id', 'source']
FIELD_PATH = "data/fields.json"
CSV_PATH = "data/out.csv"
DB_PATH = "data/pipeline.db"
TABLE_NAME = "agify_queries"

def build_parser(): 
    p = argparse.ArgumentParser(description="CLI for agify parameters") 
    sp = p.add_subparsers(dest="cmd", required=True)
  
    q = sp.add_parser("query", aliases=["q"], help="query a name")
    q.add_argument("name")
    q.add_argument("--country_id", help="choose a country code")
    q.add_argument("--json_cache", action="store_true", help="save JSON")
    q.add_argument("--csv_save", action="store_true", help="Build CSV row")
    q.add_argument("--info", action="store_true", help="info for request")
    
    db = sp.add_parser("db", help="database ops")
    db.add_argument("--init", action="store_true", help="create SQLlite db and table")
    db.add_argument("--latest", type=int, metavar="N", help='show N most recent rows')

    c = sp.add_parser("countries", help="list ISO country codes")
    return p

def _get_db(db_path = DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def db_initialize(db_path = DB_PATH):
    with _get_db(db_path) as conn:
        conn.execute(f"""
                      CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                      id INTEGER PRIMARY KEY AUTOINCREMENT,
                      name TEXT NOT NULL, 
                      age INTEGER, 
                      count INTEGER, 
                      country_id TEXT, 
                      source TEXT NOT NULL CHECK (source IN ('live', 'cache')),
                      fetched_at TEXT NOT NULL DEFAULT (datetime('now'))
                        );
                    """)
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_fetched_at ON {TABLE_NAME}(fetched_at);")
        conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_country ON {TABLE_NAME}(country_id);")

def db_insert_row(row: dict, db_path = DB_PATH):
    with _get_db(db_path) as conn:
        cur = conn.execute(
            f"""INSERT INTO {TABLE_NAME} (name, age, count, country_id, source)
            VALUES (?, ?, ?, ?, ?)""",
            (row['name'], row['age'], row['count'], row['country_id'], row['source'])
        )
        return cur.lastrowid

def db_query_latest(n: int):
    with _get_db() as conn: 
        return conn.execute(
            f"""SELECT * FROM {TABLE_NAME}
            ORDER BY datetime(fetched_at) DESC
            LIMIT ?""", (n,)
        ).fetchall()
    
def get_data(args):
    r = None
    url = "https://api.agify.io/"
    hds = {"Accept": "application/json", "User-Agent": "bh-learning/0.1"}
    prms = {'name': args.name}

    if args.country_id: 
        prms['country_id'] = args.country_id.upper()
    for attempt in range(3):
        try: 
            r = requests.get(url, headers=hds, params=prms, timeout=5)
            r.raise_for_status()
            break 
        except requests.RequestException as e: 
            if attempt == 2: 
                print(f"Network attempts failed:\n{e}")
                break
            print(f"Attempt {attempt+1} failed. Retrying...")
            time.sleep(0.5 * (attempt +1))
    if r is None: 
        data = _read_cache()
        return None, data
    else: 
        data = r.json()
        _print_data(data, args)
        return r, data


def _load_field_map(path=FIELD_PATH):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if not os.path.exists(path):
        with open(path, "w") as f:
            json.dump({
                "name": "name",
                "age": "age",
                "count": "count",
                "country_id": "country_id"
            }, f, indent=4)
    with open(path, "r") as f: 
        return json.load(f)


def _normalize_record(raw: dict, response, args, field_map:dict):
    out = {}
    for target_key, source_key in field_map.items():
        out[target_key] = raw.get(source_key)
    out["name"] = out.get("name") or args.name
    out["country_id"] = (out.get("country_id") or args.country_id or "").upper()
    out["source"] = "cache" if response is None else "live"
    return {k: out.get(k) for k in SCHEMA}

##could consolidate this 
def _build_csv_dict(data, response, args):
    field_map = _load_field_map()
    return _normalize_record(data, response, args, field_map)

def _csv_header_ok(path, schema):
    if not os.path.exists(path):
        return True
    with open(path, newline='') as file: 
        csv_reader = csv.reader(file)
        first_line = next(csv_reader, None)
    return first_line == schema 

def write_csv(csv_path, data, response, args):
    csv_dict = _build_csv_dict(data, response, args)
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    is_new = not os.path.exists(csv_path)
    mode = 'w' if is_new else 'a'
    if not _csv_header_ok(csv_path, SCHEMA):
        rewrite = input("Warning: header mismatch. Recreate CSV file? y/n: ").lower()
        if rewrite in ('y', 'yes'):
            is_new = True
            mode = 'w'
        else:
            print('Command aborted')
            raise SystemExit(0)
    try: 
        with open(csv_path, mode, newline='') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=SCHEMA)
            if is_new: 
                writer.writeheader()
            writer.writerow(csv_dict)
            rowid = db_insert_row(csv_dict)
        print(f"Successfully saved CSV file to {csv_path}")
        print(f"Inserted row id {rowid} into {TABLE_NAME}")
    except (OSError, IOError) as e:
        print(f"CSV writer error: {e}")

def _print_data(data, args): 
    output = f"The average age for {args.name.capitalize()} is {data['age']}"
    sample_size = f"\nSample size: {data['count']}"
    try: 
        if args.country_id:
            country = pycountry.countries.get(alpha_2=args.country_id.upper())
            print(output, "in", country.name, sample_size)
        else: 
            print(output, sample_size)
    except AttributeError:
        print(args.country_id, "does not exist\n 'countries' for all countries")

def _read_cache():
    file_path = "data/raw.json"
    if os.path.exists(file_path):
        print("using cache")
        with open(file_path, "r") as cached_json:
            data = json.load(cached_json)
        print(f"{data['name'].capitalize()}'s age is {data['age']}")
        return data
    else:
        print("Fail: cache not found")
        raise SystemExit(1)

#---------------------------------------tests-------------------------------------------#
def country_test(data):
  print()

#---------------------------------------main-------------------------------------------#

if __name__ == "__main__":
    args = build_parser().parse_args()
 
    if args.cmd in ("query", "q"):
        r, data = get_data(args)

        if args.csv_save and data['age'] is not None:
            write_csv(CSV_PATH, data, r, args)

        if args.json_cache and data['age'] is not None:
            file_path = "data/raw.json"
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            try: 
                with open(file_path, 'w') as json_file: 
                    json.dump(data, json_file, indent=4)
                print(f"data successfully written to {file_path}")   
            except IOError as e: 
                print(f"Error writing file to {file_path}: {e}")
                
        if args.info:
            if r is not None:
                print("Full URL:", r.url)
                print("Status code:", r.status_code)    
            else: 
                print("Info: using cache (offline)")

    if args.cmd == "db":
        if args.init:
            if os.path.exists(DB_PATH):
                print("DB already initalized")
            elif not os.path.exists(DB_PATH):
                db_initialize()
                print(f"DB initialized at {DB_PATH} with table '{TABLE_NAME}'.")
        elif args.latest:
            if getattr(args, "latest", None):
                for r in db_query_latest(args.latest): 
                    print(dict(r))

    if args.cmd == "countries":
        for country in pycountry.countries:
            print(f"{country.name}: {country.alpha_2}")
