import requests, argparse, pycountry, json, os, time, csv

SCHEMA = ['name', 'age', 'count', 'country_id', 'source']

def build_parser(): 
    p = argparse.ArgumentParser(description="CLI for agify parameters") 
    sp = p.add_subparsers(dest="cmd", required=True)
  
    q = sp.add_parser("query", aliases=["q"], help="query a name")
    q.add_argument("name")
    q.add_argument("--country_id", help="choose a country code")
    q.add_argument("--save_json", action="store_true", help="save JSON")
    q.add_argument("--info", action="store_true", help="info for request")
    q.add_argument("--to_csv", action="store_true", help="Build CSV row")

    c = sp.add_parser("countries", help="list ISO country codes")
    return p

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
        except requests.RequestException: 
            time.sleep(0.5 * (attempt +1))
    if r is None: 
        data = _read_cache()
        return None, data
    else: 
        data = r.json()
        _print_data(data, args)
        return r, data

def _build_csv_dict(data, response, args):
    csv_dict = {
        'name': data.get('name') or args.name, 
        'age': data.get('age'),
        'count': data.get('count'), 
        'country_id': (args.country_id or "").upper(),
        'source': 'cache' if response is None else 'live'
    }
    return csv_dict

def _csv_header_ok(path, schema):
    if not os.path.exists(path):
        return True
    with open(path, newline='') as file: 
        csv_reader = csv.reader(file)
        first_line = next(csv_reader, None)
    return first_line == schema 

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
        if args.to_csv and data['age'] is not None:
            file_path = "data/out.csv"
            csv_dict = _build_csv_dict(data, r, args)
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            is_new = not os.path.exists(file_path)
            mode = 'w' if is_new else 'a'
            if not _csv_header_ok(file_path, SCHEMA):
                rewrite = input("Warning: header mismatch. Recreate CSV file? Y/N: ")
                if rewrite in ('y', 'yes'):
                    is_new = True
                    mode = 'w'
                else: 
                    print("Aborted.")
                    raise SystemExit(0)
            try: 
                with open(file_path, mode, newline='') as csv_file:
                    writer = csv.DictWriter(csv_file, fieldnames=SCHEMA)
                    if is_new:
                        writer.writeheader()
                    writer.writerow(csv_dict)
                print(f"Successfully saved CSV file to {file_path}")
            except (OSError, IOError) as e: 
                print(f"CSV writer error: {e}")

        if args.info:
         if r is not None:
            print("Full URL:", r.url)
            print("Status code:", r.status_code)    
         else: 
            print("Info: using cache (offline)")
            
        if args.save_json and data['age'] is not None:
            file_path = "data/raw.json"
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            try: 
                with open(file_path, 'w') as json_file: 
                    json.dump(data, json_file, indent=4)
                print(f"data successfully written to {file_path}")   
            except IOError as e: 
                print(f"Error wrtiing file to {file_path}: {e}")

    if args.cmd == "countries":
        for country in pycountry.countries:
            print(f"{country.name}: {country.alpha_2}")



