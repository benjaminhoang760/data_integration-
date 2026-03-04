# Agify Pipeline
## Quickstart
```bash 
### How do I make a good README with and directions notes like this? 
### Navigate or create desired directory
    python -m venv .venv
# Windows powershell
## Initializing v environment
    source .venv/bin/activate
## Install libraries
    pip install pycountry, requests
## Sample query with all fields
    python agify_API.py q ben --country_id US --json_cache --csv_save --info
## Sample database query
    python agify_API.py db init #to initialize db
    python agify_API.py db latest --limit 3 #SQL query for latest 3 entries

```
## Sample output
    The average age for Ben is 50 in United States 
    Sample size: 10936
    Successfully saved CSV file to data/out.csv
    data successfully written to data/raw.json
    Full URL: https://api.agify.io/?name=ben&country_id=US
    Status code: 200


