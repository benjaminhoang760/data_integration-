import requests, json, os, argparse

def build_parser(): 
    p = argparse.ArgumentParser(description="CLI modes")
    p.add_argument("--params", help="Choose a parameter for endpoint URL")
    return p

args = build_parser().parse_args()

URL = "https://jsonplaceholder.typicode.com/posts"
params = {}
if args.params:
    params = args.params

response = requests.get(URL, params=params, timeout=3)
data = response.json()

print(response.status_code)
###print(response.headers)
print("Full URL:", response.url)
print(data)