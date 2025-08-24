# quake/etl.py

import requests
from datetime import datetime
from pathlib import Path

# folder where data will be saved (you already made it)
raw_folder = Path("/Users/mac/Earthquake Data")

# fixed query values
starttime = "2024-01-01"
endtime   = "2025-08-20"
minmag    = "3"
fmt       = "csv"   # can also be "geojson"

# nepal bounding box
use_bbox = True
minlat, maxlat = "26", "31"
minlon, maxlon = "80", "89"

def extract():
    raw_folder.mkdir(parents=True, exist_ok=True)

    # build the url like your old code style (plain string concat)
    url = f"https://earthquake.usgs.gov/fdsnws/event/1/query?format={fmt}&starttime={starttime}&endtime={endtime}&minmagnitude={minmag}"
    if use_bbox:
        url += f"&minlatitude={minlat}&maxlatitude={maxlat}&minlongitude={minlon}&maxlongitude={maxlon}"

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"usgs_{stamp}.{fmt if fmt=='csv' else 'json'}"
    outpath = raw_folder / filename

    print("[extract] downloading:", url)
    r = requests.get(url, timeout=120)
    r.raise_for_status()

    if fmt == "csv":
        outpath.write_bytes(r.content)
    else:
        outpath.write_text(r.text, encoding="utf-8")

    print("[extract] saved to:", outpath)
    return outpath

if __name__ == "__main__":
    extract()