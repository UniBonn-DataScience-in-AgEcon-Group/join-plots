help = """
Uni Bonn - Crop rotation joiner

Joins crop type information from multiple years using spatial joins 
and largest area overlap to determine crop rotations on a single plot level.

Usage:
  python join-plots.py --cur <current_year_file> --hist <historical_files_folder> --out <result_file>

Example:
  python join-plots.py --cur ./test/input/2023.json --hist ./test/input --out ./test/output/joined-plots.shp
  python3.12 join-plots.py --cur ./hist/schlaege_2023.gpkg --hist ./hist --out ./joined-plots-2015-2023.parquet
"""

import argparse, os, re
import geopandas as gpd
import math
from tqdm import tqdm
gpd.options.io_engine = 'pyogrio'
tqdm.pandas()
parser=argparse.ArgumentParser(add_help=False)

parser.add_argument('-h', '--help', action='help', default=argparse.SUPPRESS, help=help)
parser.add_argument("--cur", help="Path to the current year plot file")
parser.add_argument("--hist", help="Path to the historical plots folder, containing one file per year, e.g. 2017.json, 2018.json, etc.")
parser.add_argument("--out", help="Path to the output file")
parser.add_argument("--key", help="Key that stores the crop type in the plot file, default: CODE")
parser.add_argument("--id", help="Key that stores the plot ID in the plot file, default: ID")

args=parser.parse_args()

current_year_file = args.cur
historical_files_folder = args.hist
result_file = args.out
crop_key = args.key if args.key else 'Nutzartcode'
id_key = args.id if args.id else 'ID'

# Import current year plots
print(f'Importing current year plots: {current_year_file}')
plots_current = gpd.read_file(current_year_file)
plots_current.set_crs(epsg=25832, inplace=True)
cur_year = re.search(r'\d{4}', current_year_file).group()
# rename crop_key column to crop_key_{cur_year}
plots_current[id_key] = plots_current.index + 1
plots_current.rename(columns={crop_key: f"{crop_key}_{cur_year}"}, inplace=True)
# Print number of rows in plots_current
print(f"Number of rows in {cur_year}: {len(plots_current)}")


# only consider the first 50000 rows
# plots_current = plots_current.head(50000)

# Import historical plots (except current year file), 
# extract year from filename
# and rename {crop_key} column to {crop_key}_{year}
crop_types = {}
start_year = ""
for file in os.listdir(historical_files_folder):
  if not file.startswith('.'):
    year = int(re.search(r'\d{4}', file)[0])
    if not start_year or year < start_year:
      start_year = year
    # Don't import the current year file
    if year == int(cur_year):
      continue
    print(f'Importing {year}')
    crop_types[year] = gpd.read_file(
      os.path.join(historical_files_folder, file), 
      columns=['geometry', crop_key, id_key]
    )
    crop_types[year].set_crs(epsg=25832, inplace=True)
    # print number of rows in crop_types[year]
    print(f"Number of rows in {year}: {len(crop_types[year])}")
    crop_types[year][id_key] = crop_types[year].index + 1
    crop_types[year].rename(columns={crop_key: f"{crop_key}_{year}"}, inplace=True)
    
    # only consider the first 50000 rows
    # crop_types[year] = crop_types[year].head(50000)
  
# print keys in crop_types
print('Crop types:')
print(crop_types.keys())

def get_intersection_area(row):
  try:
    area = row['geometry'].intersection(row['geometry_right']).area
    return area
  except:
    return 0
    
def stringify_row(row):
  row = row[1]
  return f"{row[f'{id_key}_right']}_{row['intersection']}_{row[f'{crop_key}_{year}']}"

# Perform a spatial join, so that every plot from cur_year 
# has a {crop_key}_{year} property for the keys (years) in the crop_types dict
for year in crop_types.keys():
  print(f'Joining {year}')
  # Join all plots from {year} that intersect
  # with the current year plots
  crop_types[year]["geometry_right"] = crop_types[year].geometry
  plots_current = plots_current.sjoin(
    crop_types[year],
    how='left',
    predicate='intersects'
  )
  # Add a column with the area of the intersection
  plots_current["intersection"] = plots_current.progress_apply(
    # lambda row: row['geometry'].intersection(row['geometry_right']).area if row['geometry_right'] else 0, axis=1
    lambda row: get_intersection_area(row), axis=1
  )
  
  # remove rows where the intersection area is less than 1 square meter
  plots_current = plots_current[plots_current["intersection"] > 1]
  
  # We only join the plot with the largest intersection area,
  # however, we want to store the other intersections in a separate column
  # so we can check them later
  intersections = plots_current\
    .sort_values(by='intersection')\
    .groupby(f"{id_key}_left")\
    .apply(lambda group: "::".join(stringify_row(row) for row in group.iterrows()) if group.shape[0] > 0 else None)\
    .rename(f"intersections_{year}")
      
  # plots_current["intersection"] = plots_current["geometry"].intersection(plots_current["geometry_right"], align=False).area
  # Sort by intersection area and keep only the last row 
  # (largest intersection) for each plot
  plots_current = plots_current\
    .sort_values(by='intersection')\
    .groupby(f"{id_key}_left")\
    .last()\
    .reset_index()
  
  # Join intersections with the plots_current dataframe
  plots_current = plots_current.join(
    intersections, on=f"{id_key}_left"
  )
  # Rename f"{id_key}_right" to f"{id_key}_{year}"
  plots_current.rename(columns={f"{id_key}_right": f"{id_key}_{year}"}, inplace=True)
  # Drop the columns that are not needed anymore
  plots_current = plots_current.drop(
    columns=['index_right', 'intersection', 'geometry_right']
    # columns=['index_right', 'intersection', 'geometry_right', f"{id_key}_right"]
  )
  plots_current.set_crs(epsg=25832, inplace=True)
  # rename ID_left to ID
  plots_current.rename(columns={f"{id_key}_left": id_key}, inplace=True)
    # Print number of rows in plots_current
  print(f"Number of rows in {cur_year} after joining {year}: {len(plots_current)}")


# export the joined data
print('Exporting')
print(plots_current)
if not args.out:
  result_file = f'./joined-plots_{start_year}-{cur_year}.shp'

# check if result_file ends with .parquet
if result_file.endswith('.parquet'):
  plots_current.to_parquet(result_file)
else:
  plots_current.to_file(result_file)
# Also export as CSV, replace anything after the last dot with .csv
# without geometry column
plots_current.to_csv(
  re.sub(r'\.[^.]+$', '.csv', result_file), 
  columns=[c for c in plots_current.columns if c != 'geometry']
)