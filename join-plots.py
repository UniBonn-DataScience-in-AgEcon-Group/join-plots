help = """
Uni Bonn - Crop rotation joiner

Joins crop type information from multiple years using spatial joins 
and largest area overlap to determine crop rotations on a single plot level.

Usage:
  python join-plots.py --cur <current_year_file> --hist <historical_files_folder> --out <result_file>

Example:
  python join-plots.py --cur ./test/input/2023.json --hist ./test/input --out ./test/output/joined-plots.shp
"""

import argparse, os, re
import geopandas as gpd
gpd.options.io_engine = 'pyogrio'

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
crop_key = args.key if args.key else 'CODE'
id_key = args.id if args.id else 'ID'

# Import current year plots
print(f'Importing current year plots: {current_year_file}')
plots_current = gpd.read_file(current_year_file)
cur_year = re.search(r'\d{4}', current_year_file).group()
# rename crop_key column to crop_key_{cur_year}
plots_current.rename(columns={crop_key: f"{crop_key}_{cur_year}"}, inplace=True)

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
    crop_types[year].rename(columns={crop_key: f"{crop_key}_{year}"}, inplace=True)


# print keys in crop_types
print('Crop types:')
print(crop_types.keys())
# Perform a spatial join, so that every plot from cur_year 
# has a {crop_key}_{year} property for the keys (years) in the crop_types dict
for year in crop_types.keys():
  print(f'Joining {year}')
  # Join all plots from {year} that intersect
  # with the current year plots
  plots_current = plots_current.sjoin(
    crop_types[year],
    how='inner',
    predicate='intersects'
  )
  # Add a column with the area of the intersection
  plots_current['intersection'] = [
    a.intersection(crop_types[year][crop_types[year].index == b
  ].geometry.values[0]).area for a, b in zip(
    plots_current.geometry.values, plots_current.index_right
  )]
  # Sort by intersection area and keep only the last row 
  # (largest intersection) for each plot
  plots_current = plots_current\
    .sort_values(by='intersection')\
    .groupby(f"{id_key}_left")\
    .last()\
    .reset_index()
  # Drop the columns that are not needed anymore
  plots_current = plots_current.drop(
    columns=['index_right', 'intersection', f"{id_key}_right"]
  )
  # rename ID_left to ID
  plots_current.rename(columns={f"{id_key}_left": id_key}, inplace=True)

# export the joined data
print('Exporting')
print(plots_current)
if not args.out:
  result_file = f'./joined-plots_{start_year}-{cur_year}.shp'
plots_current.to_file(result_file)
# Also export as CSV, replace anything after the last dot with .csv
# without geometry column
plots_current.to_csv(
  re.sub(r'\.[^.]+$', '.csv', result_file), 
  columns=[c for c in plots_current.columns if c != 'geometry']
)