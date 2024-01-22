# Uni Bonn - Crop rotation joiner

A simple script to determine crop rotations from single plot geometries (vector data).
Joins crop type information from multiple years using spatial joins
and largest area overlap to determine crop rotations on a single plot level.

## Requirements

- Python 3.6+
- [GeoPandas](https://geopandas.org/)
- [pyogrio](https://pypi.org/project/pyogrio/)

## Usage

```sh
  python join-plots.py --cur <current_year_file> --hist <historical_files_folder> --out <result_file>
```

## Options

- `--help` - Show help
- `--cur` - Path to the current year file (vector geometry file, must be readable by GeoPandas/pyogrio)
- `--hist` - Path to the historical files folder (vector geometry files, must be readable by GeoPandas/pyogrio)
- `--out` - Path to the output file (vector geometry file AND CSV, (vector geometry must be writable by GeoPandas/pyogrio)
- `--key` - Crop key to use for the join (default: `CODE`) -> will be converted to {key}_{year} in the output
- `--id` - Plot ID key to use for the join (default: `ID`)

## Example

```sh
  python join-plots.py --cur ./test/input/2023.json --hist ./test/input --out ./test/output/joined-plots.shp
```