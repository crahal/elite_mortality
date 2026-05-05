# Elite Mortality Data Pipeline

![coverage](https://img.shields.io/badge/Purpose-Research-yellow)
[![Generic badge](https://img.shields.io/badge/Python-3-red.svg)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/License-GNU3.0-purple.svg)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/Maintained-Yes-brightgreen.svg)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/BuildPassing-Yes-blue.svg)](https://shields.io/)
[![Generic badge](https://img.shields.io/badge/Data-Generated%20Locally-lightgrey.svg)](https://shields.io/)

A reproducible pipeline for scraping thePeerage, preparing peer and offspring
lineages, and constructing period lifetables for elite mortality research.

The current pipeline has three active scripts in `./src`: scrape, preprocess,
and build lifetables. Generated data and logs are intentionally ignored by Git;
source code and documentation are the tracked parts of the repository.

#### Repository layout

```text
src/0_thepeerage_scraper.py          scrape thePeerage into raw TSV files
src/1_thepeerage_preprocessing.py    wrangle people, peers, descendants, classes
src/2_thepeerage_lifetables.py       build consolidated period lifetables
src/old/                             archived notebooks and older pipeline code
data/thepeerage/                     generated raw, derived, clean, and lifetable data
logging/                             generated run logs
old_version/                         archived repository copy, ignored by Git
```

#### Requirements

The scripts are plain Python and expect the usual scientific Python stack:

```bash
python -m pip install pandas numpy requests beautifulsoup4 lxml tqdm
```

The scraper is network-bound and can take a long time because it walks the
current thePeerage page range. The preprocessing and lifetable scripts are
local once the raw files exist.

#### Run order

Run everything from the repository root:

```bash
cd /path/to/elite_mortality
```

For a full rebuild from the live site:

```bash
python src/0_thepeerage_scraper.py
python src/1_thepeerage_preprocessing.py
python src/2_thepeerage_lifetables.py
```

If the raw scrape already exists and you only want to rebuild the analysis
inputs and lifetables:

```bash
python src/1_thepeerage_preprocessing.py
python src/2_thepeerage_lifetables.py
```

#### thePeerage scraper

The scraper fetches current data from `https://www.thepeerage.com/`. It
discovers the current highest person page and source page from the live site,
then scrapes through those bounds. It also extends the source scrape if person
pages cite source pages above the initially discovered source bound.

Outputs:

```text
data/thepeerage/raw/entire_thepeerage.tsv
data/thepeerage/raw/british_peers_and_orders.tsv
data/thepeerage/raw/sources.tsv
```

Useful runtime controls:

```bash
THEPEERAGE_SLEEP=0.05 THEPEERAGE_MAX_RETRIES=3 python src/0_thepeerage_scraper.py
THEPEERAGE_SLEEP=0.25 python src/0_thepeerage_scraper.py
```

Key environment variables:

```text
THEPEERAGE_SLEEP                    delay after successful page fetches, default 0.1
THEPEERAGE_DISCOVERY_SLEEP          delay during page-bound discovery, default 0.02
THEPEERAGE_MAX_RETRIES              retries for transient 429/5xx failures, default 4
THEPEERAGE_BACKOFF_FACTOR           retry backoff factor in seconds, default 2
THEPEERAGE_TIMEOUT                  request timeout in seconds, default 30
THEPEERAGE_DISCOVERY_MISSES         missing person-page run that stops discovery, default 50
THEPEERAGE_SOURCE_DISCOVERY_MISSES  missing source-page run that stops discovery, default 15
```

Missing pages inside the valid range are expected on thePeerage. The scraper
skips HTTP 404/410 pages without retrying them.

#### Preprocessing

Run:

```bash
python src/1_thepeerage_preprocessing.py
```

By default, preprocessing uses deterministic midpoint-style imputation for
incomplete dates. If a usable year exists but month and/or day are missing, it
fills the missing pieces with fixed dates and marks the date as synthetic:
year-only dates become July 1, and month-only dates become the middle of that
month.

The plain command now uses deterministic filling:

```bash
python src/1_thepeerage_preprocessing.py
```

To explicitly request the same behavior:

```bash
python src/1_thepeerage_preprocessing.py --date-imputation deterministic
```

To use the old seedable random fill instead:

```bash
python src/1_thepeerage_preprocessing.py --date-imputation random --random-seed 20260505
```

The script consumes:

```text
data/thepeerage/raw/entire_thepeerage.tsv
data/thepeerage/raw/british_peers_and_orders.tsv
data/thepeerage/raw/sources.tsv
```

It writes old-compatible derived files:

```text
data/thepeerage/derived/british_peers_and_orders_full.tsv
data/thepeerage/derived/british_peers_and_orders_child.tsv
data/thepeerage/derived/british_peers_and_orders_grandchild.tsv
data/thepeerage/derived/british_peers_and_orders_greatgrandchild.tsv
```

It also writes lifetable-ready files:

```text
data/thepeerage/clean/full_df.tsv
data/thepeerage/clean/elite_df.tsv
data/thepeerage/clean/child_df.tsv
data/thepeerage/clean/grandchild_df.tsv
data/thepeerage/clean/greatgrandchild_df.tsv
data/thepeerage/clean/offspring_df.tsv
data/thepeerage/clean/preprocessing_summary.tsv
```

Generation outputs:

```text
data/thepeerage/clean/first_generation_peers.tsv
data/thepeerage/clean/secondary_generation.tsv
data/thepeerage/clean/tertiary_peers.tsv
```

Generation rule:

```text
first_generation_peers = every peer used as an anchor
secondary_generation   = children of any peer
tertiary_peers         = grandchildren of any peer
```

This means a person can be included through the nearest peer in their ancestry.
For example, if B is the child of peer A, C is the child of peer B, and D is
the child of C, then D is included as a grandchild of peer B even though D is a
great-grandchild of peer A.

Bad or dropped records are written to:

```text
data/thepeerage/bad_stuff/
```

#### Offspring classes

Preprocessing writes the class manifest consumed by the lifetable script:

```text
data/thepeerage/clean/offspring_lifetable_classes.tsv
data/thepeerage/clean/offspring_classes/
```

The manifest includes aggregate classes:

```text
children
grandchildren
greatgrandchildren
all_offspring
```

It also includes peer/nonpeer lineage splits: children by self peer status,
grandchildren by parent peer status and self peer status, and
great-grandchildren by grandparent, parent, and self peer status.

The `all_offspring` file is de-duplicated by person ID across children,
grandchildren, and great-grandchildren.

The manifest also includes `all_offspring_only_peer_type_*` classes. These are
de-duplicated all-offspring samples where the person's relevant peer ancestors
across the child, grandchild, and great-grandchild trees have exactly one
unique peer type, such as only baronet-linked offspring or only baron-linked
offspring.

#### Lifetables

Run:

```bash
python src/2_thepeerage_lifetables.py
```

Output:

```text
data/thepeerage/lifetables/thepeerage_lifetables.tsv
```

The output is one long TSV with columns identifying the offspring class, sex,
calendar year, period width, age width, and lifetable method. It includes `M`,
`F`, and `T`, and all requested table types:

```text
1x1, 1x5, 1x10, 5x1, 5x5, 5x10, 10x1, 10x5, 10x10
```

By default the script builds rolling period lifetables for start years 1500
through the latest complete observed year, closes tables at age 110+, and uses
Kannisto-style old-age smoothing/extrapolation.

Useful options:

```bash
python src/2_thepeerage_lifetables.py --start-year 1700
python src/2_thepeerage_lifetables.py --end-year 1950
python src/2_thepeerage_lifetables.py --classes children grandchildren all_offspring
```

#### Logs

Each script creates a timestamped log under:

```text
logging/
```

Log messages use repo-relative paths, not absolute local paths. This keeps logs
stable after moving the repository.

#### Current local output check

After the latest completed run, the consolidated lifetable file contained
25,587,351 lines including the header. Recheck after a new run with:

```bash
wc -l data/thepeerage/lifetables/thepeerage_lifetables.tsv
wc -l data/thepeerage/clean/offspring_lifetable_classes.tsv
tail -n 20 logging/*_thepeerage_lifetables.log
```

To do a lightweight syntax check without writing bytecode into `./src`:

```bash
PYTHONPYCACHEPREFIX=/tmp/thepeerage_pycache python -m py_compile \
  src/1_thepeerage_preprocessing.py \
  src/2_thepeerage_lifetables.py
```

#### Git tracking

The repository keeps source and documentation in Git while ignoring generated
or archived material:

```text
src/                 tracked
data/                ignored
logging/             ignored
old_version/         ignored
src/__pycache__/     ignored
src/.ipynb_checkpoints/ ignored
```

The canonical ignore file is `.gitignore`. A matching `.git_ignore` is also
kept in the root because that name was used during the repository move.

#### License

This work is made available under a GNU General Public License v3.0. See
`LICENSE`.
