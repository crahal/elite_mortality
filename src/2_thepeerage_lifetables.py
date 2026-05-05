#!/usr/bin/env python3
"""Build period lifetables for thePeerage offspring samples.

The output is one long TSV with one row per class/sex/year/table-type/age cell.
Table types use AGE_INTERVAL x PERIOD_INTERVAL notation, so 5x10 means
abridged five-year age intervals over a ten-year rolling calendar period.
"""

from __future__ import annotations

import argparse
import logging
import math
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_INPUT_DIR = PROJECT_ROOT / "data" / "thepeerage" / "clean"
DEFAULT_OUTPUT_FILE = PROJECT_ROOT / "data" / "thepeerage" / "lifetables" / "thepeerage_lifetables.tsv"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logging"
DEFAULT_CLASS_MANIFEST_NAME = "offspring_lifetable_classes.tsv"

BASE_OFFSPRING_FILES = {
    "children": "child_df.tsv",
    "grandchildren": "grandchild_df.tsv",
    "greatgrandchildren": "greatgrandchild_df.tsv",
    "all_offspring": "offspring_df.tsv",
}
SEX_LABELS = ("M", "F", "T")
SEX_TO_INDEX = {"M": 0, "F": 1}
AGE_WIDTHS = (1, 5, 10)
PERIOD_WIDTHS = (1, 5, 10)
RADIX = 100_000.0
DEFAULT_MIN_START_YEAR = 1500


def display_path(path: Path | str) -> str:
    path = Path(path)
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


@dataclass(frozen=True)
class AgeGroup:
    age_start: int
    age_end: int | None
    width: int | None
    label: str
    base_age_indices: tuple[int, ...]


def configure_logging(log_dir: Path) -> Path:
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_file = log_dir / f"{timestamp}_thepeerage_lifetables.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    logging.info("Logging to %s", display_path(log_file))
    return log_file


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build thePeerage offspring lifetables in 1x1 through 10x10 formats."
    )
    parser.add_argument("--input-dir", type=Path, default=DEFAULT_INPUT_DIR)
    parser.add_argument("--output-file", type=Path, default=DEFAULT_OUTPUT_FILE)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument(
        "--class-manifest",
        type=Path,
        default=None,
        help="Class manifest from preprocessing. Default: input-dir/offspring_lifetable_classes.tsv.",
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Inclusive first calendar year. Default: max(1500, earliest birth year), matching the old pipeline.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Exclusive calendar end year. Default is the latest complete year before the observation date.",
    )
    parser.add_argument(
        "--observation-end",
        type=str,
        default=None,
        help="Right-censoring date for people without a death date, YYYY-MM-DD. Default: max(lastedit).",
    )
    parser.add_argument("--max-age", type=int, default=110)
    parser.add_argument(
        "--min-table-exposure",
        type=float,
        default=1.0,
        help="Skip class/sex/year/type tables with less total exposure than this.",
    )
    parser.add_argument(
        "--min-smoothing-age",
        type=int,
        default=85,
        help="Earliest age at which old-age Kannisto smoothing may replace raw rates.",
    )
    parser.add_argument(
        "--min-fit-points",
        type=int,
        default=5,
        help="Minimum observed age cells required for a Kannisto fit.",
    )
    parser.add_argument(
        "--classes",
        nargs="+",
        default=None,
        help="Optional subset of offspring classes from the manifest.",
    )
    parser.add_argument(
        "--table-types",
        nargs="+",
        default=[f"{age}x{period}" for age in AGE_WIDTHS for period in PERIOD_WIDTHS],
        help="Subset of table types to build, e.g. 1x1 5x10.",
    )
    parser.add_argument(
        "--chunk-tables",
        type=int,
        default=250,
        help="Number of completed lifetables to buffer before appending to disk.",
    )
    return parser.parse_args()


def load_class_manifest(input_dir: Path, manifest_path: Path | None) -> pd.DataFrame:
    path = manifest_path if manifest_path is not None else input_dir / DEFAULT_CLASS_MANIFEST_NAME
    if path.exists():
        manifest = pd.read_csv(path, sep="\t", dtype=str, keep_default_na=False)
        required = {"offspring_class", "file"}
        missing = required - set(manifest.columns)
        if missing:
            raise ValueError(f"{display_path(path)} is missing required columns: {', '.join(sorted(missing))}")
        return manifest

    rows = [
        {
            "offspring_class": class_name,
            "file": file_name,
            "generation": class_name,
            "aggregate": "1",
            "rows": "",
            "criteria": "fallback_base_class",
            "description": "Fallback base class; preprocessing manifest not found.",
        }
        for class_name, file_name in BASE_OFFSPRING_FILES.items()
    ]
    return pd.DataFrame(rows)


def class_file_map(manifest: pd.DataFrame) -> dict[str, str]:
    return dict(zip(manifest["offspring_class"], manifest["file"]))


def resolve_class_path(input_dir: Path, file_name: str) -> Path:
    path = Path(file_name)
    if path.is_absolute():
        return path
    return input_dir / path


def decimal_year(dates: pd.Series) -> pd.Series:
    parts = dates.astype("string").str.extract(r"^\s*(\d{1,4})-(\d{1,2})-(\d{1,2})\s*$")
    year = pd.to_numeric(parts[0], errors="coerce")
    month = pd.to_numeric(parts[1], errors="coerce")
    day = pd.to_numeric(parts[2], errors="coerce")
    valid = year.notna() & month.between(1, 12) & day.between(1, 31)

    year_int = year.fillna(1).astype(int)
    month_int = month.fillna(1).astype(int)
    leap = ((year_int % 4 == 0) & (year_int % 100 != 0)) | (year_int % 400 == 0)
    month_lengths_common = np.array([31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])
    month_lengths_leap = np.array([31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31])

    day_of_year = np.zeros(len(dates), dtype="float64")
    for month_number in range(1, 13):
        before_common = month_lengths_common[: month_number - 1].sum()
        before_leap = month_lengths_leap[: month_number - 1].sum()
        mask = month_int == month_number
        day_of_year[mask] = np.where(leap[mask], before_leap, before_common)

    max_day = np.array(
        [
            month_lengths_leap[m - 1] if is_leap else month_lengths_common[m - 1]
            for m, is_leap in zip(month_int, leap)
        ],
        dtype="float64",
    )
    valid = valid & (day <= max_day)
    days_in_year = np.where(leap, 366.0, 365.0)
    value = year.astype("float64") + (day_of_year + day.astype("float64") - 1.0) / days_in_year
    return pd.Series(value, index=dates.index).where(valid)


def parse_observation_end(
    input_dir: Path,
    classes: list[str],
    class_files: dict[str, str],
    explicit_date: str | None,
) -> float:
    if explicit_date:
        parsed = pd.to_datetime(explicit_date, errors="raise")
        return float(decimal_year(pd.Series([parsed.strftime("%Y-%m-%d")])).iloc[0])

    latest = pd.NaT
    for class_name in classes:
        path = resolve_class_path(input_dir, class_files[class_name])
        if not path.exists():
            continue
        lastedit = pd.read_csv(path, sep="\t", usecols=["lastedit"], low_memory=False)["lastedit"]
        parsed = pd.to_datetime(lastedit, format="%d %b %Y", errors="coerce")
        if parsed.notna().any():
            latest = max(latest, parsed.max()) if pd.notna(latest) else parsed.max()

    if pd.isna(latest):
        latest = pd.Timestamp(datetime.now().date())
    return float(decimal_year(pd.Series([latest.strftime("%Y-%m-%d")])).iloc[0])


def load_offspring(
    input_dir: Path,
    class_name: str,
    observation_end: float,
    class_files: dict[str, str],
) -> pd.DataFrame:
    path = resolve_class_path(input_dir, class_files[class_name])
    if not path.exists():
        raise FileNotFoundError(f"Missing input for {class_name}: {display_path(path)}")

    columns = ["ID", "gender", "born_dt", "died_dt", "lastedit"]
    df = pd.read_csv(path, sep="\t", usecols=columns, low_memory=False)
    df = df[df["gender"].isin(("M", "F"))].copy()
    df["birth"] = decimal_year(df["born_dt"])
    df["death"] = decimal_year(df["died_dt"])
    df = df[df["birth"].notna()].copy()
    df["death_observed"] = df["death"].notna() & (df["death"] <= observation_end)
    df["exit"] = np.where(df["death_observed"], df["death"], observation_end)
    df = df[df["birth"] < df["exit"]].copy()
    df["sex_index"] = df["gender"].map(SEX_TO_INDEX).astype("int8")
    return df[["ID", "gender", "sex_index", "birth", "death", "exit", "death_observed"]].reset_index(drop=True)


def default_start_year(input_dir: Path, classes: list[str], class_files: dict[str, str]) -> int:
    minimum = None
    for class_name in classes:
        path = resolve_class_path(input_dir, class_files[class_name])
        if not path.exists():
            continue
        years = pd.read_csv(path, sep="\t", usecols=["born_year"], low_memory=False)["born_year"]
        value = pd.to_numeric(years, errors="coerce").min()
        if pd.notna(value):
            minimum = int(value) if minimum is None else min(minimum, int(value))
    if minimum is None:
        raise ValueError("Could not infer a start year from the input files.")
    return max(DEFAULT_MIN_START_YEAR, minimum)


def age_groups(age_width: int, max_age: int) -> list[AgeGroup]:
    groups: list[AgeGroup] = []
    if age_width == 1:
        starts = list(range(0, max_age))
        for start in starts:
            groups.append(AgeGroup(start, start + 1, 1, str(start), (start,)))
        groups.append(AgeGroup(max_age, None, None, f"{max_age}+", (max_age,)))
        return groups

    if age_width == 5:
        groups.append(AgeGroup(0, 1, 1, "0", (0,)))
        groups.append(AgeGroup(1, 5, 4, "1-4", tuple(range(1, 5))))
        starts = range(5, max_age, 5)
    elif age_width == 10:
        groups.append(AgeGroup(0, 1, 1, "0", (0,)))
        groups.append(AgeGroup(1, 10, 9, "1-9", tuple(range(1, 10))))
        starts = range(10, max_age, 10)
    else:
        raise ValueError(f"Unsupported age width: {age_width}")

    for start in starts:
        end = min(start + age_width, max_age)
        label = f"{start}-{end - 1}"
        groups.append(AgeGroup(start, end, end - start, label, tuple(range(start, end))))
    groups.append(AgeGroup(max_age, None, None, f"{max_age}+", (max_age,)))
    return groups


def split_table_type(table_type: str) -> tuple[int, int]:
    try:
        age_width, period_width = table_type.lower().split("x", maxsplit=1)
        age_width_int = int(age_width)
        period_width_int = int(period_width)
    except ValueError as exc:
        raise ValueError(f"Invalid table type {table_type!r}; expected e.g. 5x10") from exc

    if age_width_int not in AGE_WIDTHS or period_width_int not in PERIOD_WIDTHS:
        raise ValueError(f"Unsupported table type {table_type!r}")
    return age_width_int, period_width_int


def build_lexis_1x1(
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    max_age: int,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    years = np.arange(start_year, end_year, dtype=int)
    exposures = np.zeros((2, len(years), max_age + 1), dtype="float64")
    deaths = np.zeros_like(exposures)
    final_time = float(end_year)
    first_time = float(start_year)

    for row in df.itertuples(index=False):
        birth = float(row.birth)
        exit_time = min(float(row.exit), final_time)
        if exit_time <= first_time or birth >= final_time:
            continue

        t = max(birth, first_time)
        sex = int(row.sex_index)
        while t < exit_time - 1e-10:
            calendar_year = int(math.floor(t))
            if calendar_year < start_year or calendar_year >= end_year:
                break

            age_now = t - birth
            if age_now < 0:
                age_index = 0
            elif age_now >= max_age:
                age_index = max_age
            else:
                age_index = int(math.floor(age_now))

            next_calendar = float(calendar_year + 1)
            if age_index >= max_age:
                next_birthday = float("inf")
            else:
                next_birthday = birth + age_index + 1.0
            next_t = min(exit_time, next_calendar, next_birthday)
            if next_t <= t + 1e-10:
                next_t = min(exit_time, next_calendar)
            if next_t > t:
                exposures[sex, calendar_year - start_year, age_index] += next_t - t
            t = next_t

        if bool(row.death_observed):
            death = float(row.death)
            if first_time <= death < final_time and death >= birth:
                death_year = int(math.floor(death))
                age_at_death = death - birth
                age_index = max_age if age_at_death >= max_age else int(math.floor(age_at_death))
                if 0 <= age_index <= max_age:
                    deaths[sex, death_year - start_year, age_index] += 1.0

    return exposures, deaths, years


def rolling_period_sum(values: np.ndarray, period_width: int) -> np.ndarray:
    if period_width == 1:
        return values.copy()
    zero = np.zeros((values.shape[0], 1, values.shape[2]), dtype=values.dtype)
    cumulative = np.concatenate([zero, np.cumsum(values, axis=1)], axis=1)
    return cumulative[:, period_width:, :] - cumulative[:, :-period_width, :]


def aggregate_age(values: np.ndarray, groups: list[AgeGroup]) -> np.ndarray:
    out = np.zeros((values.shape[0], values.shape[1], len(groups)), dtype="float64")
    for index, group in enumerate(groups):
        out[:, :, index] = values[:, :, list(group.base_age_indices)].sum(axis=2)
    return out


def expit(x: np.ndarray) -> np.ndarray:
    return 1.0 / (1.0 + np.exp(-x))


def fit_kannisto(
    age_starts: np.ndarray,
    widths: np.ndarray,
    mx: np.ndarray,
    deaths: np.ndarray,
    exposures: np.ndarray,
    min_smoothing_age: int,
    min_fit_points: int,
) -> tuple[np.ndarray, np.ndarray, int | None, int, str]:
    adjusted = mx.copy()
    methods = np.where(np.isfinite(mx), "raw", "missing").astype(object)
    closed = np.isfinite(widths)

    smoothing_candidates = np.where((age_starts >= min_smoothing_age) & closed)[0]
    if len(smoothing_candidates) == 0:
        return adjusted, methods, None, 0, "none"

    invalid_old = smoothing_candidates[
        (~np.isfinite(mx[smoothing_candidates]))
        | (mx[smoothing_candidates] <= 0)
        | (exposures[smoothing_candidates] <= 0)
    ]
    smooth_start_index = int(invalid_old[0]) if len(invalid_old) else int(smoothing_candidates[0])
    smooth_start_age = int(age_starts[smooth_start_index])

    fit_mask = (
        closed
        & (age_starts < smooth_start_age)
        & (age_starts >= max(50, smooth_start_age - 35))
        & np.isfinite(mx)
        & (mx > 0)
        & np.isfinite(exposures)
        & (exposures > 0)
    )
    fit_indices = np.where(fit_mask)[0]
    if len(fit_indices) > max(min_fit_points, 12):
        fit_indices = fit_indices[-max(min_fit_points, 12) :]

    project_indices = np.where(age_starts >= smooth_start_age)[0]
    if len(fit_indices) >= min_fit_points:
        x = age_starts[fit_indices].astype("float64") + np.nan_to_num(widths[fit_indices], nan=5.0) / 2.0
        y_mx = np.clip(mx[fit_indices].astype("float64"), 1e-8, 0.95)
        y = np.log(y_mx / (1.0 - y_mx))
        weights = np.sqrt(np.maximum(deaths[fit_indices], 1.0))
        try:
            slope, intercept = np.polyfit(x, y, 1, w=weights)
            slope = max(float(slope), 0.005)
            x_project = age_starts[project_indices].astype("float64")
            x_project += np.nan_to_num(widths[project_indices], nan=5.0) / 2.0
            predicted = np.clip(expit(intercept + slope * x_project), 1e-8, 1.0)
            adjusted[project_indices] = predicted
            methods[project_indices] = "kannisto"
            return adjusted, methods, smooth_start_age, len(fit_indices), "kannisto"
        except (ValueError, np.linalg.LinAlgError, FloatingPointError):
            pass

    prior = np.where((age_starts < smooth_start_age) & np.isfinite(mx) & (mx > 0))[0]
    if len(prior):
        base_mx = float(mx[prior[-1]])
    else:
        positive = np.where(np.isfinite(mx) & (mx > 0))[0]
        base_mx = float(mx[positive[-1]]) if len(positive) else 0.01
    projected_age = age_starts[project_indices].astype("float64") - smooth_start_age
    adjusted[project_indices] = np.clip(base_mx * np.exp(0.08 * projected_age), 1e-8, 1.0)
    methods[project_indices] = "fallback_gompertz"
    return adjusted, methods, smooth_start_age, len(fit_indices), "fallback_gompertz"


def make_nax(age_starts: np.ndarray, widths: np.ndarray, mx: np.ndarray, sex: str) -> np.ndarray:
    nax = np.full_like(mx, np.nan, dtype="float64")
    for index, (age, width, rate) in enumerate(zip(age_starts, widths, mx)):
        if not np.isfinite(rate):
            continue
        if not np.isfinite(width):
            continue
        if age == 0 and width == 1:
            if sex == "M":
                nax[index] = 0.330 if rate >= 0.107 else 0.045 + 2.684 * rate
            elif sex == "F":
                nax[index] = 0.350 if rate >= 0.107 else 0.053 + 2.800 * rate
            else:
                nax[index] = 0.340 if rate >= 0.107 else 0.049 + 2.742 * rate
        elif age == 1 and width == 4:
            if sex == "M":
                nax[index] = 1.352 if rate >= 0.107 else 1.651 + 2.816 * rate
            elif sex == "F":
                nax[index] = 1.361 if rate >= 0.107 else 1.522 + 1.518 * rate
            else:
                nax[index] = 1.356 if rate >= 0.107 else 1.586 + 2.167 * rate
        else:
            nax[index] = width / 2.0
    return nax


def construct_lifetable(
    exposures: np.ndarray,
    deaths: np.ndarray,
    groups: list[AgeGroup],
    sex: str,
    min_smoothing_age: int,
    min_fit_points: int,
) -> tuple[pd.DataFrame, dict[str, object]]:
    age_starts = np.array([group.age_start for group in groups], dtype="int64")
    widths = np.array([np.nan if group.width is None else group.width for group in groups], dtype="float64")
    with np.errstate(divide="ignore", invalid="ignore"):
        mx_raw = deaths / exposures
    mx_raw = np.where(exposures > 0, mx_raw, np.nan)

    mx, mx_methods, smoothing_start_age, fit_points, smoothing_method = fit_kannisto(
        age_starts,
        widths,
        mx_raw,
        deaths,
        exposures,
        min_smoothing_age,
        min_fit_points,
    )
    nax = make_nax(age_starts, widths, mx, sex)
    qx = np.full_like(mx, np.nan, dtype="float64")
    for index, (width, rate, ax) in enumerate(zip(widths, mx, nax)):
        if index == len(mx) - 1:
            qx[index] = 1.0 if np.isfinite(rate) and rate > 0 else np.nan
        elif np.isfinite(width) and np.isfinite(rate) and np.isfinite(ax):
            qx[index] = (width * rate) / (1.0 + (width - ax) * rate)
            qx[index] = min(max(qx[index], 0.0), 1.0)

    lx = np.full_like(mx, np.nan, dtype="float64")
    ndx = np.full_like(mx, np.nan, dtype="float64")
    nLx = np.full_like(mx, np.nan, dtype="float64")
    lx[0] = RADIX
    for index in range(len(mx)):
        if not np.isfinite(lx[index]) or not np.isfinite(qx[index]):
            break
        ndx[index] = lx[index] * qx[index]
        if index == len(mx) - 1:
            if np.isfinite(mx[index]) and mx[index] > 0:
                nLx[index] = lx[index] / mx[index]
        else:
            width = widths[index]
            nLx[index] = width * (lx[index] - ndx[index]) + nax[index] * ndx[index]
            lx[index + 1] = lx[index] - ndx[index]

    Tx = np.full_like(mx, np.nan, dtype="float64")
    running = 0.0
    valid_tail = True
    for index in range(len(mx) - 1, -1, -1):
        if not np.isfinite(nLx[index]):
            valid_tail = False
        if valid_tail:
            running += nLx[index]
            Tx[index] = running

    with np.errstate(divide="ignore", invalid="ignore"):
        ex = Tx / lx

    result = pd.DataFrame(
        {
            "age_start": age_starts,
            "age_end": [group.age_end for group in groups],
            "age_interval_width": widths,
            "age": [group.label for group in groups],
            "open_age": [group.age_end is None for group in groups],
            "Dx": deaths,
            "Px": exposures,
            "nMx_raw": mx_raw,
            "nMx": mx,
            "nMx_method": mx_methods,
            "nax": nax,
            "nqx": qx,
            "lx": lx,
            "ndx": ndx,
            "nLx": nLx,
            "Tx": Tx,
            "ex": ex,
        }
    )
    diagnostics = {
        "table_exposure": float(np.nansum(exposures)),
        "table_deaths": float(np.nansum(deaths)),
        "age_cells_with_exposure": int(np.sum(exposures > 0)),
        "age_cells_with_deaths": int(np.sum(deaths > 0)),
        "smoothing_start_age": smoothing_start_age,
        "smoothing_fit_points": fit_points,
        "smoothing_method": smoothing_method,
        "valid_lifetable": bool(np.isfinite(ex[0])),
    }
    return result, diagnostics


def append_rows(output_file: Path, rows: list[pd.DataFrame], write_header: bool) -> bool:
    if not rows:
        return write_header
    output_file.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.concat(rows, ignore_index=True)
    frame.to_csv(
        output_file,
        sep="\t",
        index=False,
        mode="w" if write_header else "a",
        header=write_header,
        encoding="utf-8",
    )
    return False


def build_for_class(
    class_name: str,
    df: pd.DataFrame,
    start_year: int,
    end_year: int,
    table_types: list[str],
    max_age: int,
    min_table_exposure: float,
    min_smoothing_age: int,
    min_fit_points: int,
    output_file: Path,
    write_header: bool,
    chunk_tables: int,
    observation_end: float,
) -> bool:
    logging.info("Building 1x1 Lexis arrays for %s (%s people)", class_name, len(df))
    exposures_1x1, deaths_1x1, base_years = build_lexis_1x1(df, start_year, end_year, max_age)

    table_specs = [(*split_table_type(table_type), table_type) for table_type in table_types]
    rows: list[pd.DataFrame] = []
    tables_written = 0

    for period_width in PERIOD_WIDTHS:
        specs_for_period = [spec for spec in table_specs if spec[1] == period_width]
        if not specs_for_period:
            continue
        if len(base_years) < period_width:
            continue

        period_years = base_years[: len(base_years) - period_width + 1]
        period_exposures = rolling_period_sum(exposures_1x1, period_width)
        period_deaths = rolling_period_sum(deaths_1x1, period_width)
        total_exposures = period_exposures.sum(axis=0, keepdims=True)
        total_deaths = period_deaths.sum(axis=0, keepdims=True)
        sex_period_exposures = np.concatenate([period_exposures, total_exposures], axis=0)
        sex_period_deaths = np.concatenate([period_deaths, total_deaths], axis=0)

        for age_width, _, table_type in specs_for_period:
            groups = age_groups(age_width, max_age)
            age_exposures = aggregate_age(sex_period_exposures, groups)
            age_deaths = aggregate_age(sex_period_deaths, groups)

            for sex_index, sex in enumerate(SEX_LABELS):
                for year_index, year in enumerate(period_years):
                    exposure_vector = age_exposures[sex_index, year_index, :]
                    death_vector = age_deaths[sex_index, year_index, :]
                    if np.nansum(exposure_vector) < min_table_exposure:
                        continue

                    lifetable, diagnostics = construct_lifetable(
                        exposure_vector,
                        death_vector,
                        groups,
                        sex,
                        min_smoothing_age,
                        min_fit_points,
                    )
                    lifetable.insert(0, "offspring_class", class_name)
                    lifetable.insert(1, "sex", sex)
                    lifetable.insert(2, "year", int(year))
                    lifetable.insert(3, "period_start_year", int(year))
                    lifetable.insert(4, "period_end_year", int(year + period_width))
                    lifetable.insert(5, "lifetable_type", table_type)
                    lifetable.insert(6, "age_width", age_width)
                    lifetable.insert(7, "period_width", period_width)
                    lifetable["radix"] = RADIX
                    lifetable["max_age"] = max_age
                    lifetable["observation_end"] = observation_end
                    for key, value in diagnostics.items():
                        lifetable[key] = value
                    rows.append(lifetable)
                    tables_written += 1

                    if len(rows) >= chunk_tables:
                        write_header = append_rows(output_file, rows, write_header)
                        rows.clear()

    write_header = append_rows(output_file, rows, write_header)
    logging.info("Wrote %s lifetables for %s", tables_written, class_name)
    return write_header


def run(args: argparse.Namespace) -> Path:
    start = time.monotonic()
    log_file = configure_logging(args.log_dir)
    if args.output_file.exists():
        args.output_file.unlink()

    table_types = []
    for table_type in args.table_types:
        age_width, period_width = split_table_type(table_type)
        normalized = f"{age_width}x{period_width}"
        if normalized not in table_types:
            table_types.append(normalized)

    manifest = load_class_manifest(args.input_dir, args.class_manifest)
    class_files = class_file_map(manifest)
    classes = list(args.classes) if args.classes is not None else list(manifest["offspring_class"])
    unknown_classes = sorted(set(classes) - set(class_files))
    if unknown_classes:
        raise ValueError(
            "Unknown offspring class(es): "
            + ", ".join(unknown_classes)
            + ". Available classes: "
            + ", ".join(class_files)
        )

    observation_end = parse_observation_end(args.input_dir, classes, class_files, args.observation_end)
    end_year = args.end_year if args.end_year is not None else int(math.floor(observation_end))
    start_year = (
        args.start_year
        if args.start_year is not None
        else default_start_year(args.input_dir, classes, class_files)
    )
    if end_year <= start_year:
        raise ValueError(f"end_year must exceed start_year; got {start_year=} and {end_year=}")

    logging.info("Input directory: %s", display_path(args.input_dir))
    logging.info(
        "Class manifest: %s",
        display_path(
            args.class_manifest if args.class_manifest is not None else args.input_dir / DEFAULT_CLASS_MANIFEST_NAME
        ),
    )
    logging.info("Output file: %s", display_path(args.output_file))
    logging.info("Classes: %s", ", ".join(classes))
    logging.info("Table types: %s", ", ".join(table_types))
    logging.info("Calendar years: [%s, %s)", start_year, end_year)
    logging.info("Observation end as decimal year: %.6f", observation_end)
    logging.info("Close lifetables at age %s+", args.max_age)

    write_header = True
    for class_name in classes:
        class_df = load_offspring(args.input_dir, class_name, observation_end, class_files)
        logging.info(
            "%s: loaded %s M/F records after date/censoring checks",
            class_name,
            len(class_df),
        )
        write_header = build_for_class(
            class_name=class_name,
            df=class_df,
            start_year=start_year,
            end_year=end_year,
            table_types=table_types,
            max_age=args.max_age,
            min_table_exposure=args.min_table_exposure,
            min_smoothing_age=args.min_smoothing_age,
            min_fit_points=args.min_fit_points,
            output_file=args.output_file,
            write_header=write_header,
            chunk_tables=args.chunk_tables,
            observation_end=observation_end,
        )

    duration = time.monotonic() - start
    logging.info("Completed lifetable construction in %.1fs", duration)
    logging.info("Log file: %s", display_path(log_file))
    logging.info("Output file: %s", display_path(args.output_file))
    return args.output_file


if __name__ == "__main__":
    output = run(parse_args())
    print(f"Lifetable output: {display_path(output)}")
