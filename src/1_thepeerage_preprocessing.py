import argparse
import logging
import re
import time
import random
from calendar import isleap
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DEFAULT_RAW_DIR = PROJECT_ROOT / "data" / "thepeerage" / "raw"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "thepeerage"
DEFAULT_LOG_DIR = PROJECT_ROOT / "logging"

SIZE_OF_DAY = 1.0 / 366.0
SIZE_OF_SECOND = SIZE_OF_DAY / (24.0 * 60.0 * 60.0)

MISSING_TOKENS = {"", "n/a", "na", "nan", "none", "null", "<na>"}
MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
DATE_IMPUTATION = "deterministic"
DATE_RNG = random.Random(20260505)


def display_path(path):
    path = Path(path)
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


@dataclass
class OutputPaths:
    raw: Path
    derived: Path
    clean: Path
    bad: Path
    classes: Path


def configure_logging(log_dir):
    log_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    log_file = log_dir / f"{timestamp}_thepeerage_preprocessing.log"

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    logger.addHandler(console_handler)

    logging.info("Logging to %s", display_path(log_file))
    return log_file


def build_output_paths(output_dir):
    paths = OutputPaths(
        raw=output_dir / "raw",
        derived=output_dir / "derived",
        clean=output_dir / "clean",
        bad=output_dir / "bad_stuff",
        classes=output_dir / "clean" / "offspring_classes",
    )
    for path in (paths.derived, paths.clean, paths.bad, paths.classes):
        path.mkdir(parents=True, exist_ok=True)
    return paths


def write_tsv(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep="\t", index=False, encoding="utf-8")
    logging.info("Wrote %s rows to %s", len(df), display_path(path))


def write_bad(df, path):
    write_tsv(df, path)


def normalize_missing_values(df):
    df = df.copy()
    for column in df.columns:
        if df[column].dtype == object:
            values = df[column].astype("string").str.strip()
            df[column] = values.mask(values.str.lower().isin(MISSING_TOKENS), pd.NA)
    return df


def normalize_id_column(df, column):
    df = df.copy()
    df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    return df


def read_raw_inputs(raw_dir):
    required = {
        "full": raw_dir / "entire_thepeerage.tsv",
        "peers": raw_dir / "british_peers_and_orders.tsv",
        "sources": raw_dir / "sources.tsv",
    }
    missing = [display_path(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError(
            "Missing raw input file(s): "
            + ", ".join(missing)
            + ". Run src/0_thepeerage_scraper.py first."
        )

    full = pd.read_csv(required["full"], sep="\t", dtype=str, keep_default_na=False, low_memory=False)
    peers = pd.read_csv(required["peers"], sep="\t", dtype=str, keep_default_na=False, low_memory=False)
    sources = pd.read_csv(required["sources"], sep="\t", dtype=str, keep_default_na=False, low_memory=False)

    full = normalize_id_column(normalize_missing_values(full), "ID")
    full = normalize_id_column(full, "Page")
    peers = normalize_id_column(normalize_missing_values(peers), "id")
    sources = normalize_id_column(normalize_missing_values(sources), "Page")

    logging.info("Loaded %s raw people, %s peer rows, %s source rows", len(full), len(peers), len(sources))
    return full, peers, sources


def drop_duplicate_people(full, bad_dir):
    missing_id = full[full["ID"].isna()].copy()
    write_bad(missing_id, bad_dir / "missing_person_ID.csv")
    full = full[full["ID"].notna()].copy()

    missing_page = full[full["Page"].isna()].copy()
    write_bad(missing_page, bad_dir / "missing_person_Page.csv")

    duplicated = full[full.duplicated("ID", keep=False)].sort_values("ID")
    write_bad(duplicated, bad_dir / "duplicate_person_ID.csv")
    full = full.drop_duplicates("ID", keep="first").copy()
    full["ID"] = full["ID"].astype("int64")
    return full


def parse_id_list(value):
    if pd.isna(value):
        return []
    ids = []
    for token in re.split(r"[;\s,]+", str(value)):
        if token.isdigit():
            ids.append(int(token))
    return ids


def join_ids(values):
    ids = sorted({int(value) for value in values if pd.notna(value)})
    return ";".join(str(value) for value in ids)


def parse_type_list(value):
    if pd.isna(value):
        return []
    return sorted({token for token in str(value).split(";") if token})


def join_types(values):
    return ";".join(sorted({str(value) for value in values if str(value)}))


def class_slug(value):
    slug = re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")
    return slug or "unknown"


def annotate_peer_flags(full, peers, bad_dir):
    peers = peers[peers["id"].notna()].copy()
    peers["id"] = peers["id"].astype("int64")
    peers["type"] = peers["type"].astype("string").str.strip()

    missing_peer_ids = peers[~peers["id"].isin(full["ID"])].copy()
    write_bad(missing_peer_ids, bad_dir / "peers_not_in_raw_ID.csv")
    peers = peers[peers["id"].isin(full["ID"])].copy()

    type_lookup = peers.groupby("id")["type"].agg(lambda s: ";".join(sorted(set(s.dropna()))))
    peer_ids = set(type_lookup.index.astype(int))
    peer_types = sorted({value for value in peers["type"].dropna().unique()})

    annotated = full.copy()
    annotated["peer_types"] = annotated["ID"].map(type_lookup).fillna("")
    annotated["is_peer"] = annotated["ID"].isin(peer_ids).astype(int)
    for peer_type in peer_types:
        column = f"is_{peer_type}"
        annotated[column] = annotated["peer_types"].str.split(";").apply(lambda types: int(peer_type in types))

    all_peers = annotated[annotated["is_peer"] == 1].copy()
    logging.info("Matched %s unique peers to the full dataset", len(all_peers))
    return annotated, all_peers, peers


def build_parent_child_edges(full):
    records = []
    parent_cols = ["ID", "gender", "fullname", "born", "child"]
    for row in full[parent_cols].itertuples(index=False):
        parent_id, gender, fullname, born, child = row
        for child_id in parse_id_list(child):
            records.append(
                {
                    "parent_id": int(parent_id),
                    "child_id": int(child_id),
                    "parent_gender": gender,
                    "parent_name": fullname,
                    "parent_born": born,
                }
            )
    if not records:
        return pd.DataFrame(
            columns=["parent_id", "child_id", "parent_gender", "parent_name", "parent_born"]
        )
    edges = pd.DataFrame.from_records(records)
    edges = edges.drop_duplicates(["parent_id", "child_id"]).copy()
    logging.info("Built %s unique parent-child edges", len(edges))
    return edges


def build_parent_summary(edges):
    if edges.empty:
        return pd.DataFrame()

    parent_summary = (
        edges.groupby("child_id")
        .agg(
            ParentIDs=("parent_id", join_ids),
            ParentCount=("parent_id", "nunique"),
        )
        .reset_index()
        .rename(columns={"child_id": "ID"})
    )

    father = (
        edges[edges["parent_gender"] == "M"]
        .sort_values(["child_id", "parent_id"])
        .drop_duplicates("child_id", keep="first")
        .rename(
            columns={
                "child_id": "ID",
                "parent_name": "FatherName",
                "parent_born": "FatherBorn",
                "parent_id": "FatherIndex",
            }
        )[["ID", "FatherName", "FatherBorn", "FatherIndex"]]
    )
    mother = (
        edges[edges["parent_gender"] == "F"]
        .sort_values(["child_id", "parent_id"])
        .drop_duplicates("child_id", keep="first")
        .rename(
            columns={
                "child_id": "ID",
                "parent_name": "MotherName",
                "parent_born": "MotherBorn",
                "parent_id": "MotherIndex",
            }
        )[["ID", "MotherName", "MotherBorn", "MotherIndex"]]
    )

    parent_summary = parent_summary.merge(father, how="left", on="ID")
    parent_summary = parent_summary.merge(mother, how="left", on="ID")
    return parent_summary


def add_parent_summary(df, parent_summary):
    if parent_summary.empty:
        return df.copy()
    existing = [
        "ParentIDs",
        "ParentCount",
        "FatherName",
        "FatherBorn",
        "FatherIndex",
        "MotherName",
        "MotherBorn",
        "MotherIndex",
    ]
    df = df.drop(columns=[column for column in existing if column in df.columns], errors="ignore")
    return df.merge(parent_summary, how="left", on="ID")


def peer_parent_ids(parent_ids, peer_ids):
    return sorted({int(parent_id) for parent_id in parse_id_list(parent_ids) if int(parent_id) in peer_ids})


def build_parent_lookup(edges):
    if edges.empty:
        return {}
    return (
        edges.groupby("child_id")["parent_id"]
        .agg(lambda values: tuple(sorted({int(value) for value in values})))
        .to_dict()
    )


def exact_ancestor_ids(person_id, depth, parent_lookup):
    current = {int(person_id)}
    for _ in range(depth):
        next_ids = set()
        for current_id in current:
            next_ids.update(parent_lookup.get(int(current_id), ()))
        current = next_ids
        if not current:
            break
    return sorted(current)


def add_lineage_peer_status(df, parent_lookup, peer_ids, peer_type_lookup, max_depth):
    df = df.copy()
    peer_ids = {int(value) for value in peer_ids}
    df["SelfIsPeer"] = df["ID"].isin(peer_ids).astype(int)
    df["SelfPeerTypes"] = [
        join_types(parse_type_list(peer_type_lookup.get(int(person_id), "")))
        for person_id in df["ID"]
    ]

    depth_names = {
        1: "Parent",
        2: "Grandparent",
        3: "GreatGrandparent",
    }
    ancestor_peer_ids_by_row = [[] for _ in range(len(df))]
    for depth in range(1, max_depth + 1):
        name = depth_names[depth]
        peer_values = []
        has_peer = []
        peer_count = []
        peer_type_values = []
        for row_index, person_id in enumerate(df["ID"]):
            ancestors = exact_ancestor_ids(person_id, depth, parent_lookup)
            peer_ancestors = [ancestor_id for ancestor_id in ancestors if ancestor_id in peer_ids]
            ancestor_peer_ids_by_row[row_index].extend(peer_ancestors)
            peer_types = []
            for ancestor_id in peer_ancestors:
                peer_types.extend(parse_type_list(peer_type_lookup.get(int(ancestor_id), "")))
            peer_values.append(join_ids(peer_ancestors))
            has_peer.append(int(bool(peer_ancestors)))
            peer_count.append(len(peer_ancestors))
            peer_type_values.append(join_types(peer_types))
        df[f"Peer{name}IDs"] = peer_values
        df[f"HasPeer{name}"] = has_peer
        df[f"Peer{name}Count"] = peer_count
        df[f"Peer{name}Types"] = peer_type_values
        df[f"Peer{name}TypeCount"] = [len(parse_type_list(value)) for value in peer_type_values]

    ancestor_peer_ids = []
    ancestor_peer_types = []
    for ancestor_ids in ancestor_peer_ids_by_row:
        unique_ids = sorted({int(value) for value in ancestor_ids})
        peer_types = []
        for ancestor_id in unique_ids:
            peer_types.extend(parse_type_list(peer_type_lookup.get(int(ancestor_id), "")))
        ancestor_peer_ids.append(join_ids(unique_ids))
        ancestor_peer_types.append(join_types(peer_types))
    df["PeerAncestorIDs"] = ancestor_peer_ids
    df["PeerAncestorCount"] = [len(parse_id_list(value)) for value in ancestor_peer_ids]
    df["PeerAncestorTypes"] = ancestor_peer_types
    df["PeerAncestorTypeCount"] = [len(parse_type_list(value)) for value in ancestor_peer_types]
    return df


def descendants_from(full, edges, parent_summary, parent_ids, generation_name):
    parent_ids = {int(value) for value in parent_ids if pd.notna(value)}
    direct_edges = edges[edges["parent_id"].isin(parent_ids)].copy()
    direct_edges = direct_edges[direct_edges["child_id"].isin(set(full["ID"]))].copy()
    child_ids = sorted(set(direct_edges["child_id"].astype(int)))

    descendants = full[full["ID"].isin(child_ids)].copy()
    descendants = add_parent_summary(descendants, parent_summary)
    if len(direct_edges):
        direct_parent_summary = (
            direct_edges.groupby("child_id")["parent_id"]
            .agg(lambda s: join_ids(s))
            .reset_index()
            .rename(columns={"child_id": "ID", "parent_id": "DirectParentIDs"})
        )
        direct_parent_count = (
            direct_edges.groupby("child_id")["parent_id"]
            .nunique()
            .reset_index()
            .rename(columns={"child_id": "ID", "parent_id": "DirectParentCount"})
        )
        descendants = descendants.merge(direct_parent_summary, how="left", on="ID")
        descendants = descendants.merge(direct_parent_count, how="left", on="ID")
    else:
        descendants["DirectParentIDs"] = pd.NA
        descendants["DirectParentCount"] = pd.NA
    descendants["generation"] = generation_name
    return descendants.sort_values("ID").reset_index(drop=True)


def compute_halfway_day(month, year):
    if month == 2:
        return 15 if isleap(year) else 14
    if month in {1, 3, 5, 7, 8, 10, 12}:
        return 16
    return 15


def max_day_for_month(month, year):
    if month == 2:
        return 29 if isleap(year) else 28
    if month in {4, 6, 9, 11}:
        return 30
    return 31


def impute_month_day(year, month=None):
    if DATE_IMPUTATION == "random":
        if month is None:
            month = DATE_RNG.randint(1, 12)
        day = DATE_RNG.randint(1, max_day_for_month(month, year))
        return month, day

    if month is None:
        return 7, 1
    return month, compute_halfway_day(month, year)


def reconstructed_second_year(base_year, second_year):
    if len(second_year) < len(base_year):
        return int(base_year[: len(base_year) - len(second_year)] + second_year)
    return int(second_year)


def find_month(text):
    for name, number in MONTHS.items():
        if re.search(rf"\b{name}\w*\b", text):
            return number
    return None


def parse_peerage_date(value):
    if pd.isna(value):
        return None
    text = str(value).strip().lower().replace("\xa0", " ")
    if text in MISSING_TOKENS:
        return None
    has_qualifier = bool(
        re.search(r"\b(before|after|between|circa|about|abt\.?|ca\.?|c\.)\b", text)
    )

    text = re.sub(r"\b(circa|about|abt\.?|ca\.?|c\.)\b", " ", text)
    text = re.sub(r"\b(before|after|between)\b", " ", text)
    text = re.sub(r"[^a-z0-9/\s-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    range_match = re.fullmatch(r"(\d{3,4})\s*/\s*(\d{1,4})", text)
    if range_match:
        base_year = range_match.group(1)
        year = reconstructed_second_year(base_year, range_match.group(2))
        month, day = impute_month_day(year)
        return year, month, day, True, "year_range"

    year_numbers = re.findall(r"\d{3,4}", text)
    if not year_numbers:
        return None

    if "/" in text or " or " in text:
        parts = text.split("/") if "/" in text else text.split(" or ")
        base_match = re.search(r"\d{3,4}", parts[0])
        second_match = re.search(r"\d+", parts[1]) if len(parts) > 1 else None
        if base_match and second_match:
            year = reconstructed_second_year(base_match.group(0), second_match.group(0))
        else:
            year = int(year_numbers[-1])
    else:
        year = int(year_numbers[-1])

    month = find_month(text)
    synthetic = False
    if month is None:
        month, day = impute_month_day(year)
        synthetic = True
        precision = "year"
    else:
        numbers = [int(number) for number in re.findall(r"\d+", text)]
        day_candidates = [number for number in numbers if 1 <= number <= 31 and number != year]
        if re.match(r"\d", text) and day_candidates:
            day = day_candidates[0]
            precision = "day"
        else:
            month, day = impute_month_day(year, month)
            synthetic = True
            precision = "month"

    try:
        datetime(year, month, day)
    except ValueError:
        return None
    if has_qualifier:
        synthetic = True
        precision = f"qualified_{precision}"
    return year, month, day, synthetic, precision


def date_as_float(dt):
    jan1 = dt - datetime(dt.year, 1, 1)
    if not isleap(dt.year) and jan1.days >= 31 + 28:
        jan1 += timedelta(1)
    return dt.year + jan1.days * SIZE_OF_DAY + jan1.seconds * SIZE_OF_SECOND


def format_assigned_date(year, month, day):
    return f"{month:02d}-{day:02d}-{year:04d}"


def format_iso_date(year, month, day):
    return f"{year:04d}-{month:02d}-{day:02d}"


def parsed_date_rows(values, prefix):
    rows = []
    date_objects = []
    for value in values:
        parsed = parse_peerage_date(value)
        if parsed is None:
            rows.append(
                {
                    f"{prefix}_year": pd.NA,
                    f"{prefix}_month": pd.NA,
                    f"{prefix}_day": pd.NA,
                    f"is_synthetic_{prefix}date": pd.NA,
                    f"{prefix}_date_precision": pd.NA,
                    f"ass_{prefix}": pd.NA,
                    f"{prefix}_dt": pd.NA,
                }
            )
            date_objects.append(None)
            continue
        year, month, day, synthetic, precision = parsed
        rows.append(
            {
                f"{prefix}_year": year,
                f"{prefix}_month": month,
                f"{prefix}_day": day,
                f"is_synthetic_{prefix}date": synthetic,
                f"{prefix}_date_precision": precision,
                f"ass_{prefix}": format_assigned_date(year, month, day),
                f"{prefix}_dt": format_iso_date(year, month, day),
            }
        )
        date_objects.append(datetime(year, month, day))
    return pd.DataFrame(rows), date_objects


def add_lifetable_dates(df):
    df = df.copy()
    born_rows, born_objects = parsed_date_rows(df["born"], "born")
    died_rows, died_objects = parsed_date_rows(df["died"], "died")
    born_rows.index = df.index
    died_rows.index = df.index
    df = pd.concat([df, born_rows, died_rows], axis=1)

    ages = []
    for born_dt, died_dt in zip(born_objects, died_objects):
        if born_dt is None or died_dt is None:
            ages.append(np.nan)
        else:
            ages.append(date_as_float(died_dt) - date_as_float(born_dt))
    df["age_at_death"] = ages
    return df


def clean_for_lifetable(df, label, bad_dir, bad_aliases=None):
    bad_labels = [label, *(bad_aliases or [])]

    def write_bad_records(bad_df, suffix):
        for bad_label in bad_labels:
            write_bad(bad_df, bad_dir / f"{bad_label}_{suffix}.csv")

    df = df.drop_duplicates("ID", keep="first").copy()
    df = add_lifetable_dates(df)

    missing_birth = df[df["born_dt"].isna()].copy()
    write_bad_records(missing_birth, "missing_or_unparsed_birth")
    df = df[df["born_dt"].notna()].copy()

    born_year = pd.to_numeric(df["born_year"], errors="coerce")
    missing_death_pre1910_mask = df["died_dt"].isna() & (born_year < 1910)
    missing_death_pre1910 = df[missing_death_pre1910_mask].copy()
    write_bad_records(missing_death_pre1910, "missing_death_before_1910_births")
    df = df[~missing_death_pre1910_mask].copy()

    born_year = pd.to_numeric(df["born_year"], errors="coerce")
    bad_death_post1910_mask = df["died_dt"].isna() & df["died"].notna() & (born_year >= 1910)
    bad_death_post1910 = df[bad_death_post1910_mask].copy()
    write_bad_records(bad_death_post1910, "unparsed_death_after_1910_births")
    df = df[~bad_death_post1910_mask].copy()

    negative_age = df[df["age_at_death"].notna() & (df["age_at_death"] < 0)].copy()
    write_bad_records(negative_age, "negative_age_at_death")
    df = df[~(df["age_at_death"].notna() & (df["age_at_death"] < 0))].copy()

    logging.info("Cleaned %s: %s lifetable-ready rows", label, len(df))
    return df.sort_values("ID").reset_index(drop=True)


def build_generation_frames(full, all_peers, edges, parent_summary):
    full_ids = set(full["ID"].astype(int))
    missing_edges = edges[~edges["child_id"].isin(full_ids)].copy()
    known_edges = edges[edges["child_id"].isin(full_ids)].copy()
    peer_ids = set(all_peers["ID"].astype(int))
    parent_lookup = build_parent_lookup(known_edges)
    peer_type_lookup = full.set_index("ID")["peer_types"].fillna("").to_dict()

    children_of_peers = descendants_from(
        full, known_edges, parent_summary, all_peers["ID"], "children_of_peers"
    )
    children_of_peers = add_lineage_peer_status(
        children_of_peers, parent_lookup, peer_ids, peer_type_lookup, 1
    )
    grandchildren_of_peers = descendants_from(
        full, known_edges, parent_summary, children_of_peers["ID"], "grandchildren_of_peers"
    )
    grandchildren_of_peers = add_lineage_peer_status(
        grandchildren_of_peers, parent_lookup, peer_ids, peer_type_lookup, 2
    )
    greatgrandchildren_of_peers = descendants_from(
        full,
        known_edges,
        parent_summary,
        grandchildren_of_peers["ID"],
        "greatgrandchildren_of_peers",
    )
    greatgrandchildren_of_peers = add_lineage_peer_status(
        greatgrandchildren_of_peers, parent_lookup, peer_ids, peer_type_lookup, 3
    )

    first_generation_peers = add_parent_summary(all_peers.copy(), parent_summary)
    first_generation_peers["PeerParentIDs"] = first_generation_peers["ParentIDs"].apply(
        lambda parent_ids: join_ids(peer_parent_ids(parent_ids, peer_ids))
    )
    first_generation_peers["HasPeerParent"] = first_generation_peers["PeerParentIDs"].ne("").astype(int)
    first_generation_peers["SelfIsPeer"] = 1
    first_generation_peers["generation"] = "first_generation_peers"
    first_generation_peers = first_generation_peers.sort_values("ID").reset_index(drop=True)

    secondary_generation = children_of_peers.copy()
    secondary_generation["generation"] = "secondary_generation"
    tertiary_peers = grandchildren_of_peers.copy()
    tertiary_peers["generation"] = "tertiary_peers"

    logging.info(
        "Built generations: peers=%s, first_generation_peers=%s, children_of_peers=%s, "
        "grandchildren_of_peers=%s, greatgrandchildren_of_peers=%s, secondary_generation=%s, tertiary_peers=%s",
        len(all_peers),
        len(first_generation_peers),
        len(children_of_peers),
        len(grandchildren_of_peers),
        len(greatgrandchildren_of_peers),
        len(secondary_generation),
        len(tertiary_peers),
    )
    return {
        "missing_edges": missing_edges,
        "all_peers": first_generation_peers.copy(),
        "first_generation_peers": first_generation_peers,
        "children_of_peers": children_of_peers,
        "grandchildren_of_peers": grandchildren_of_peers,
        "greatgrandchildren_of_peers": greatgrandchildren_of_peers,
        "secondary_generation": secondary_generation,
        "tertiary_peers": tertiary_peers,
    }


def write_generation_outputs(frames, paths):
    write_bad(frames["missing_edges"], paths.bad / "child_references_not_in_raw.csv")

    write_tsv(frames["all_peers"], paths.derived / "british_peers_and_orders_full.tsv")
    write_tsv(frames["children_of_peers"], paths.derived / "british_peers_and_orders_child.tsv")
    write_tsv(frames["grandchildren_of_peers"], paths.derived / "british_peers_and_orders_grandchild.tsv")
    write_tsv(
        frames["greatgrandchildren_of_peers"],
        paths.derived / "british_peers_and_orders_greatgrandchild.tsv",
    )
    write_tsv(frames["first_generation_peers"], paths.derived / "first_generation_peers.tsv")
    write_tsv(frames["secondary_generation"], paths.derived / "secondary_generation.tsv")
    write_tsv(frames["secondary_generation"], paths.derived / "second_generation.tsv")
    write_tsv(frames["tertiary_peers"], paths.derived / "tertiary_peers.tsv")


def peer_value(value):
    return "peer" if int(value) == 1 else "nonpeer"


def write_offspring_class(df, paths, class_name, generation, criteria, description, manifest_rows):
    class_df = df.copy()
    class_df["offspring_class"] = class_name
    class_df["offspring_class_generation"] = generation
    class_df["offspring_class_description"] = description
    relative_file = f"offspring_classes/{class_name}.tsv"
    write_tsv(class_df, paths.classes / f"{class_name}.tsv")
    manifest_rows.append(
        {
            "offspring_class": class_name,
            "file": relative_file,
            "generation": generation,
            "aggregate": 0,
            "rows": len(class_df),
            "criteria": criteria,
            "description": description,
        }
    )


def add_aggregate_manifest_entry(manifest_rows, class_name, file_name, rows, description):
    manifest_rows.append(
        {
            "offspring_class": class_name,
            "file": file_name,
            "generation": class_name,
            "aggregate": 1,
            "rows": rows,
            "criteria": "aggregate",
            "description": description,
        }
    )


def merge_id_lists(values):
    merged = []
    for value in values:
        merged.extend(parse_id_list(value))
    return join_ids(merged)


def merge_type_lists(values):
    merged = []
    for value in values:
        merged.extend(parse_type_list(value))
    return join_types(merged)


def deduplicate_offspring(clean_child, clean_grandchild, clean_greatgrandchild):
    combined = pd.concat([clean_child, clean_grandchild, clean_greatgrandchild], sort=False)
    offspring = combined.drop_duplicates("ID", keep="first").sort_values("ID").reset_index(drop=True)

    ancestor_summary = (
        combined.groupby("ID", as_index=False)
        .agg(
            PeerAncestorIDs=("PeerAncestorIDs", merge_id_lists),
            PeerAncestorTypes=("PeerAncestorTypes", merge_type_lists),
        )
    )
    ancestor_summary["PeerAncestorCount"] = ancestor_summary["PeerAncestorIDs"].apply(
        lambda value: len(parse_id_list(value))
    )
    ancestor_summary["PeerAncestorTypeCount"] = ancestor_summary["PeerAncestorTypes"].apply(
        lambda value: len(parse_type_list(value))
    )
    offspring = offspring.drop(
        columns=["PeerAncestorIDs", "PeerAncestorCount", "PeerAncestorTypes", "PeerAncestorTypeCount"],
        errors="ignore",
    )
    offspring = offspring.merge(ancestor_summary, how="left", on="ID")
    return offspring.sort_values("ID").reset_index(drop=True)


def write_offspring_class_outputs(clean_child, clean_grandchild, clean_greatgrandchild, offspring, paths):
    for stale_file in paths.classes.glob("*.tsv"):
        stale_file.unlink()

    manifest_rows = []
    add_aggregate_manifest_entry(
        manifest_rows,
        "children",
        "child_df.tsv",
        len(clean_child),
        "All children of any peer, de-duplicated by person ID within this generation.",
    )
    add_aggregate_manifest_entry(
        manifest_rows,
        "grandchildren",
        "grandchild_df.tsv",
        len(clean_grandchild),
        "All grandchildren of any peer, de-duplicated by person ID within this generation.",
    )
    add_aggregate_manifest_entry(
        manifest_rows,
        "greatgrandchildren",
        "greatgrandchild_df.tsv",
        len(clean_greatgrandchild),
        "All great-grandchildren of any peer, de-duplicated by person ID within this generation.",
    )
    add_aggregate_manifest_entry(
        manifest_rows,
        "all_offspring",
        "offspring_df.tsv",
        len(offspring),
        "Children, grandchildren, and great-grandchildren of peers, de-duplicated across all three generations.",
    )

    for self_is_peer in (1, 0):
        label = peer_value(self_is_peer)
        subset = clean_child[clean_child["SelfIsPeer"] == self_is_peer].copy()
        class_name = f"children_self_{label}"
        write_offspring_class(
            subset,
            paths,
            class_name,
            "children",
            f"SelfIsPeer={self_is_peer}",
            f"Children of any peer who are themselves {label}s.",
            manifest_rows,
        )

    for parent_is_peer in (1, 0):
        for self_is_peer in (1, 0):
            parent_label = peer_value(parent_is_peer)
            self_label = peer_value(self_is_peer)
            subset = clean_grandchild[
                (clean_grandchild["HasPeerParent"] == parent_is_peer)
                & (clean_grandchild["SelfIsPeer"] == self_is_peer)
            ].copy()
            class_name = f"grandchildren_parent_{parent_label}_self_{self_label}"
            write_offspring_class(
                subset,
                paths,
                class_name,
                "grandchildren",
                f"HasPeerParent={parent_is_peer};SelfIsPeer={self_is_peer}",
                f"Grandchildren of any peer with {parent_label} parent status and {self_label} self status.",
                manifest_rows,
            )

    for grandparent_is_peer in (1, 0):
        for parent_is_peer in (1, 0):
            for self_is_peer in (1, 0):
                grandparent_label = peer_value(grandparent_is_peer)
                parent_label = peer_value(parent_is_peer)
                self_label = peer_value(self_is_peer)
                subset = clean_greatgrandchild[
                    (clean_greatgrandchild["HasPeerGrandparent"] == grandparent_is_peer)
                    & (clean_greatgrandchild["HasPeerParent"] == parent_is_peer)
                    & (clean_greatgrandchild["SelfIsPeer"] == self_is_peer)
                ].copy()
                class_name = (
                    f"greatgrandchildren_grandparent_{grandparent_label}_"
                    f"parent_{parent_label}_self_{self_label}"
                )
                write_offspring_class(
                    subset,
                    paths,
                    class_name,
                    "greatgrandchildren",
                    (
                        f"HasPeerGrandparent={grandparent_is_peer};"
                        f"HasPeerParent={parent_is_peer};SelfIsPeer={self_is_peer}"
                    ),
                    (
                        "Great-grandchildren of any peer with "
                        f"{grandparent_label} grandparent status, "
                        f"{parent_label} parent status, and {self_label} self status."
                    ),
                    manifest_rows,
                )

    single_type_offspring = offspring[offspring["PeerAncestorTypeCount"] == 1].copy()
    peer_types = sorted(
        {
            peer_type
            for value in single_type_offspring["PeerAncestorTypes"].dropna()
            for peer_type in parse_type_list(value)
        }
    )
    for peer_type in peer_types:
        slug = class_slug(peer_type)
        subset = single_type_offspring[single_type_offspring["PeerAncestorTypes"] == peer_type].copy()
        class_name = f"all_offspring_only_peer_type_{slug}"
        write_offspring_class(
            subset,
            paths,
            class_name,
            "all_offspring",
            f"PeerAncestorTypes={peer_type};PeerAncestorTypeCount=1",
            (
                "Children, grandchildren, and great-grandchildren whose relevant "
                f"peer ancestors are exclusively {peer_type}; de-duplicated by person ID."
            ),
            manifest_rows,
        )

    manifest = pd.DataFrame(manifest_rows)
    write_tsv(manifest, paths.clean / "offspring_lifetable_classes.tsv")
    return manifest


def write_clean_outputs(full, frames, paths):
    clean_full = clean_for_lifetable(full, "full_df", paths.bad)
    clean_elite = clean_for_lifetable(
        frames["all_peers"], "elite_df", paths.bad, bad_aliases=["first_generation_peers"]
    )
    clean_child = clean_for_lifetable(
        frames["children_of_peers"],
        "child_df",
        paths.bad,
        bad_aliases=["secondary_generation", "second_generation"],
    )
    clean_grandchild = clean_for_lifetable(
        frames["grandchildren_of_peers"], "grandchild_df", paths.bad, bad_aliases=["tertiary_peers"]
    )
    clean_greatgrandchild = clean_for_lifetable(
        frames["greatgrandchildren_of_peers"], "greatgrandchild_df", paths.bad
    )

    clean_first_generation = clean_elite.copy()
    clean_first_generation["generation"] = "first_generation_peers"
    clean_second_generation = clean_child.copy()
    clean_second_generation["generation"] = "secondary_generation"
    clean_tertiary = clean_grandchild.copy()
    clean_tertiary["generation"] = "tertiary_peers"

    offspring = deduplicate_offspring(clean_child, clean_grandchild, clean_greatgrandchild)

    write_tsv(clean_full, paths.clean / "full_df.tsv")
    write_tsv(clean_elite, paths.clean / "elite_df.tsv")
    write_tsv(clean_child, paths.clean / "child_df.tsv")
    write_tsv(clean_grandchild, paths.clean / "grandchild_df.tsv")
    write_tsv(clean_greatgrandchild, paths.clean / "greatgrandchild_df.tsv")
    write_tsv(offspring, paths.clean / "offspring_df.tsv")
    write_tsv(clean_first_generation, paths.clean / "first_generation_peers.tsv")
    write_tsv(clean_second_generation, paths.clean / "secondary_generation.tsv")
    write_tsv(clean_second_generation, paths.clean / "second_generation.tsv")
    write_tsv(clean_tertiary, paths.clean / "tertiary_peers.tsv")
    class_manifest = write_offspring_class_outputs(
        clean_child, clean_grandchild, clean_greatgrandchild, offspring, paths
    )

    return {
        "full_df": clean_full,
        "elite_df": clean_elite,
        "child_df": clean_child,
        "grandchild_df": clean_grandchild,
        "greatgrandchild_df": clean_greatgrandchild,
        "offspring_df": offspring,
        "first_generation_peers": clean_first_generation,
        "secondary_generation": clean_second_generation,
        "tertiary_peers": clean_tertiary,
        "offspring_lifetable_classes": class_manifest,
    }


def write_summary(frames, clean_frames, paths):
    summary_rows = []
    for name, frame in frames.items():
        if name != "missing_edges":
            summary_rows.append({"dataset": f"derived/{name}", "rows": len(frame)})
    for name, frame in clean_frames.items():
        summary_rows.append({"dataset": f"clean/{name}", "rows": len(frame)})
    class_manifest = clean_frames.get("offspring_lifetable_classes")
    if class_manifest is not None:
        for row in class_manifest.itertuples(index=False):
            summary_rows.append({"dataset": f"clean/{row.file}", "rows": int(row.rows)})
    summary = pd.DataFrame(summary_rows)
    write_tsv(summary, paths.clean / "preprocessing_summary.tsv")


def run(raw_dir, output_dir, log_dir):
    start = time.monotonic()
    log_file = configure_logging(log_dir)
    paths = build_output_paths(output_dir)
    logging.info(
        "Date imputation mode: %s. Random seed is used only when mode=random.",
        DATE_IMPUTATION,
    )

    full, peers, sources = read_raw_inputs(raw_dir)
    full = drop_duplicate_people(full, paths.bad)
    full, all_peers, peers = annotate_peer_flags(full, peers, paths.bad)

    write_tsv(sources, paths.derived / "sources.tsv")
    write_tsv(peers, paths.derived / "british_peers_and_orders.tsv")

    edges = build_parent_child_edges(full)
    parent_summary = build_parent_summary(edges)
    frames = build_generation_frames(full, all_peers, edges, parent_summary)
    write_generation_outputs(frames, paths)

    full_with_parent_summary = add_parent_summary(full, parent_summary).sort_values("ID").reset_index(drop=True)
    clean_frames = write_clean_outputs(full_with_parent_summary, frames, paths)
    write_summary(frames, clean_frames, paths)

    duration = time.monotonic() - start
    logging.info("Completed preprocessing in %.1fs", duration)
    logging.info("Log file: %s", display_path(log_file))
    return log_file


def parse_args():
    parser = argparse.ArgumentParser(
        description="Preprocess thePeerage scraper outputs for lifetable construction."
    )
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--log-dir", type=Path, default=DEFAULT_LOG_DIR)
    parser.add_argument(
        "--date-imputation",
        choices=["random", "deterministic"],
        default="deterministic",
        help="How to fill missing month/day when a usable year exists. Default: deterministic midpoint-style filling.",
    )
    parser.add_argument(
        "--random-seed",
        type=int,
        default=20260505,
        help="Seed for random date imputation.",
    )
    return parser.parse_args()


def main():
    global DATE_IMPUTATION, DATE_RNG
    args = parse_args()
    DATE_IMPUTATION = args.date_imputation
    DATE_RNG = random.Random(args.random_seed)
    log_file = run(args.raw_dir, args.output_dir, args.log_dir)
    print(f"Preprocessing complete. Log file: {display_path(log_file)}")


if __name__ == "__main__":
    main()
