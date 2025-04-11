import re
import os
import numpy as np
import pandas as pd
from datetime import datetime


def compute_halfway_day(month, year):
    """
    Return the default ("halfway") day for the given month and year:
      - For February: 14 if not a leap year, 15 if a leap year.
      - Months with 31 days: 16.
      - Months with 30 days: 15.
    """
    if month == 2:
        if (year % 400 == 0) or ((year % 4 == 0) and (year % 100 != 0)):
            return 15
        else:
            return 14
    if month in [1, 3, 5, 7, 8, 10, 12]:
        return 16
    return 15


def parse_dates(val):
    """
    Parse a string from the 'born' column and extract (year, month, day, is_synthetic_date).

    Rules:
    - If the string contains "before" or "after" (but not "circa"), return missing values.

    (A) If the string contains a slash ("/") or the word "or":
         - First, if the string matches exactly two year numbers separated by a slash
           (e.g. "1464/65"), return (second_year, 1, 1, True) where the second year is reconstructed if abbreviated.
         - Otherwise, split on "/" (or " or "), extract a base year from the first token and a secondary year
           from the second token, then:
             • If no month name is found, default to month 7 and day 1 (synthetic).
             • If a month name is found, use it. For the day, if the string starts with a digit and the first
               number (day candidate) is ≤ 31, use it; otherwise, compute a fallback day (mark synthetic).

    (B) If no slash or "or" is present:
         - If a month name is found:
             • If the string starts with a digit and at least two numbers exist, take the first as day and the last as year (synthetic = False).
             • Otherwise, use the sole found number as year and compute the day (synthetic = True).
         - If no month name is found:
             • If only one number is present, default to month 7 and day 1 (synthetic = True).
             • If more than one number is present and the string starts with a digit, use the first as day and the last as year (synthetic = False);
               otherwise default to day 1 (synthetic = True) with month 7.

    If nothing can be parsed, returns (np.nan, np.nan, np.nan, False).
    """
    if pd.isnull(val) or str(val).strip() == '':
        return (np.nan, np.nan, np.nan, False)

    s = str(val).strip().lower()

    # If the string indicates uncertainty with "before" or "after" (but not "circa"), return missing.
    if (("before" in s or "after" in s) and ("circa" not in s)):
        return (np.nan, np.nan, np.nan, False)

    synthetic = False
    month_names = {
        'january': 1, 'february': 2, 'march': 3, 'april': 4,
        'may': 5, 'june': 6, 'july': 7, 'august': 8,
        'september': 9, 'october': 10, 'november': 11, 'december': 12
    }

    # (A) Handle strings with a slash ("/") or "or".
    if '/' in s or ' or ' in s:
        # Check if the entire string is exactly two year numbers separated by a slash.
        match_range = re.fullmatch(r'(\d{3,4})\s*/\s*(\d{1,4})', s)
        if match_range:
            base_year_str = match_range.group(1)
            second_year_str = match_range.group(2)
            if len(second_year_str) < len(base_year_str):
                full_year = int(base_year_str[:len(base_year_str) - len(second_year_str)] + second_year_str)
            else:
                full_year = int(second_year_str)
            synthetic = True  # synthesized default for missing month/day
            return (full_year, 1, 1, synthetic)

        # Otherwise, split on '/' (or " or ").
        tokens = s.split('/') if '/' in s else s.split(' or ')
        match1 = re.search(r'\d{3,4}', tokens[0])
        if match1:
            base_year_str = match1.group(0)
        else:
            return (np.nan, np.nan, np.nan, False)
        match2 = re.search(r'\d+', tokens[1]) if len(tokens) > 1 else None
        if match2:
            second_year_str = match2.group(0)
        else:
            second_year_str = None
        if second_year_str:
            if len(second_year_str) < len(base_year_str):
                full_year = int(base_year_str[:len(base_year_str) - len(second_year_str)] + second_year_str)
            else:
                full_year = int(second_year_str)
        else:
            full_year = int(base_year_str)

        # Look for a month name in the whole string.
        found_month = None
        for name, number in month_names.items():
            if name in s:
                found_month = number
                break
        if found_month is None:
            synthetic = True
            return (full_year, 7, 1, synthetic)
        else:
            month = found_month

        # Determine day: if the string starts with a digit, try to use the first number.
        if re.match(r'\d', s):
            numbers = re.findall(r'\d+', s)
            try:
                day_candidate = int(numbers[0])
            except Exception:
                day_candidate = None
            if day_candidate is not None and day_candidate <= 31:
                day = day_candidate
            else:
                synthetic = True
                day = compute_halfway_day(month, full_year)
        else:
            synthetic = True
            day = compute_halfway_day(month, full_year)
        return (full_year, month, day, synthetic)

    # (B) Handle strings without a slash or "or".
    else:
        found_month = None
        for name, number in month_names.items():
            if name in s:
                found_month = number
                break
        numbers = re.findall(r'\d+', s)
        if found_month is not None:
            if len(numbers) == 0:
                return (np.nan, np.nan, np.nan, False)
            if re.match(r'\d', s):
                if len(numbers) >= 2:
                    # Both day and year appear.
                    day = int(numbers[0])
                    year = int(numbers[-1])
                    synthetic = False
                else:
                    year = int(numbers[0])
                    synthetic = True
                    day = compute_halfway_day(found_month, year)
            else:
                year = int(numbers[-1])
                synthetic = True
                day = compute_halfway_day(found_month, year)
            month = found_month
        else:
            # No month name.
            if len(numbers) == 0:
                return (np.nan, np.nan, np.nan, False)
            if len(numbers) == 1:
                year = int(numbers[0])
                month = 7
                day = 1
                synthetic = True
            else:
                if re.match(r'\d', s):
                    day = int(numbers[0])
                    year = int(numbers[-1])
                    synthetic = False
                else:
                    year = int(numbers[-1])
                    day = 1
                    synthetic = True
                month = 7
        return (year, month, day, synthetic)


def create_datetime(row):
    """
    Create a datetime from the row's 'born_year', 'born_month', and 'born_day'.
    If any component is missing or invalid (e.g., day out of range for the month), print the error
    along with the entire 'born' field and return pd.NaT.
    For years less than 1678 (outside the np.datetime64 range), a native Python datetime is created.
    """
    if pd.isnull(row['born_year']) or pd.isnull(row['born_month']) or pd.isnull(row['born_day']):
        return pd.NaT
    try:
        y = int(row['born_year'])
        m = int(row['born_month'])
        d = int(row['born_day'])
        if y < 1678:
            return datetime(y, m, d)
        else:
            return pd.Timestamp(year=y, month=m, day=d)
    except ValueError as e:
        print(f"Error creating datetime for row index {row.name} with 'born'='{row['born']}': "
              f"year={row['born_year']}, month={row['born_month']}, day={row['born_day']}. Error: {e}")
        return pd.NaT


def main():
    # ====================== File Paths & Data Loading ======================
    rawpath = '../data/thepeerage/raw'
    bad_stuff = '../data/thepeerage/bad_stuff'

    source = pd.read_csv(os.path.join(rawpath, 'sources.tsv'), sep='\t')
    peers = pd.read_csv(os.path.join(rawpath, 'british_peers_and_orders.tsv'), sep='\t')
    raw = pd.read_csv(os.path.join(rawpath, 'entire_thepeerage.tsv'), sep='\t')

    print(f"There are {len(raw[raw['ID'].isnull()])} rows of missing ids in the raw dataset")
    raw[raw['ID'].isnull()].to_csv(os.path.join(bad_stuff, 'missing_person_ID.csv'))
    print(f"These are saved to {os.path.join(bad_stuff, 'missing_person_ID.csv')}.")
    raw = raw[raw['ID'].notnull()]
    print("Dropping them")
    print(f"There are {len(raw[raw['Page'].isnull()])} rows of missing pages in the raw dataset")
    raw[raw['Page'].isnull()].to_csv(os.path.join(bad_stuff, 'missing_person_Page.csv'))
    print(f"These are saved to {os.path.join(bad_stuff, 'missing_person_Page.csv')}.")
    print(f"There are {len(peers[peers['id'].isnull()])} rows of missing ids in the peers dataset")
    print(f"There are {len(peers[peers['type'].isnull()])} rows of missing types in the peers dataset")
    print(f"There are {len(source[source['Page'].isnull()])} rows of missing pages in the source dataset")
    print(f"There are {len(source[source['SourceID'].isnull()])} rows of missing sourceID in the source dataset")
    print(f"There are {len(source[source['Source'].isnull()])} rows of missing Source in the source dataset")
    print(f"There are {len(peers[~peers['id'].isin(raw['ID'])])} peer IDs not in the raw...")
    peers[~peers['id'].isin(raw['ID'])].to_csv(os.path.join(bad_stuff, 'peers_not_in_raw_ID.csv'))
    print(f"These are saved to {os.path.join(bad_stuff, 'peers_not_in_raw_ID.csv')}.")
    peers = peers[peers['id'].isin(raw['ID'])]
    print("Dropping them")
    peers = peers.set_index('id')
    df = raw
    df = df.set_index('ID')
    print(f"We begin with {len(df)} rows of the raw df")
    print("Now, let's merge on our peerage data.")
    peers = peers.rename({'type': 'type_of_peer'}, axis=1)
    for type_of_peer in peers['type_of_peer'].unique():
        df['is_' + type_of_peer] = 0
    df['is_peer'] = 0
    for index, row in peers.iterrows():
        df.loc[index, 'is_' + row['type_of_peer']] = 1
        df.loc[index, 'is_peer'] = 1
    df['is_child_of_peer'] = 0
    df['is_grandchild_of_peer'] = 0

    # ====================== Process Child / Grandchild Relationships ======================
    child_not_found = set()
    grandchild_not_found = set()
    for item in df['child'].str.split(';'):
        if item is not np.nan:
            for child in item:
                if int(child) in df.index:
                    df.loc[int(child), 'is_child_of_peer'] = 1
                    if df.loc[int(child), 'child'] is not np.nan:
                        for grandchild in df.loc[int(child), 'child'].split(';'):
                            if grandchild is not np.nan:
                                if int(grandchild) in df.index:
                                    df.loc[int(grandchild), 'is_grandchild_of_peer'] = 1
                                else:
                                    grandchild_not_found.add(int(grandchild))
                else:
                    child_not_found.add(int(child))
    print(f"We have {len(child_not_found)} children not found")
    print(f"We have {len(grandchild_not_found)} grandchildren not found")

    # Save these two sets to CSV files.
    child_not_found_df = pd.DataFrame({'child_not_found': list(child_not_found)})
    child_not_found_file = os.path.join(bad_stuff, 'child_not_found.csv')
    child_not_found_df.to_csv(child_not_found_file, index=False)
    print(f"Child not found list saved to: {child_not_found_file}")

    grandchild_not_found_df = pd.DataFrame({'grandchild_not_found': list(grandchild_not_found)})
    grandchild_not_found_file = os.path.join(bad_stuff, 'grandchild_not_found.csv')
    grandchild_not_found_df.to_csv(grandchild_not_found_file, index=False)
    print(f"Grandchild not found list saved to: {grandchild_not_found_file}")

    # ====================== Parse 'born' Column & Create Date Columns ======================
    df[['born_year', 'born_month', 'born_day', 'is_synthetic_birthdate']] = df['born'].apply(
        lambda x: pd.Series(parse_dates(x))
    )
    df['born_datetime'] = df.apply(create_datetime, axis=1)
    df['born_datetime_str'] = df['born_datetime'].apply(lambda x: x.strftime('%d-%m-%Y') if pd.notnull(x) else np.nan)

    df[['died_year', 'died_month', 'died_day', 'is_synthetic_dieddate']] = df['died'].apply(
        lambda x: pd.Series(parse_dates(x))
    )
    df['died_datetime'] = df.apply(create_datetime, axis=1)
    df['died_datetime_str'] = df['died_datetime'].apply(lambda x: x.strftime('%d-%m-%Y') if pd.notnull(x) else np.nan)

    print(f"We end with {len(df)} rows of the df")
    print(f"Saving out to ../data/wrangled/wrangled_peerage.csv")
    df.to_csv('../data/wrangled/wrangled_peerage.csv')



if __name__ == "__main__":
    main()