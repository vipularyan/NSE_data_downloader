# Auto-install dependency
# Download selected stock Details in year wise sheets
import sys, subprocess
def ensure(pkg):
    try: __import__(pkg)
    except ImportError: subprocess.check_call([sys.executable, "-m", "pip", "install", pkg, "-q"])
ensure("pandas")
ensure("requests")
ensure("nsepython")

import pandas as pd
import requests
from datetime import date, timedelta, datetime
import time, io, zipfile
import nsepython as nse
import openpyxl  # ensures Excel writer support in pandas

UDIFF_START_DATE = date(2024, 7, 8)  # per NSE circular

def download_old_bhavcopy(report_date: date):
    """Download old CM Bhavcopy (archives) for dates before 2024-07-08."""
    base_url = "https://archives.nseindia.com/content/historical/EQUITIES"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": "https://www.nseindia.com",
    }
    y = report_date.strftime("%Y")
    m = report_date.strftime("%b").upper()
    dmy = report_date.strftime("%d%b%Y").upper()
    url = f"{base_url}/{y}/{m}/cm{dmy}bhav.csv.zip"
    try:
        r = requests.get(url, headers=headers, timeout=20)
        if r.status_code == 404:
            print(f"  404 (old archive): {url}")
            return None
        r.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            csv_name = z.namelist()[0]
            with z.open(csv_name) as f:
                df = pd.read_csv(f)
        df["Date"] = report_date
        return df
    except Exception as e:
        print(f"  Error old archive {report_date}: {e}")
        return None

def download_new_bhavcopy(report_date: date):
    """Download new Bhavcopy via nsepython for dates from 2024-07-08 onward."""
    ds = report_date.strftime("%d-%m-%Y")  # DD-MM-YYYY
    try:
        df = nse.get_bhavcopy(ds)
        if df is None or len(df) == 0:
            return None
        # Drop unnamed cols if present
        df = df.loc[:, ~df.columns.astype(str).str.match(r"^Unnamed")]
        df["Date"] = report_date
        return df
    except Exception as e:
        print(f"  nsepython error {report_date}: {e}")
        return None

def _norm(s: str) -> str:
    return "".join(ch.lower() for ch in str(s) if ch.isalnum())

# Synonyms for schema normalization (normalized strings)
SYN = {
    "symbol": {"symbol"},
    "series": {"series"},
    "open": {"open","openprice"},
    "high": {"high","highprice"},
    "low": {"low","lowprice"},
    "close": {"close","closeprice"},
    "last": {"last","lastprice"},
    "prev_close": {"prevclose","previousclose","prevclosingprice","previousclosingprice"},
    "vwap": {"vwap","avgprice","averageprice","averagepricevwap","avgtradedprice"},
    "volume": {"volume","tottrdqty","totaltradedquantity","ttltrdqnty","tradedquantity","totaltradedqty","totaltrdqnty","volumein000s"},
    "turnover": {"turnover","tottrdval","totaltradedvalue","turnoverlacs","turnoverinlakhs","turnoverlac","turnovercr","turnoverincrores","turnovercrores"},
    "trades": {"trades","totaltrades","nooftrades","nooftrade","nooftrds","noofdealings"},
    "deliv_vol": {"deliverablevolume","delivqty","deliverablevolumecontracts","deliverablequantity","deliveryqty","deliveryquantity"},
    "deliv_pct": {"deliverable","deliverableper","deliverablepercentage","deliverablepct","deliverytotradedquantity","delivper","percentdeliverable","deliverablepercent","percentdeliv","percdeliv"},
    "datecol": {"timestamp","date","tradingdate","tradedate","tradingday"},
    "isin": {"isin"},
}

def standardize_bhavcopy(df: pd.DataFrame, report_date: date) -> pd.DataFrame:
    """Standardize columns to a unified schema and units (Volume in shares, Turnover in INR)."""
    if df is None or df.empty:
        return None

    # Build lookup: normalized -> original
    norm2orig = {_norm(c): c for c in df.columns}

    def find_col(keys):
        # exact normalized match via synonyms set
        for k in keys:
            if k in norm2orig:
                return norm2orig[k]
        # fuzzy contains if not found
        for nk, orig in norm2orig.items():
            for k in keys:
                if k in nk:
                    return orig
        return None

    def pick(field):
        return find_col(SYN[field])

    rename_map = {}
    # Pick columns
    c_symbol = pick("symbol")
    c_series = pick("series")
    c_open   = pick("open")
    c_high   = pick("high")
    c_low    = pick("low")
    c_close  = pick("close")
    c_last   = pick("last")
    c_prev   = pick("prev_close")
    c_vwap   = pick("vwap")
    c_vol    = pick("volume")
    c_val    = pick("turnover")
    c_trd    = pick("trades")
    c_dv     = pick("deliv_vol")
    c_dp     = pick("deliv_pct")
    c_isin   = pick("isin")

    # Rename what we have
    if c_symbol: rename_map[c_symbol] = "Symbol"
    if c_series: rename_map[c_series] = "Series"
    if c_open:   rename_map[c_open]   = "Open"
    if c_high:   rename_map[c_high]   = "High"
    if c_low:    rename_map[c_low]    = "Low"
    if c_close:  rename_map[c_close]  = "Close"
    if c_last:   rename_map[c_last]   = "Last"
    if c_prev:   rename_map[c_prev]   = "Prev Close"
    if c_vwap:   rename_map[c_vwap]   = "VWAP"
    if c_vol:    rename_map[c_vol]    = "Volume"
    if c_val:    rename_map[c_val]    = "Turnover"
    if c_trd:    rename_map[c_trd]    = "Trades"
    if c_dv:     rename_map[c_dv]     = "Deliverable Volume"
    if c_dp:     rename_map[c_dp]     = "Deliverable %"
    if c_isin:   rename_map[c_isin]   = "ISIN"

    df = df.rename(columns=rename_map).copy()

    # Ensure Date column
    df["Date"] = report_date

    # Unit normalization
    # Volume: if source column name suggests '000s', multiply by 1,000
    if "Volume" in df.columns and c_vol:
        vol_src_name = c_vol.lower()
        if "000" in vol_src_name or "thousand" in vol_src_name:
            df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce") * 1000
        else:
            df["Volume"] = pd.to_numeric(df["Volume"], errors="coerce")

    # Turnover: convert to INR (rupees)
    if "Turnover" in df.columns and c_val:
        val_src = c_val.lower()
        x = pd.to_numeric(df["Turnover"], errors="coerce")
        if "lakh" in val_src or "lac" in val_src or "lacs" in val_src:
            df["Turnover"] = x * 1e5
        elif "cr" in val_src or "crore" in val_src or "crores" in val_src:
            df["Turnover"] = x * 1e7
        else:
            df["Turnover"] = x

    # Numerics for price fields
    for col in ["Open","High","Low","Close","Last","Prev Close","VWAP","Trades","Deliverable Volume","Deliverable %"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Compute VWAP if missing and we have Turnover and Volume
    if "VWAP" not in df.columns and {"Turnover","Volume"}.issubset(df.columns):
        with pd.option_context('mode.use_inf_as_na', True):
            df["VWAP"] = (df["Turnover"] / df["Volume"]).replace([pd.NA], None)

    # Compute Deliverable % if missing and we have deliverable volume and total volume
    if "Deliverable %" not in df.columns and {"Deliverable Volume","Volume"}.issubset(df.columns):
        df["Deliverable %"] = (df["Deliverable Volume"] / df["Volume"] * 100).replace([pd.NA], None)

    # Final column order
    cols_order = [
        "Date","Symbol","Series","Prev Close","Open","High","Low","Last","Close",
        "VWAP","Volume","Turnover","Trades","Deliverable Volume","Deliverable %","ISIN"
    ]
    # Keep only available among desired order, then append any extras
    present = [c for c in cols_order if c in df.columns]
    extras = [c for c in df.columns if c not in present]
    df = df[present + extras]

    return df

# Your 50 stock symbols list (NSE codes)
WATCHLIST = [
    "RELIANCE","HDFCBANK","BHARTIARTL","TCS","ICICIBANK","SBIN","BAJFINANCE",
    "INFY","HINDUNILVR","LICI","MARUTI","LT","ITC","M&M","KOTAKBANK",
    "SUNPHARMA","HCLTECH","AXISBANK","ULTRACEMCO","NTPC","BAJAJFINSV",
    "HAL","ADANIPORTS","ONGC","TITAN","DMART","ADANIENT","BEL","ADANIPOWER",
    "JSWSTEEL","POWERGRID","WIPRO","BAJAJ-AUTO","TATAMOTORS","COALINDIA",
    "ASIANPAINT","NESTLEIND","TATASTEEL","IOC","HINDZINC",
    "EICHERMOT","GRASIM","SBILIFE","VEDL","DLF","ADANIGREEN"
]

def filter_selected_stocks(df: pd.DataFrame, watchlist=WATCHLIST) -> pd.DataFrame:
    """Filter only selected stocks from merged bhavcopy DataFrame."""
    if df is None or df.empty:
        return pd.DataFrame()  # nothing to filter
    filt_df = df[df["Symbol"].isin(watchlist)].copy()
    return filt_df

def get_bhavcopy_for_date(report_date: date):
    if report_date.weekday() >= 5:
        return None  # weekend
    if report_date < UDIFF_START_DATE:
        raw = download_old_bhavcopy(report_date)
    else:
        raw = download_new_bhavcopy(report_date)
    if raw is None or raw.empty:
        return None
    return standardize_bhavcopy(raw, report_date)

def save_excel_by_year(filtered_df: pd.DataFrame, start_date: date, end_date: date):
    """Save filtered data to Excel with separate sheets for each year"""
    if filtered_df.empty:
        print("No data to save!")
        return

    # Extract year from Date column
    filtered_df['Year'] = pd.to_datetime(filtered_df['Date']).dt.year

    # Get unique years in the data
    years = sorted(filtered_df['Year'].unique())

    # Create Excel writer
    excel_name = f"NSE_Selected_Stocks_{start_date}_to_{end_date}.xlsx"

    with pd.ExcelWriter(excel_name, engine='openpyxl') as writer:
        # Save each year to separate sheet
        for year in years:
            year_data = filtered_df[filtered_df['Year'] == year].copy()
            # Remove the Year column before saving
            year_data = year_data.drop('Year', axis=1)

            # Sort by Date and Symbol for better organization
            year_data = year_data.sort_values(['Date', 'Symbol'])

            sheet_name = f"Year_{year}"
            # Excel sheet names have max 31 characters
            if len(sheet_name) > 31:
                sheet_name = sheet_name[:31]

            year_data.to_excel(writer, sheet_name=sheet_name, index=False)

            print(f"✅ Saved {len(year_data)} rows for year {year} in sheet '{sheet_name}'")

        # Also create a summary sheet with all data
        # summary_data = filtered_df.drop('Year', axis=1).copy()
        # summary_data = summary_data.sort_values(['Date', 'Symbol'])
        # summary_data.to_excel(writer, sheet_name='All_Data', index=False)
        # print(f"✅ Saved {len(summary_data)} rows in summary sheet 'All_Data'")

        # Create a statistics sheet
        stats_data = create_statistics_sheet(filtered_df)
        stats_data.to_excel(writer, sheet_name='Statistics', index=True)
        print("✅ Created statistics sheet")

    print(f"\n💾 Excel file saved: {excel_name}")
    return excel_name

def create_statistics_sheet(df: pd.DataFrame) -> pd.DataFrame:
    """Create a statistics summary sheet"""
    stats = []

    # Overall statistics
    stats.append(["Total Records", len(df)])
    stats.append(["Unique Stocks", df['Symbol'].nunique()])
    stats.append(["Date Range", f"{df['Date'].min()} to {df['Date'].max()}"])
    stats.append(["Total Trading Days", df['Date'].nunique()])

    # Year-wise statistics
    years = sorted(df['Year'].unique())
    stats.append(["", ""])  # Empty row for separation

    for year in years:
        year_data = df[df['Year'] == year]
        stats.append([f"Year {year} - Records", len(year_data)])
        stats.append([f"Year {year} - Trading Days", year_data['Date'].nunique()])
        stats.append([f"Year {year} - Stocks", year_data['Symbol'].nunique()])

    # Stock-wise record count
    stats.append(["", ""])  # Empty row for separation
    stats.append(["Records per Stock:", ""])

    stock_counts = df['Symbol'].value_counts().head(10)  # Top 10 stocks by record count
    for stock, count in stock_counts.items():
        stats.append([f"  {stock}", count])

    return pd.DataFrame(stats, columns=["Statistic", "Value"])

# ===================== RUN =====================
if __name__ == "__main__":
    # Configure your date range here
    # starting date...
    strt_year = int(input("Enter Start year (YYYY): "))
    strt_month = int(input("Enter Start month (MM): "))
    strt_day = int(input("Enter Start day (DD): "))
    start_date = date(strt_year, strt_month, strt_day)
    # Ending Date...
    end_year = int(input("Enter end year (YYYY): "))
    end_month = int(input("Enter end month (MM): "))
    end_day = int(input("Enter end day (DD): "))
    end_date   = date(end_year, end_month, end_day)

    all_dfs = []
    cur = start_date
    total_ok = 0
    total_fail = 0

    print(f"Fetching NSE Bhavcopy from {start_date} to {end_date}")
    print(f"Old method until {UDIFF_START_DATE - timedelta(days=1)}, nsepython from {UDIFF_START_DATE} onwards")
    print("="*70)

    while cur <= end_date:
        if cur.weekday() >= 5:
            print(f"Skipping weekend: {cur}")
            cur += timedelta(days=1)
            continue

        method = "OLD(archive)" if cur < UDIFF_START_DATE else "NEW(nsepython)"
        print(f"{cur} [{method}] ... ", end="")
        df = get_bhavcopy_for_date(cur)
        if df is not None and not df.empty:
            all_dfs.append(df)
            total_ok += 1
            print(f"ok ({len(df)} rows)")
        else:
            total_fail += 1
            print("no data")
        cur += timedelta(days=1)
        time.sleep(1.2)  # polite delay

    if all_dfs:
       # Get main DataFrame
       master = pd.concat(all_dfs, ignore_index=True)

       # Save master CSV (optional)
       csv_name = f"NSE_Bhavcopy_merged_{start_date}_to_{end_date}.csv"
       master.to_csv(csv_name, index=False)
       print(f"💾 Master CSV saved: {csv_name}")

       # Filter for watchlist
       filtered = filter_selected_stocks(master)

       if not filtered.empty:
           # Save to Excel with separate sheets by year
           excel_name = save_excel_by_year(filtered, start_date, end_date)

           print(f"\n🎉 SUCCESS!")
           print(f"📊 Total trading days processed: {total_ok}")
           print(f"📈 Total records in master file: {len(master):,}")
           print(f"⭐ Watchlist records: {len(filtered):,}")
           print(f"📅 Data spans {filtered['Date'].nunique()} trading days")

           # Display sample
           print(f"\n📋 Sample data:")
           display(filtered.head(10))

       else:
           print(f"\n⚠️ No rows matched your watchlist of {len(WATCHLIST)} stocks!")
           print("Please check if the symbol names match NSE trading symbols.")

    else:
        print("❌ No data downloaded for the specified period!")
