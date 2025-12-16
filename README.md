# 📊 NSE Historical Data Downloader 🚀

### 👨‍💻 Project Overview

This is a **robust Python script** designed to automatically **download, clean, and organize historical End-of-Day (EOD) Bhavcopy data** from the National Stock Exchange (NSE) for a selected watchlist of stocks. It handles legacy and modern data formats seamlessly.

* **Goal:** To provide clean, normalized, historical NSE trading data ready for immediate analysis.
* **Core Technologies:** Python, Pandas, Requests, `nsepython`.

---

### ✨ Key Features

| Feature | Description |
| :--- | :--- |
| **Dual Data Sourcing** | Seamlessly uses **old NSE archives (ZIP files)** and the modern **`nsepython` API** to ensure comprehensive historical coverage. |
| **Data Normalization** | Includes advanced logic to **standardize** column names, perform **unit conversions** (e.g., Lakhs/Crores to INR), and calculate missing fields like **VWAP** and **Deliverable %**. |
| **Automation** | **Auto-installs** all necessary Python dependencies (`pandas`, `requests`, `nsepython`) upon first run. |
| **Organized Output** | Saves the final results into a single **Excel file**, automatically creating separate, tidy sheets for **each year** in the downloaded range. |
| **Filtering** | Processes data only for the specific list of stocks defined in the Python script's **`WATCHLIST`**. |

---

### 🚀 Getting Started

1.  **Save the Script:** Save the provided Python code as a file named `nse_downloader.py`.
2.  **Execute from Terminal:** Run the script using the Python interpreter:
    ```bash
    python nse_downloader.py
    ```
3.  **Input Dates:** The script will prompt you to interactively enter the **Start** and **End** dates for the data range (YYYY, MM, DD).
4.  **Check Output:** The processed Excel file will be saved in the same directory, named like:
    `NSE_Selected_Stocks_YYYY-MM-DD_to_YYYY-MM-DD.xlsx`

---

### ⚙️ Configuration

To customize the list of stock symbols tracked by the downloader, modify the **`WATCHLIST`** array directly inside the `nse_downloader.py` file:

```python
# Your 50 stock symbols watchlist (NSE codes)
WATCHLIST = [
    "RELIANCE",
    "HDFCBANK",
    "TCS",
    # Add or remove NSE trading symbols here
    # ...
]
```
### 📧 Contact
Created by [Vipul Yadav](https://github.com/vipularyan)

---
### 🛑 Licensing and Usage Restrictions

This project is protected by **All Rights Reserved** copyright.

* You are welcome to view and study the code for personal learning.
* **You may NOT copy, redistribute, modify, or reuse this code for any other project (commercial or non-commercial).**
* For any exceptions, please contact [Vipul Yadav](https://github.com/vipularyan).
