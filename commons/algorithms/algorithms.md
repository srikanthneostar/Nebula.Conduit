# Anomaly Detection using Random Forest

This script is designed to perform time-series anomaly detection using a **Random Forest Regressor**. It retrieves data from a SQL database, processes it, trains a model, and identifies anomalous data points based on prediction errors.

---

## 📥 Input

### 1. Command-line Argument
- `--queryid` (int): ID of the SQL query stored in the `APPLICATIONQUERIES` table.

### 2. Database Query
The SQL query fetched using the provided `queryid` must return a dataset with at least the following columns:
- `date` (format: `YYYY-MM-DD`)
- `value` (numeric)

---

## 📤 Output

### Structure:
A dictionary with the following key:
- `"anomalies"`: A Pandas DataFrame of anomalous rows from the test set.

### File Output:
- Writes the anomaly detection result to `outputN.txt`, where `N` is the next available number (e.g., `output1.txt`, `output2.txt`, etc.).

---

## 📋 Example Command

```bash
python anomaly_detector.py --queryid 5


------------------------------------------------------------------------------------------------------------------------------------------


# Correlation Analysis Algorithm

## Overview

This algorithm performs correlation analysis on event-based data retrieved via a SQL query identified by a `queryid`. It extracts temporal and location-based features, calculates summary statistics, and computes the correlation matrix for these features.

---

## 📥 Input

| Parameter | Type   | Description |
|----------|--------|-------------|
| `queryid` | `int` | An integer ID used to fetch a SQL query from the `APPLICATIONQUERIES` table in the database. |

**Required Database Table:**
- `APPLICATIONQUERIES`:
  - Must contain columns: `ID (int)`, `QUERY (text)`

**Expected Query Output Columns:**
- `EVENTDATE` (`datetime`): Timestamp of the event.
- `CARDNUMBER` (`str` or `int`): Unique identifier for a user/card.
- `DEVICEID` (`str`): Device involved in the event.
- `LOCATION` (`str`): Where the event occurred.

---

## 📤 Output

Returns a tuple:
```python
(corr_matrix, features_sample)


--------------------------------------------------------------------------------------------------------------------------------------
# 📈 Time Series Forecasting Algorithm

## Overview

This algorithm performs time series forecasting on a dataset retrieved using a query from a database. It preprocesses the time series data, engineers features, trains an XGBoost regression model, and forecasts future values for a specified number of days.

---

## 📥 Input

| Parameter     | Type   | Description |
|---------------|--------|-------------|
| `queryid`     | `int`  | ID of the query stored in the `APPLICATIONQUERIES` table used to retrieve the time series data. |
| `futuredays`  | `int`  | Number of future days to forecast (default: 30). |

### Required Query Output Format

Your query must return a dataset with **at least 10 rows** and the following two columns:

- `date` (`YYYY-MM-DD`): Timestamp of the observation.
- `value` (`float` or `int`): The target value to be forecasted.

---

## OUTPUT

{
  "category": "week",
  "predictions": [
    {
      "predictedDate": "YYYY-MM-DD",
      "predictedValue": 123
    },
    ...
  ]
}


----------------------------------------------------------------------------------------------------------------------------------

# Pattern Identification Module

This Python module performs **pattern identification**, **clustering**, and **anomaly detection** on event and user data retrieved from a SQL database. It uses a combination of `KMeans` clustering and `IsolationForest` for detecting anomalies based on behavioral and temporal features.

## 📥 Input

The module requires the following inputs:

- **`queryid1` (str)**: ID of the primary SQL query to fetch `event`-based data.
- **`queryid2` (str)**: ID of the secondary SQL query to fetch `user/account`-based data.
- **`n_clusters` (int, optional)**: Number of clusters to use for `KMeans` clustering. Default is `4`.
- **`anomaly_contamination` (float, optional)**: The contamination rate (expected anomaly percentage) for `IsolationForest`. Default is `0.02`.

All data is fetched using the `SqlConnector` class via the `execute_Sql()` method.

## 📤 Output

The module prints the following dataframes for inspection:

- **Continuity Metrics**
- **NUMERIC MATRIX + SCALING**
- **LATENT GROUPINGS (K‑MEANS)**
- **ANOMALY DETECTION (Isolation Forest)**
