import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.ensemble import IsolationForest
import numpy as np
import argparse
from database.sqlService import SqlConnector

sql = SqlConnector()


class patternIdentification:
    def __init__(self, queryid1, queryid2, n_clusters=4, anomaly_contamination=0.02):
        self.queryid1 = queryid1
        self.queryid2 = queryid2
        self.n_clusters = n_clusters
        self.anomaly_contamination = anomaly_contamination
        qry1 = self.get_query(self.queryid1)
        qry2 = self.get_query(self.queryid2)

        self.df = self.process_data(qry1, qry2)

    def get_query(self, query_id):
        try:
            qry = "SELECT QUERY FROM APPLICATIONQUERIES WHERE ID = ?"
            results = sql.execute_Sql(qry, (query_id,))
            if results.empty:
                raise ValueError(f"No query found with ID {query_id}")
            if isinstance(results, pd.DataFrame):
                return results.iloc[0]["QUERY"]
            elif isinstance(results, list) and len(results) > 0:
                return results[0]["QUERY"]
            else:
                raise ValueError("Unexpected result format from database")
        except Exception as e:
            print(f"Database error: {str(e)}")
            raise

    def process_data(self, qry1, qry2):
        print("Processing data with query A:", qry1)
        A = sql.execute_Sql(qry1)
        print(A.head())
        A["EVENTDATE"] = pd.to_datetime(A["EVENTDATE"], errors="coerce")
        A = A.dropna(subset=["EVENTDATE"])
        A["hour"] = A["EVENTDATE"].dt.hour
        A["dayofweek"] = A["EVENTDATE"].dt.dayofweek
        A["is_weekend"] = A["dayofweek"].isin([5, 6]).astype(int)
        A["month"] = A["EVENTDATE"].dt.month
        A["LOCATION"] = A["LOCATION"].fillna("No Location").astype(str)

        weekly = (
            A.set_index("EVENTDATE")
            .groupby("CARDNUMBER")
            .resample("W")["DEVICEID"]
            .count()
            .unstack(fill_value=0)
        )
        if weekly.shape[1] >= 2:
            weekly["week_delta"] = weekly.iloc[:, -1] - weekly.iloc[:, -2]
        else:
            weekly["week_delta"] = 0  # not enough weeks yet
        weekly = weekly[["week_delta"]].reset_index()

        # USER‑LOCATION level features (continuity baseline)
        A_feat = (
            A.groupby(["LOCATION", "CARDNUMBER"])
            .agg(
                unique_devices=("DEVICEID", "nunique"),
                event_count=("EVENTDATE", "count"),
                avg_hour=("hour", "mean"),
                weekend_ratio=("is_weekend", "mean"),
            )
            .reset_index()
        )
        A_feat = A_feat.add_prefix("A_")
        A_feat.rename(
            columns={"A_CARDNUMBER": "CARDNUMBER", "A_LOCATION": "LOCATION"},
            inplace=True,
        )
        print("Processing data with query B:", qry2)
        B = sql.execute_Sql(qry2)
        print(B.head())
        B.columns = [c.strip() for c in B.columns]
        B["ACTIVATIONDATE"] = pd.to_datetime(B["ACTIVATIONDATE"], errors="coerce")
        B = B.dropna(subset=["ACTIVATIONDATE"])
        latest_evt = A["EVENTDATE"].max()
        B["account_age_days"] = (latest_evt - B["ACTIVATIONDATE"]).dt.days
        B["act_year"] = B["ACTIVATIONDATE"].dt.year
        B["act_month"] = B["ACTIVATIONDATE"].dt.month

        B["prev_event_count"] = B.get("PREV_EVENT_COUNT", np.nan)  # optional column
        B["prev_location"] = B.get("PREV_LOCATION", np.nan)

        if "PERSONCARDNUMBER" in B.columns:
            B.rename(columns={"PERSONCARDNUMBER": "CARDNUMBER"}, inplace=True)
        B_feat = B.add_prefix("B_")
        B_feat.rename(columns={"B_CARDNUMBER": "CARDNUMBER"}, inplace=True)

        merged = A_feat.merge(B_feat, on="CARDNUMBER", how="inner").merge(
            weekly, on="CARDNUMBER", how="left"
        )

        # Continuity metrics
        merged["event_diff"] = merged["A_event_count"] - merged["B_prev_event_count"]
        merged["location_switch"] = (
            merged["LOCATION"] != merged["B_prev_location"]
        ).astype(int)

        # Replace NaNs produced by missing prev_* columns with 0
        merged[["event_diff", "location_switch"]] = merged[
            ["event_diff", "location_switch"]
        ].fillna(0)

        # ------------- 5. NUMERIC MATRIX + SCALING ----------------------------------
        num_cols = merged.select_dtypes(include="number").columns
        X = merged[num_cols].fillna(0)
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        # ------------- 6. LATENT GROUPINGS (K‑MEANS) --------------------------------
        kmeans = KMeans(n_clusters=self.n_clusters, random_state=42)
        merged["cluster"] = kmeans.fit_predict(X_scaled)

        # ------------- 7. ANOMALY DETECTION (Isolation Forest) ----------------------
        iso = IsolationForest(contamination=self.anomaly_contamination, random_state=42)
        merged["anomaly"] = iso.fit_predict(X_scaled)  # 1 = normal, -1 = anomaly

        continuity_cols = ["CARDNUMBER", "LOCATION", "event_diff", "location_switch"]
        print("continuity", merged[continuity_cols], "\n")

        deviation_cols = continuity_cols + ["anomaly"]
        print("deviation", merged[deviation_cols], "\n")

        cluster_cols = ["CARDNUMBER", "LOCATION", "cluster"]
        print("cluster", merged[cluster_cols], "\n")

        temporal_cols = ["CARDNUMBER", "week_delta"]
        print("temporal", merged[temporal_cols], "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Process clustering and anomaly detection on event data."
    )
    parser.add_argument(
        "--queryid1", type=str, required=True, help="Query ID 1 to fetch data"
    )
    parser.add_argument(
        "--queryid2", type=str, required=True, help="Query ID 2 to fetch data"
    )
    parser.add_argument(
        "--n_clusters",
        type=int,
        default=4,
        help="Number of KMeans clusters (default: 4)",
    )
    parser.add_argument(
        "--anomaly_contamination",
        type=float,
        default=0.02,
        help="Isolation Forest contamination ratio (default: 0.02)",
    )
    args = parser.parse_args()
    pattern_identification = patternIdentification(
        args.queryid1, args.queryid2, args.n_clusters, args.anomaly_contamination
    )
