from datetime import datetime
import json
import os
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from database.sqlService import SqlConnector
import argparse
from messaging.NebulaRequestProducer import NebulaRequestProducer

sql = SqlConnector()


class AnomalyDetection:
    def __init__(self, queryid: int):
        self.queryid = queryid
        qry = self.get_application_query()
        self.df, self.df2 = self.load_and_prepare_data(qry)
        self.producer = NebulaRequestProducer()

    def get_application_query(self):
        """Get query text safely using parameterized SQL"""
        try:
            qry = "SELECT QUERY FROM APPLICATIONQUERIES WHERE ID = ?"
            results = sql.execute_Sql(qry, (self.queryid,))
            if results.empty:  # Replace "if not results"
                raise ValueError(f"No query found with ID {self.queryid}")

            if isinstance(results, pd.DataFrame):
                return results.iloc[0]["QUERY"]
            elif isinstance(results, list) and len(results) > 0:
                return results[0]["QUERY"]
            else:
                raise ValueError("Unexpected result format from database")
        except Exception as e:
            print(f"Database error: {str(e)}")
            raise

    def load_and_prepare_data(self, query):
        try:
            df = sql.execute_Sql(query)
            last_col_value = df.iloc[0, -1]
            if "date" not in df.columns or "value" not in df.columns:
                print("Missing Date or Value column!")
                return None
            elif (
                not pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
                .notna()
                .all()
            ):
                print(df["date"])
                print("Date format is incorrect!, It should be in YYYY-MM-DD format")
                return None
            df = pd.DataFrame(df, columns=["date", "value"])
            df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
            df = (
                df.drop_duplicates(subset="date")
                .sort_values("date")
                .reset_index(drop=True)
            )
            return df, last_col_value
        except Exception as e:
            print("Error loading data from SQL:", e)
            return pd.DataFrame()

    def feature_engineering(self, df):
        df["rolling_mean"] = df["value"].rolling(window=5).mean()
        df["rolling_std"] = df["value"].rolling(window=5).std()
        df.dropna(inplace=True)
        return df

    def train_and_detect(self, df, df2):
        train_size = int(len(df) * 0.8)
        train, test = df.iloc[:train_size], df.iloc[train_size:]

        features = [col for col in df.columns if col not in ["date", "value"]]
        X_train, y_train = train[features], train["value"]
        X_test, y_test = test[features], test["value"]

        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        y_pred = model.predict(X_test)

        errors = np.abs(y_test - y_pred)
        threshold = np.percentile(errors, 95)
        anomalies = test[errors > threshold]
        # print(f"Detected {len(anomalies)} anomalies out of {len(test)} test points.")
        # print(anomalies)

        response_json = [
            {
                "category": "AnomalyDetection",
                "predictedDate": row["date"],
                "predictedValue": row["value"],
                "entityname": df2,
            }
            for _, row in anomalies.iterrows()
        ]
        print(response_json)
        reqPayLoad = self.producer.get_request_payload(
            "AnomalyDetection", json.dumps(response_json)
        )
        self.producer.get_producer().sendMessage(
            "nebula.processor.sink.forecasting", reqPayLoad
        )
        # file_number = 1
        # while True:
        #     time = datetime.now().strftime("%Y-%m-%d")
        #     output_file = f"anomalyResults-{time}-{file_number}.csv"
        #     if not os.path.exists(output_file):
        #         break
        #     file_number += 1
        # try:
        #     anomalies.to_csv(output_file, index=False)
        # except Exception as e:
        #     print(f"Error writing to file: {str(e)}")
        # json_file = {
        #     "results": anomalies.to_dict(orient="records"),
        #     "output_file": output_file,
        # }
        # return json.dumps(json_file, indent=2)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Anomaly Detection using Random Forest"
    )
    parser.add_argument(
        "--queryid", type=int, required=True, help="Query ID to fetch data"
    )
    args = parser.parse_args()
    anomaly_detector = AnomalyDetection(args.queryid)
    df = anomaly_detector.df
    df2 = anomaly_detector.df2
    if df is not None and not df.empty:
        df = anomaly_detector.feature_engineering(df)
        results = anomaly_detector.train_and_detect(df, df2)
        # print(results, type(results))
        # json_results = results.to_json(orient="records", indent=2)
        # print(results)
    else:
        print("No valid data available for anomaly detection.")
