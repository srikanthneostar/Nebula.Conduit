import argparse
import pandas as pd
from database.sqlService import SqlConnector
sql = SqlConnector()


class Correlation:
     def __init__(self, queryid: int):
          self.queryid = queryid
          qry = self.get_query()
          self.df = self.main(qry)

     def get_query(self):
          try:
               qry = "SELECT QUERY FROM APPLICATIONQUERIES WHERE ID = ?"
               results = sql.execute_Sql(qry, (self.queryid,)) # type: ignore
               
               if results.empty:  # Replace "if not results"
                    raise ValueError(f"No query found with ID {self.queryid}")
               
               if isinstance(results, pd.DataFrame):
                    return results.iloc[0]['QUERY']
               elif isinstance(results, list) and len(results) > 0:
                    return results[0]['QUERY']
               else:
                    raise ValueError("Unexpected result format from database")
          except Exception as e:
               print(f"Database error: {str(e)}")
               raise

     def main(self,qry):
          
          df = sql.execute_Sql(qry)
          print(df.head())
          if "EVENTDATE" not in df.columns or "CARDNUMBER" not in df.columns:
               return ("Missing Date or Value column!")
          df['EVENTDATE'] = pd.to_datetime(df['EVENTDATE'], errors='coerce')
          df = df.dropna(subset=['EVENTDATE'])
          df['hour'] = df['EVENTDATE'].dt.hour
          df['dayofweek'] = df['EVENTDATE'].dt.dayofweek
          df['is_weekend'] = df['dayofweek'].isin([5, 6]).astype(int)
          df['month'] = df['EVENTDATE'].dt.month

          df['LOCATION'] = df['LOCATION'].fillna('No Location').astype(str)

          features = (
          df.groupby(['LOCATION', 'CARDNUMBER'])
               .agg(unique_devices = ('DEVICEID',  'nunique'),
                    event_count    = ('EVENTDATE', 'count'),
                    avg_hour       = ('hour',      'mean'),
                    avg_dayofweek  = ('dayofweek', 'mean'),
                    weekend_ratio  = ('is_weekend','mean'),
                    active_months  = ('month',     'nunique'))
               .reset_index()
          )

          numeric_cols   = features.select_dtypes(include='number').columns
          corr_matrix    = features[numeric_cols].corr()
          
          return corr_matrix, features.head()

          # features_out = 'single_dataset_features.csv'
          # corr_out     = 'single_dataset_corr.csv'

          # features.to_csv(features_out, index=False)
          # corr_matrix.to_csv(corr_out)
          # print('✔  Features and correlation matrix generated')
          # print(corr_matrix)

          # print(f'✔  Feature table saved to {features_out}')
          # print(f'✔  Correlation matrix saved to {corr_out}')
          # print('\nFeature sample:')
          # print(features.head())

if __name__ == "__main__":
     parser = argparse.ArgumentParser(description="Correlation Analysis")
     parser.add_argument("--queryid", type=int, required=True, help="Query ID to fetch data")
     args = parser.parse_args()
     correlation = Correlation(args.queryid)
     result = correlation.df
     if isinstance(result, tuple) and len(result) == 2:
          corr_matrix, features_sample = result
          print("Correlation Matrix:")
          print(corr_matrix)
          print("\nFeatures Sample:")
          print(features_sample)
     else:
          # Handle error case where main() returned an error message
          print("Error:", result)
     # print("Correlation Matrix:")
     # print(corr_matrix)
     # print("\nFeatures Sample:")
     # print(features_sample)