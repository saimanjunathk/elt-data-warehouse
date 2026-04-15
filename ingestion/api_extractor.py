import requests
import pandas as pd
import os

class APIExtractor:
    def __init__(self, base_url: str, output_dir: str = "data/raw"):
        self.base_url = base_url        # stores the URL for later use
        self.output_dir = output_dir    # stores the output folder path

        os.makedirs(self.output_dir, exist_ok=True)

    def fetch(self, endpoint: str) -> pd.DataFrame:
        url = f"{self.base_url}{endpoint}"

        print(f"Fetching data from: {url}")
        response = requests.get(url)
        response.raise_for_status()

        df = pd.DataFrame(response.json())

        print(f"Successfully fetched {len(df)} records from {endpoint}")

        return df

    def save(self, df: pd.DataFrame, filename: str):

        filepath = os.path.join(self.output_dir, filename)


        df.to_csv(filepath, index=False)

        print(f"Saved {len(df)} records to {filepath}")



    def extract_and_save(self, endpoint: str, filename: str) -> pd.DataFrame:
        df = self.fetch(endpoint)
        self.save(df, filename)
        return df

if __name__ == "__main__":

    extractor = APIExtractor(
        base_url="https://jsonplaceholder.typicode.com"
    )

    users_df = extractor.extract_and_save(
        endpoint="/users",
        filename="users.csv"
    )

    posts_df = extractor.extract_and_save(
        endpoint="/posts",
        filename="posts.csv"
    )

    todos_df = extractor.extract_and_save(
        endpoint="/todos",
        filename="todos.csv"
    )

    print("\nSample Users Data:")
    print(users_df.head(3))