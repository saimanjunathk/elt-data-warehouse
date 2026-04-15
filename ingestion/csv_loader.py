import pandas as pd
import os
import logging

logging.basicConfig(
    level=logging.INFO,           
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CSVLoader:
    def __init__(self, input_dir: str = "data/raw"):
        self.input_dir = input_dir
    def load(self, filename: str) -> pd.DataFrame:
        filepath = os.path.join(self.input_dir, filename)
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"CSV file not found: {filepath}")
        logger.info(f"Loading CSV file: {filepath}")
        df = pd.read_csv(filepath, encoding="utf-8", on_bad_lines="skip")
        logger.info(f"Loaded {len(df)} rows and {len(df.columns)} columns from {filename}")
        return df
    def clean(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.dropna(how="all")
        after = len(df)
        if before != after:
            logger.warning(f"Removed {before - after} completely empty rows")
        df.columns = (
            df.columns
            .str.strip()           
            .str.lower()           
            .str.replace(" ", "_") 
            .str.replace("-", "_") 
        )
        string_cols = df.select_dtypes(include=["object", "str"]).columns
        df[string_cols] = df[string_cols].apply(lambda x: x.str.strip())
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        if before != after:
            logger.warning(f"Removed {before - after} duplicate rows")
        logger.info(f"Cleaning complete. Final shape: {df.shape}")
        return df
    def summarize(self, df: pd.DataFrame, name: str = "DataFrame"):
        print(f"\n{'='*50}")
        print(f"Summary: {name}")
        print(f"{'='*50}")
        print(f"Rows:    {df.shape[0]}")
        print(f"Columns: {df.shape[1]}")
        print(f"\nColumn Names & Data Types:")
        print(df.dtypes)
        print(f"\nMissing Values per Column:")
        print(df.isnull().sum())
        print(f"\nFirst 3 rows:")
        print(df.head(3))
        print(f"{'='*50}\n")
    def load_and_clean(self, filename: str) -> pd.DataFrame:
        df = self.load(filename)
        df = self.clean(df)
        return df
if __name__ == "__main__":
    loader = CSVLoader(input_dir="data/raw")
    users_df = loader.load_and_clean("users.csv")
    posts_df = loader.load_and_clean("posts.csv")
    todos_df = loader.load_and_clean("todos.csv")
    loader.summarize(users_df, "Users")
    loader.summarize(posts_df, "Posts")
    loader.summarize(todos_df, "Todos")