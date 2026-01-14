from pathlib import Path
import pandas as pd
from kaggle.api.kaggle_api_extended import KaggleApi

class KaggleCSVLoader:
    """
    Downloads, extracts, and loads CSV files into Pandas DataFrames.
    """

    def __init__(
        self,
        dataset_name: str,
        raw_data_dir: str = "data/raw",
        unzip: bool = True,
        parse_dates: bool = True
    ):
        self.dataset_name = dataset_name
        self.raw_data_dir = Path(raw_data_dir)
        self.unzip = unzip
        self.parse_dates = parse_dates

        self.raw_data_dir.mkdir(parents=True, exist_ok=True)
        self.api = KaggleApi()
        self.api.authenticate()
    
    def download_dataset(self) -> None:
        # Only skip if CSVs already exist
        existing_csvs = list(self.raw_data_dir.rglob("*.csv"))
        if existing_csvs:
            print("CVS files already exist. Skipping download.")
            return

        print(f"Downloading dataset: {self.dataset_name}")
        self.api.dataset_download_files(
            self.dataset_name,
            path=self.raw_data_dir,
            unzip=self.unzip 
        )
    
    def load_csv_files(self) -> dict[str, pd.DataFrame]:
        """
        Load all CSV files from download directory
        Returns: dict[str, pd.DataFrame]
        """
        csv_files = list(self.raw_data_dir.rglob("*.csv"))

        if not csv_files:
            raise FileNotFoundError(f"No CSV files found under {self.raw_data_dir.resolve()}!")
        
        dataframes = {}

        for csv_file in csv_files:
            ticker = csv_file.stem

            print(f"Loading {ticker}.csv")

            df = pd.read_csv(
                csv_file,
                parse_dates=['Date'] if self.parse_dates else None
            )

            df = df.sort_values('Date').reset_index(drop=True)

            df['Ticker'] = ticker

            dataframes[ticker] = df
        
        return dataframes
    
    def run(self) -> dict[str, pd.DataFrame]:
        """Full pipeline: download -> load"""
        self.download_dataset()
        return self.load_csv_files()



