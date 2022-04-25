import kaggle


def main():
    """Download Dataset form Kaggle"""
    # You can get the url of the dataset from kaggle.com
    # (i.e strip nicklitwinow/gdp-by-country from https://www.kaggle.com/datasets/nicklitwinow/gdp-by-country
    kaggle.api.dataset_download_files(
        "nicklitwinow/gdp-by-country",
        path="../datasets/nicklitwinow/gdp-by-country/",
        unzip=True,
    )
    kaggle.api.dataset_download_files(
        "psycon/solana-usdt-to-20220-4-historical-data",
        path="../datasets/psycon/solana-usdt-to-20220-4-historical-data",
        unzip=True,
    )


if __name__ == "__main__":
    main()
