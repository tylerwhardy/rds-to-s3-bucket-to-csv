import pandas as pd
import pyarrow.parquet as pq


def convert_to_csv():
    parquet_file = 'file.gz.parquet'
    table = pq.read_table(parquet_file)  # Read the Parquet file using pyarrow

    # Convert the table to a Pandas DataFrame
    data_frame = table.to_pandas()

    # get the original filename and change the extension to .csv
    csv_output_file = parquet_file.replace('.parquet', '.csv')

    # Export the DataFrame as a CSV file
    data_frame.to_csv(csv_output_file, index=False)

    print(f"CSV file has been saved as '{csv_output_file}'.")


if __name__ == '__main__':
    convert_to_csv()
