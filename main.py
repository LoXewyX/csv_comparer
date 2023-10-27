import csv
import ctypes
import math
from pathlib import Path
import re
import shutil
import pickle
import timeit
import pandas as pd
from alive_progress import alive_bar


class LoadCSV:
    def __init__(
        self,
        chunk_size: int,
        output_prefix: str,
        temp_dir: str,
        path1: str,
        path2: str,
        delimitator1: str,
        delimitator2: str,
        charset1: str,
        charset2: str,
    ) -> None:
        self.chunk_size: int = chunk_size
        out_path1: Path = Path(temp_dir) / Path(path1).stem
        out_path2: Path = Path(temp_dir) / Path(path2).stem
        csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))

        if Path(temp_dir).is_dir():
            shutil.rmtree(temp_dir)
        else:
            Path(temp_dir).mkdir(parents=True, exist_ok=True)

        out_path1.mkdir(parents=True, exist_ok=True)
        out_path2.mkdir(parents=True, exist_ok=True)

        df1 = self.export_chunks(path1, chunk_size, delimitator1, charset1)
        df2 = self.export_chunks(path2, chunk_size, delimitator2, charset2)

        df1 = df1.map(lambda x: str(x) if pd.notna(x) else "")
        df2 = df2.map(lambda x: str(x) if pd.notna(x) else "")

        df1 = df1.sort_values(by=df1.columns[0])
        df2 = df2.sort_values(by=df2.columns[0])

        print("Processing chunks...")
        self.dump_csv(df1, chunk_size, out_path1, output_prefix)
        self.dump_csv(df2, chunk_size, out_path2, output_prefix)
        print()

        self.compare_csv((out_path1, out_path2))

    def dump_csv(
        self, data: pd.DataFrame, chunk_size: int, output_path: Path, output_prefix: str
    ):
        total_chunks = math.ceil(len(data) / chunk_size)
        with alive_bar(total_chunks) as bar:
            for i, chunk in enumerate(range(0, len(data), chunk_size)):
                chunk_data = data.iloc[chunk : chunk + chunk_size]
                output_file = output_path / f"{output_prefix}_{i}.pkl"
                bar.title = f"{output_prefix}_{i}.pkl"

                with open(output_file, "wb") as file:
                    pickle.dump(chunk_data, file)
                bar()

    def read_dumped_files(self, out_path: Path):
        dfs = []

        with alive_bar(len(list(out_path.glob("*.pkl")))) as bar:
            for pkl_file in out_path.glob("*.pkl"):
                with open(pkl_file, "rb") as file:
                    bar.text(f"{pkl_file.name}")
                    df: pd.DataFrame = pickle.load(file)
                    dfs.append(df)
                    bar()

        return pd.concat(dfs, ignore_index=True)

    def compare_csv(self, out_paths: tuple, output_file: str = "log.txt"):
        print("Reading chunks...")
        file1 = self.read_dumped_files(out_paths[0])
        file2 = self.read_dumped_files(out_paths[1])

        print(
            "\n{} rows and {} columns on {}.".format(
                len(file1), len(file1.columns), str(out_paths[0])
            )
        )
        print(
            "{} rows and {} columns on {}.".format(
                len(file2), len(file2.columns), str(out_paths[1])
            )
        )

        output_text = ""

        if file1.columns.tolist() == file2.columns.tolist():
            output_text += "No different headers.\n"
        else:
            added_columns = set(file2.columns) - set(file1.columns)
            missing_columns = set(file1.columns) - set(file2.columns)

            output_text += "Different headers:\n"
            for col1, col2 in zip(missing_columns, added_columns):
                output_text += f"{col1} => {col2}\n"

        output_text += "-" * 48 + "\n"
        no_diff = True
        temp_output_text = ""

        for header in file1.columns:
            if header not in file2.columns:
                continue

            col1 = file1[header].tolist()
            col2 = file2[header].tolist()

            different_cells = [
                (i + 1, cell1, cell2)
                for i, (cell1, cell2) in enumerate(zip(col1, col2))
                if cell1 != cell2
            ]

            quotes_pattern = re.compile(r'^"|"$')
            temp_row_output_text = ""

            if different_cells:
                for i, cell1, cell2 in different_cells:
                    cell1 = re.sub(quotes_pattern, "", cell1)
                    cell2 = re.sub(quotes_pattern, "", cell2)
                    if cell1 != cell2:
                        temp_row_output_text += f"Row {i}: {cell1} => {cell2}\n"
                        no_diff = False

            if temp_row_output_text != "":
                temp_output_text += f"Differences in column {header}:\n"
                temp_output_text += temp_row_output_text

        if no_diff:
            output_text += "No different rows.\n"
        else:
            output_text += temp_output_text

        with open(output_file, "w", encoding="utf-8") as file:
            file.write(output_text)

    def export_chunks(
        self,
        csv_file_path: str,
        chunk_size: int,
        delimiter: str,
        encoding: str,
    ):
        chunks = []

        with open(csv_file_path, "r", encoding=encoding, errors="replace") as file:
            try:
                for chunk in pd.read_csv(
                    file,
                    iterator=True,
                    chunksize=chunk_size,
                    encoding=encoding,
                    sep=delimiter,
                    engine="python",
                    quoting=csv.QUOTE_NONE,
                    on_bad_lines="skip",
                ):
                    chunks.append(chunk)
            except pd.errors.ParserError as e:
                print(f"Error reading CSV: {e}")
                if hasattr(e, "message"):
                    print(f"Error message: {e.message}")
                if hasattr(e, "line"):
                    print(f"Error occurred at line {e.line}")

        if not chunks:
            print("No valid chunks found. Check the CSV file for issues.")
            exit()

        df = pd.concat(chunks, ignore_index=True)
        return df


def run():
    LoadCSV(
        chunk_size=int(4000),
        output_prefix="part",
        temp_dir="./temp",
        path1="./file.csv",
        path2="./file.csv",
        delimitator1=",",
        delimitator2=",",
        charset1="utf-8",
        charset2="utf-8",
    )


if __name__ == "__main__":
    execution_time = timeit.timeit(run, number=1)
    print(f"\n\nTook: {execution_time:.2f} seconds")
