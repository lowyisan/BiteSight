# reduce_data.py
# Contributor(s): Low Yi San, adapted for HDFS flat file output

import sys
from pyspark.sql import SparkSession
import subprocess

def truncate_tsv_hdfs(input_path, temp_output_dir, final_output_file, max_lines=10000):
    """
    Reads input_path from HDFS, keeps first N lines, writes to HDFS as a single file.
    Parameters:
        input_path (str): HDFS input path (e.g., /output/join/raw-r-00000)
        temp_output_dir (str): Temporary HDFS output directory (e.g., /tmp/small-raw-r-00000-tmp)
        final_output_file (str): Final HDFS file (e.g., /input/dataset/small-raw-r-00000)
    """
    spark = SparkSession.builder.appName("ReduceTSV").getOrCreate()

    print(f"Reading from: {input_path}")
    df = spark.read.csv(input_path, sep="\t", header=False)
    df_limited = df.limit(max_lines)

    print(f"Writing temporarily to: {temp_output_dir}")
    df_limited.coalesce(1).write.mode("overwrite").option("sep", "\t").csv(temp_output_dir)

    spark.stop()

    # Merge to single file in HDFS
    print(f"Merging part file into final file: {final_output_file}")
    subprocess.run([
        "hdfs", "dfs", "-rm", "-f", final_output_file
    ], check=False)  # Remove target if exists

    subprocess.run([
        "hdfs", "dfs", "-cat", f"{temp_output_dir}/part-*.csv"
    ], stdout=open("temp_local_file.tsv", "w"))

    subprocess.run([
        "hdfs", "dfs", "-put", "-f", "temp_local_file.tsv", final_output_file
    ])

    subprocess.run(["rm", "temp_local_file.tsv"])
    subprocess.run(["hdfs", "dfs", "-rm", "-r", temp_output_dir])

    print(f"Done: {final_output_file} created in HDFS")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: spark-submit reduce_data.py input_tsv output_tsv [max_lines=10000]")
        sys.exit(1)

    input_path = sys.argv[1]                    # e.g., /output/join/raw-r-00000
    final_output_file = sys.argv[2]             # e.g., /input/dataset/small-raw-r-00000
    max_lines = int(sys.argv[3]) if len(sys.argv) > 3 else 10000

    # We use a temp directory to store Spark output before merging
    temp_output_dir = final_output_file + "-tmp"

    truncate_tsv_hdfs(input_path, temp_output_dir, final_output_file, max_lines)
