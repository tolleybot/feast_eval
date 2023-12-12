import pandas as pd
import matplotlib.pyplot as plt

# Load the CSV files into DataFrames
minio_results_path = "/mnt/data/minio_results.csv"
parquet_results_path = "/mnt/data/parquet_test_results.csv"

minio_results_df = pd.read_csv(minio_results_path)
parquet_results_df = pd.read_csv(parquet_results_path)

# Merge the two DataFrames on the common columns ('Number of columns', 'Number of rows')
# and append suffixes to distinguish between the two data sources
combined_df = pd.merge(
    minio_results_df[
        ["Number of columns", "Number of rows", "get_historical_feature read in ms"]
    ],
    parquet_results_df[
        ["Number of columns", "Number of rows", "get_historical_feature read in ms"]
    ],
    on=["Number of columns", "Number of rows"],
    suffixes=("_minio", "_parquet"),
)

# Rounding the milliseconds to the nearest integer
combined_df["get_historical_feature read in ms_minio"] = (
    combined_df["get_historical_feature read in ms_minio"].round(0).astype(int)
)
combined_df["get_historical_feature read in ms_parquet"] = (
    combined_df["get_historical_feature read in ms_parquet"].round(0).astype(int)
)

# Adjusting the x-axis labels
combined_df["Number of rows label"] = combined_df["Number of rows"].apply(
    lambda x: f"{x // 1000}K" if x >= 1000 else str(x)
)
combined_df["Test Cases"] = combined_df.apply(
    lambda row: f"{row['Number of columns']}, {row['Number of rows label']}", axis=1
)

# Plotting
fig, ax = plt.subplots(figsize=(14, 8))

# Setting the positions and width for the bars
positions = list(range(len(combined_df["Test Cases"])))
bar_width = 0.35

# Plotting the bars
minio_bars = ax.bar(
    positions,
    combined_df["get_historical_feature read in ms_minio"],
    bar_width,
    label="MinIO",
    color="skyblue",
)
parquet_bars = ax.bar(
    [p + bar_width for p in positions],
    combined_df["get_historical_feature read in ms_parquet"],
    bar_width,
    label="Parquet",
    color="lightgreen",
)

# Adding the labels, title, and legend
ax.set_xlabel("Test Cases (Number of columns, Number of rows)")
ax.set_ylabel("get_historical_feature read in ms (rounded)")
ax.set_title("Comparison of get_historical_feature Read Times (MinIO vs Parquet)")
ax.set_xticks([p + bar_width / 2 for p in positions])
ax.set_xticklabels(combined_df["Test Cases"])
ax.legend()

# Adding the values on top of the bars
for bar in minio_bars + parquet_bars:
    height = bar.get_height()
    ax.annotate(
        f"{height}",
        xy=(bar.get_x() + bar.get_width() / 2, height),
        xytext=(0, 3),  # 3 points vertical offset
        textcoords="offset points",
        ha="center",
        va="bottom",
    )

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
