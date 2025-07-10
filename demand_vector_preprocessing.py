import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import os

# file path
csv_path = 'youtube_demand_aggregated.csv'

# read data
df = pd.read_csv(csv_path)

# データをピボットして、概念 × （地域ごとのview/like/comment）ベクトルに変換
pivoted = df.pivot_table(
    index='concept',
    columns='region',
    values=['viewCount', 'likeCount', 'commentCount'],
    aggfunc='sum',
    fill_value=0  # 欠損は0で埋める
)

# カラム名の整形
pivoted.columns = [f"{stat}_{region}" for stat, region in pivoted.columns]

# Normalization
scaler = StandardScaler()
normalized_array = scaler.fit_transform(pivoted)
normalized_df = pd.DataFrame(normalized_array, index=pivoted.index, columns=pivoted.columns)

# Preservation
pivoted.to_csv("youtube_demand_vector_raw.csv")
normalized_df.to_csv("youtube_demand_vector_normalized.csv")

# Confirmation
print("元ベクトルデータ（例）:")
print(pivoted.head())
print("正規化済みベクトルデータ（例）:")
print(normalized_df.head())
