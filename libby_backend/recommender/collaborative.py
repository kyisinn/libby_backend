"""
collaborative.py
----------------
User-based Collaborative Filtering for Libby-Bot.
Predicts ratings based on similar users.
"""

import pandas as pd
from math import sqrt

def pearson_similarity(user_ratings, other_ratings):
    common = user_ratings.index.intersection(other_ratings.index)
    if len(common) == 0:
        return 0
    x = user_ratings[common]
    y = other_ratings[common]
    num = ((x - x.mean()) * (y - y.mean())).sum()
    den = sqrt(((x - x.mean())**2).sum()) * sqrt(((y - y.mean())**2).sum())
    return num / den if den != 0 else 0

def predict_ratings(rating_df, target_user, k=5):
    similarities = []
    target_ratings = rating_df.loc[target_user].dropna()

    for other in rating_df.index:
        if other == target_user:
            continue
        sim = pearson_similarity(target_ratings, rating_df.loc[other].dropna())
        similarities.append((other, sim))
    
    # Top-K similar users
    similarities = sorted(similarities, key=lambda x: x[1], reverse=True)[:k]

    predictions = {}
    for book in rating_df.columns:
        if pd.isna(rating_df.loc[target_user, book]):
            num, den = 0, 0
            for (neighbor, sim) in similarities:
                if not pd.isna(rating_df.loc[neighbor, book]):
                    num += sim * rating_df.loc[neighbor, book]
                    den += abs(sim)
            if den != 0:
                predictions[book] = num / den

    return predictions