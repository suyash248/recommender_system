import pandas as pd
from settings import project_root

k_recommendations = 5
rankmod_threshold = 0.5
input_prod_cat_id = 100

df = pd.read_csv(project_root + '/test/finalresult3.csv')
df.sort_values(['rankmod', 'avg_rating', 'reviews'], ascending=[False, False, False], inplace=True)
df.drop(df[df.rankmod < rankmod_threshold].index, inplace=True)

out_df = same_cat_df = df.drop(df[df.category_cat != input_prod_cat_id].index).head(k_recommendations)
recomm_size = same_cat_df.shape[0]

if recomm_size < k_recommendations:
    diff_cat_df = df.drop(df[df.category_cat == input_prod_cat_id].index)
    frames = [same_cat_df, diff_cat_df.head(k_recommendations - recomm_size)]
    out_df = pd.concat(frames)

print out_df