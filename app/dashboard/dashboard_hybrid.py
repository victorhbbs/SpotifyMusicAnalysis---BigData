import glob
import os
import pandas as pd
import matplotlib.pyplot as plt

BASE = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
INTERIM = os.path.join(BASE, "data", "interim")
REPORTS = os.path.join(BASE, "reports")
os.makedirs(REPORTS, exist_ok=True)

def read_csv_dir(dirpath: str) -> pd.DataFrame:
    files = sorted(glob.glob(os.path.join(dirpath, "*.csv")))
    if not files:
        raise FileNotFoundError(f"Nenhum CSV encontrado em: {dirpath}")
    frames = [pd.read_csv(f) for f in files]
    return pd.concat(frames, ignore_index=True)

top_dir = os.path.join(INTERIM, "top_artists_decade_csv")
top = read_csv_dir(top_dir)
top["decade"] = top["decade"].astype(int)

top10 = (top.sort_values(["decade", "track_count"], ascending=[True, False])
            .groupby("decade").head(10))

for dec, grp in top10.groupby("decade"):
    plt.figure()
    g = grp.sort_values("track_count", ascending=True)
    plt.barh(g["artist"], g["track_count"])
    plt.title(f"Top 10 artistas – Década {dec}")
    plt.xlabel("Qtd. de faixas")
    plt.ylabel("Artista")
    plt.tight_layout()
    plt.savefig(os.path.join(REPORTS, f"top_artists_decade_{dec}.png"))
    plt.close()

yearly_dir = os.path.join(INTERIM, "yearly_popularity_csv")
yearly = read_csv_dir(yearly_dir)
yearly = yearly.sort_values("year")

plt.figure()
plt.plot(yearly["year"], yearly["avg_popularity"])
plt.title("Popularidade média por ano")
plt.xlabel("Ano")
plt.ylabel("Popularidade média")
plt.tight_layout()
plt.savefig(os.path.join(REPORTS, "yearly_popularity.png"))
plt.close()
