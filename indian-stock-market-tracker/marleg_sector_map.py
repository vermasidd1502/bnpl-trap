"""Render the current sector -> sub-sector (industry) taxonomy as a sunburst.
Outputs marle_g_sectormap.png (image) + marle_g_sectormap.html (interactive)."""
import json
from collections import Counter
import plotly.graph_objects as go

s = json.load(open("marleg_sectors.json", encoding="utf-8"))
pairs = [(v["sector"], v["industry"]) for v in s.values() if v["sector"] != "Others"]
seccnt = Counter(p[0] for p in pairs)
indcnt = Counter(pairs)

ids, labels, parents, values = [], [], [], []
for sec in seccnt:
    ids.append("S::" + sec); labels.append(f"{sec}<br>{seccnt[sec]}"); parents.append(""); values.append(seccnt[sec])
for (sec, ind) in indcnt:
    ids.append("S::" + sec + "||" + ind); labels.append(f"{ind} ({indcnt[(sec, ind)]})")
    parents.append("S::" + sec); values.append(indcnt[(sec, ind)])

fig = go.Figure(go.Sunburst(ids=ids, labels=labels, parents=parents, values=values,
                            branchvalues="total", insidetextorientation="radial",
                            marker=dict(line=dict(color="#0e1116", width=1))))
fig.update_layout(title=dict(text=f"Marle-G — NSE Sector / Sub-sector Map · {len(pairs)} stocks · "
                                  f"{len(seccnt)} sectors · {len(indcnt)} sub-sectors",
                             font=dict(color="#ff9933", size=15)),
                  paper_bgcolor="#0e1116", font=dict(color="#eef1f6", size=11),
                  width=1200, height=1200, margin=dict(t=55, l=8, r=8, b=8))
fig.write_html("marle_g_sectormap.html", include_plotlyjs=True)
fig.write_image("marle_g_sectormap.png", scale=2)
print(f"wrote marle_g_sectormap.png + .html ({len(pairs)} stocks, {len(indcnt)} sub-sectors)")
