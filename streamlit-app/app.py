import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import pyarrow as pa
import requests
import io
import time

st.set_page_config(
    page_title="Lambda Architecture — Music Analytics",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded",
)

@st.cache_data
def load_data():
    try:
        cleaned      = pd.read_parquet("/data/cleaned.parquet")
        clustered    = pd.read_parquet("/data/clustered.parquet")
        decade_stats = pd.read_parquet("/data/decade_stats.parquet")
        cluster_stats = pd.read_parquet("/data/cluster_stats.parquet")

        num_cols = ["popularity", "danceability", "energy", "valence",
                    "acousticness", "instrumentalness", "liveness",
                    "loudness", "speechiness", "tempo"]
        for col in num_cols:
            for frame in [cleaned, clustered]:
                if col in frame.columns:
                    frame[col] = pd.to_numeric(frame[col], errors="coerce")

        for col in ["avg_popularity", "avg_danceability", "avg_energy", "avg_valence",
                    "avg_loudness", "avg_acousticness", "avg_tempo",
                    "track_count", "explicit_pct", "explicit_count"]:
            for frame in [decade_stats, cluster_stats]:
                if col in frame.columns:
                    frame[col] = pd.to_numeric(frame[col], errors="coerce")

        if "year" in cleaned.columns:
            cleaned["year"] = pd.to_numeric(cleaned["year"], errors="coerce")
        if "year" in clustered.columns:
            clustered["year"] = pd.to_numeric(clustered["year"], errors="coerce")

        return cleaned, clustered, decade_stats, cluster_stats
    except Exception as e:
        st.error(f"Lỗi load data: {e}")
        st.stop()


with st.spinner("Đang tải dữ liệu..."):
    cleaned, clustered, decade_stats, cluster_stats = load_data()

cluster_labels = {
    0: "High Energy",      1: "Acoustic & Calm",
    2: "Dance & Party",    3: "Dark & Melancholic",
    4: "Happy & Upbeat",   5: "Instrumental",
    6: "Vocal & Speech",   7: "Chill & Lo-fi",
}
clustered["cluster_name"] = clustered["cluster"].map(
    lambda x: cluster_labels.get(x, f"Cluster {x}")
)

WEBHDFS = "http://namenode:9870/webhdfs/v1"

@st.cache_data(ttl=30)
def load_streaming_data():
    path = "/music/streaming/tracks"
    try:
        url = f"{WEBHDFS}{path}?op=LISTSTATUS"
        files = requests.get(url, timeout=5).json()["FileStatuses"]["FileStatus"]
        parquet_files = [f for f in files
                         if f.get("type") == "FILE" and ".parquet" in f["pathSuffix"]]
        if not parquet_files:
            return pd.DataFrame()
        dfs = []
        for f in parquet_files[:20]:
            file_url = f"{WEBHDFS}{path}/{f['pathSuffix']}?op=OPEN"
            resp = requests.get(file_url, allow_redirects=True, timeout=10)
            dfs.append(pq.read_table(io.BytesIO(resp.content)).to_pandas())
        df = pd.concat(dfs, ignore_index=True)
        for col in ["popularity", "danceability", "energy", "valence", "year"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as e:
        st.warning(f"HDFS streaming không khả dụng: {e}")
        return pd.DataFrame()


# ── Sidebar ───────────────────────────────────────────────────
st.sidebar.title("Music Analytics")
st.sidebar.markdown("*Lambda Architecture · Kafka · Spark · HDFS*")
st.sidebar.markdown("---")
page = st.sidebar.radio("Chuyên mục", [
    "Overview", "Timeline Analysis", "Cluster Analysis", "Top Tracks", "Live Stream",
])
st.sidebar.markdown("---")
st.sidebar.metric("Total Tracks", f"{len(cleaned):,}")

decade_col = "decade" if "decade" in cleaned.columns else None
year_col   = "year"   if "year"   in cleaned.columns else None

if "track_genre" in cleaned.columns:
    st.sidebar.metric("Genres", f"{cleaned['track_genre'].nunique()}")
elif decade_col:
    valid_decades = cleaned[decade_col].dropna().unique()
    valid_decades = [d for d in valid_decades if str(d).replace("s","").isdigit() and int(str(d).replace("s","")) > 1900]
    if valid_decades:
        st.sidebar.metric("Decades Covered", f"{len(valid_decades)}")
if year_col:
    yr_valid = cleaned[year_col].dropna()
    yr_valid = yr_valid[yr_valid > 1900]
    if not yr_valid.empty:
        st.sidebar.metric("Year Range", f"{int(yr_valid.min())} – {int(yr_valid.max())}")
st.sidebar.metric("Total Artists", f"{cleaned['artists'].nunique():,}")


# ══════════════════════════════════════════════════════════════
# PAGE: Overview
# ══════════════════════════════════════════════════════════════
if page == "Overview":
    st.title("Phân Tích Âm Nhạc — Lambda Architecture")
    st.markdown("*89K+ bài hát · Kafka · Spark · HDFS · Hive · KMeans*")
    st.markdown("---")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tổng bài hát",   f"{len(cleaned):,}")
    c2.metric("Nghệ sĩ",        f"{cleaned['artists'].nunique():,}")
    if year_col:
        yr_v = cleaned[year_col].dropna()
        yr_v = yr_v[yr_v > 1900]
        if not yr_v.empty:
            c3.metric("Khoảng năm", f"{int(yr_v.min())} – {int(yr_v.max())}")
    c4.metric("Độ phổ biến TB", f"{cleaned['popularity'].mean():.1f}")
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Số bài hát theo thập kỷ")
        st.caption("Sản xuất âm nhạc tăng vọt từ thập kỷ 2000 nhờ số hoá và streaming")
        if decade_col and not decade_stats.empty:
            ds = decade_stats.sort_values("decade")
            fig = px.bar(ds, x="decade", y="track_count",
                         color="avg_popularity", color_continuous_scale="Viridis",
                         labels={"decade": "Thập kỷ", "track_count": "Số bài"})
            fig.update_layout(height=400, xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Phân bố độ phổ biến")
        st.caption("Phần lớn bài hát có popularity thấp — chỉ số ít đạt viral")
        fig = px.histogram(cleaned, x="popularity", nbins=50,
                           color_discrete_sequence=["#1DB954"])
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Danceability vs Energy theo nhóm nhạc")
        st.caption("Mỗi màu là một nhóm nghe khác nhau — nhạc dance tập trung góc phải trên")
        sample = clustered.sample(min(3000, len(clustered)))
        fig = px.scatter(sample, x="danceability", y="energy",
                         color="cluster_name", opacity=0.5,
                         hover_data=["track_name", "artists"],
                         color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Cảm xúc bài hát vs Độ phổ biến")
        st.caption("Bài hát vui (valence cao) không hẳn phổ biến hơn — người nghe không phân biệt")
        sample = clustered.sample(min(3000, len(clustered)))
        fig = px.scatter(sample, x="valence", y="popularity",
                         color="cluster_name", opacity=0.5,
                         hover_data=["track_name", "artists"],
                         color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    # ── Correlation heatmap ───────────────────────────────────
    st.markdown("---")
    st.subheader("Tương quan giữa các đặc trưng âm thanh")
    st.caption("Màu xanh = tương quan thuận, màu đỏ = tương quan nghịch. Popularity tương quan mạnh nhất với feature nào?")
    feat_cols = [c for c in ["popularity", "danceability", "energy", "valence",
                              "acousticness", "instrumentalness", "liveness",
                              "loudness", "speechiness", "tempo"] if c in cleaned.columns]
    corr = cleaned[feat_cols].corr().round(2)
    fig = px.imshow(corr, text_auto=True, color_continuous_scale="RdBu_r",
                    zmin=-1, zmax=1, aspect="auto")
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    # Top correlations với popularity
    if "popularity" in corr.columns:
        pop_corr = corr["popularity"].drop("popularity").sort_values(key=abs, ascending=False)
        st.markdown("**Feature ảnh hưởng nhiều nhất đến popularity (theo tương quan):**")
        c1, c2, c3 = st.columns(3)
        for i, (feat, val) in enumerate(pop_corr.head(3).items()):
            arrow = "↑" if val > 0 else "↓"
            [c1, c2, c3][i].metric(feat, f"{val:+.3f}", delta=f"{arrow} tương quan {'thuận' if val > 0 else 'nghịch'}")


# ══════════════════════════════════════════════════════════════
# PAGE: Timeline Analysis
# ══════════════════════════════════════════════════════════════
elif page == "Timeline Analysis":
    st.title("Âm Nhạc Thay Đổi Như Thế Nào Qua 100 Năm?")
    st.markdown("*Phân tích xu hướng dựa trên đặc trưng âm thanh của 89K+ bài hát*")
    st.markdown("---")

    if decade_stats.empty or decade_col is None:
        st.warning("Chưa có dữ liệu theo thập kỷ. Chạy pipeline batch trước.")
        st.stop()

    ds = decade_stats.sort_values("decade").dropna(subset=["decade"])

    # ── Loudness War ─────────────────────────────────────────
    st.subheader("🔊 Nhạc hiện đại ngày càng to hơn")
    if "avg_loudness" in ds.columns and len(ds) >= 2:
        loud_old = ds.iloc[0]["avg_loudness"]
        loud_new = ds.iloc[-1]["avg_loudness"]
        diff_db  = loud_new - loud_old
        decade_old = ds.iloc[0]["decade"]
        decade_new = ds.iloc[-1]["decade"]
        st.info(f"**Xu hướng:** Nhạc thập kỷ {decade_new} to hơn {decade_old} khoảng **{abs(diff_db):.1f} dB** — "
                f"tương đương cảm giác to gấp ~{10**(abs(diff_db)/20):.1f} lần khi nghe. "
                f"Nguyên nhân: kỹ thuật mastering số hoá cho phép đẩy âm lượng tối đa hơn.")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ds["decade"], y=ds["avg_loudness"],
        mode="lines+markers", name="Mức độ to (dB)",
        line=dict(color="#FF4444", width=3), marker=dict(size=8),
        hovertemplate="Thập kỷ %{x}<br>Mức to: %{y:.1f} dB<extra></extra>",
    ))
    fig.update_layout(height=350, xaxis_title="Thập kỷ", yaxis_title="Mức độ to (dB, càng gần 0 càng to)",
                      hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    # ── Energy & Acoustic Retreat ─────────────────────────────
    st.subheader("⚡ Nhạc điện tử lấn át nhạc acoustic")
    if "avg_energy" in ds.columns and "avg_acousticness" in ds.columns and len(ds) >= 2:
        energy_old = ds.iloc[0]["avg_energy"]
        energy_new = ds.iloc[-1]["avg_energy"]
        acous_old  = ds.iloc[0]["avg_acousticness"]
        acous_new  = ds.iloc[-1]["avg_acousticness"]
        st.info(f"**Xu hướng:** Cường độ âm nhạc tăng **{(energy_new - energy_old)*100:.0f}%** "
                f"trong khi tỷ lệ nhạc cụ acoustic giảm **{(acous_old - acous_new)*100:.0f}%** — "
                f"phản ánh sự chuyển dịch từ guitar/piano sang synthesizer và beat điện tử.")
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ds["decade"], y=ds["avg_energy"],
        mode="lines+markers", name="Cường độ (Energy)",
        line=dict(color="#1DB954", width=3), marker=dict(size=8),
        hovertemplate="Thập kỷ %{x}<br>Cường độ: %{y:.2f}<extra></extra>",
    ))
    fig.add_trace(go.Scatter(
        x=ds["decade"], y=ds["avg_acousticness"],
        mode="lines+markers", name="Nhạc cụ acoustic",
        line=dict(color="#4ECDC4", width=3, dash="dash"), marker=dict(size=8),
        hovertemplate="Thập kỷ %{x}<br>Acoustic: %{y:.2f}<extra></extra>",
    ))
    fig.update_layout(height=350, xaxis_title="Thập kỷ", yaxis_title="Tỷ lệ [0 = thấp, 1 = cao]",
                      hovermode="x unified")
    st.plotly_chart(fig, use_container_width=True)

    # ── Danceability & Valence ────────────────────────────────
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("💃 Nhạc có dễ nhảy hơn không?")
        if "avg_danceability" in ds.columns:
            peak_dec = ds.loc[ds["avg_danceability"].idxmax(), "decade"]
            peak_val = ds["avg_danceability"].max()
            st.info(f"Dễ nhảy nhất: **{peak_dec}** ({peak_val:.2f}/1.0)")
        fig = px.bar(ds, x="decade", y="avg_danceability",
                     color="avg_danceability", color_continuous_scale="Oranges",
                     labels={"avg_danceability": "Mức độ dễ nhảy", "decade": "Thập kỷ"},
                     hover_data={"avg_danceability": ":.2f"})
        fig.update_layout(height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("😊 Nhạc vui hay buồn hơn theo thời gian?")
        if "avg_valence" in ds.columns and len(ds) >= 2:
            val_old = ds.iloc[0]["avg_valence"]
            val_new = ds.iloc[-1]["avg_valence"]
            trend = "vui hơn" if val_new > val_old else "ủ rũ hơn"
            st.info(f"Nhạc ngày nay **{trend}** so với 100 năm trước "
                    f"({val_old:.2f} → {val_new:.2f})")
        fig = px.bar(ds, x="decade", y="avg_valence",
                     color="avg_valence", color_continuous_scale="RdYlGn",
                     labels={"avg_valence": "Cảm xúc tích cực", "decade": "Thập kỷ"},
                     hover_data={"avg_valence": ":.2f"})
        fig.update_layout(height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Explicit content ──────────────────────────────────────
    st.subheader("🔞 Nội dung 18+ trong âm nhạc")
    if "explicit_pct" in ds.columns:
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=ds["decade"], y=ds["explicit_pct"],
            marker_color=[
                "#FF4444" if float(v) > 20 else "#FFA500" if float(v) > 10 else "#4CAF50"
                for v in ds["explicit_pct"].fillna(0)
            ],
            name="Explicit %",
        ))
        fig.update_layout(height=350, xaxis_title="Decade",
                          yaxis_title="Explicit Tracks (%)", xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Popularity trend ──────────────────────────────────────
    st.subheader("🌟 Bài hát cũ có được nghe nhiều không?")
    if "avg_popularity" in ds.columns and len(ds) >= 2:
        pop_new = ds.iloc[-1]["avg_popularity"]
        pop_old = ds.iloc[0]["avg_popularity"]
        st.info(f"**Xu hướng:** Bài hát mới phổ biến hơn bài cũ trên nền tảng streaming — "
                f"chênh lệch **{pop_new - pop_old:.0f} điểm** ({pop_old:.0f} → {pop_new:.0f}/100). "
                f"Lý do: thuật toán Spotify ưu tiên nhạc mới trong đề xuất.")
    fig = px.area(ds, x="decade", y="avg_popularity",
                  color_discrete_sequence=["#7C4DFF"],
                  labels={"avg_popularity": "Độ phổ biến TB", "decade": "Thập kỷ"},
                  hover_data={"avg_popularity": ":.1f"})
    fig.update_layout(height=320, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

    # ── Multi-feature radar per selected decades ──────────────
    st.subheader("🕸️ So sánh tổng thể đặc trưng âm nhạc giữa các thập kỷ")
    available_decades = sorted(ds["decade"].dropna().unique().tolist())
    selected = st.multiselect(
        "Chọn thập kỷ để so sánh",
        available_decades,
        default=available_decades[::3][:5] if len(available_decades) >= 3 else available_decades,
    )
    if selected:
        radar_features = ["avg_danceability", "avg_energy", "avg_valence",
                          "avg_acousticness", "avg_tempo"]
        radar_labels   = ["Danceability", "Energy", "Valence", "Acousticness", "Tempo (norm)"]
        fig = go.Figure()
        for dec in selected:
            row = ds[ds["decade"] == dec]
            if row.empty:
                continue
            r = row.iloc[0]
            vals = [
                float(r.get("avg_danceability", 0) or 0),
                float(r.get("avg_energy", 0) or 0),
                float(r.get("avg_valence", 0) or 0),
                float(r.get("avg_acousticness", 0) or 0),
                float(r.get("avg_tempo", 120) or 120) / 200,   # normalise tempo 0-200→0-1
            ]
            fig.add_trace(go.Scatterpolar(
                r=vals + [vals[0]], theta=radar_labels + [radar_labels[0]],
                fill="toself", name=dec,
            ))
        fig.update_layout(polar=dict(radialaxis=dict(range=[0, 1])), height=450)
        st.plotly_chart(fig, use_container_width=True)

    # ── Raw table ─────────────────────────────────────────────
    st.subheader("Bảng dữ liệu đầy đủ theo thập kỷ")
    st.dataframe(ds, use_container_width=True, height=400)


# ══════════════════════════════════════════════════════════════
# PAGE: Cluster Analysis
# ══════════════════════════════════════════════════════════════
elif page == "Cluster Analysis":
    st.title("Phân Nhóm Âm Nhạc — KMeans")
    st.markdown("*Thuật toán tự phân nhóm dựa trên đặc trưng âm thanh, không dùng nhãn genre có sẵn*")
    st.markdown("---")

    st.subheader("Tổng quan các nhóm nhạc")
    st.caption("Mỗi nhóm đại diện cho một phong cách nghe khác nhau được phát hiện tự động")
    st.dataframe(cluster_stats.sort_values("prediction") if "prediction" in cluster_stats.columns
                 else cluster_stats, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Tỷ lệ bài hát mỗi nhóm")
        st.caption("Nhóm nhạc nào được đại diện nhiều nhất trong dataset")
        counts = clustered["cluster_name"].value_counts().reset_index()
        counts.columns = ["cluster", "count"]
        fig = px.pie(counts, values="count", names="cluster",
                     color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        st.subheader("Đặc trưng âm thanh từng nhóm")
        st.caption("So sánh Danceability, Energy, Valence, Acousticness để thấy sự khác biệt rõ nhất")
        fig = go.Figure()
        colors = px.colors.qualitative.Set2
        for i, row in cluster_stats.iterrows():
            fig.add_trace(go.Bar(
                name=f"Cluster {int(row['prediction'])}",
                x=["Danceability", "Energy", "Valence", "Acousticness"],
                y=[row.get("avg_danceability", 0), row.get("avg_energy", 0),
                   row.get("avg_valence", 0),       row.get("avg_acousticness", 0)],
                marker_color=colors[i % len(colors)],
            ))
        fig.update_layout(barmode="group", height=400)
        st.plotly_chart(fig, use_container_width=True)

    # Cluster composition by decade
    if decade_col and decade_col in clustered.columns:
        st.subheader("Cluster Composition by Decade")
        comp = clustered.groupby(["cluster_name", decade_col]).size().reset_index(name="count")
        fig = px.bar(comp, x=decade_col, y="count", color="cluster_name",
                     barmode="stack",
                     color_discrete_sequence=px.colors.qualitative.Set2,
                     labels={decade_col: "Decade"})
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    # ── Boxplot so sánh phân phối giữa các cluster ───────────
    st.markdown("---")
    st.subheader("Phân phối đặc trưng âm thanh giữa các nhóm")
    st.caption("Boxplot cho thấy median, tứ phân vị và outlier — dùng để đánh giá độ tách biệt giữa các cluster")
    box_feat = st.selectbox(
        "Chọn đặc trưng để so sánh",
        [c for c in ["danceability", "energy", "valence", "acousticness",
                     "instrumentalness", "tempo", "loudness", "speechiness", "liveness"]
         if c in clustered.columns],
        key="box_feat"
    )
    fig = px.box(clustered, x="cluster_name", y=box_feat, color="cluster_name",
                 points=False,
                 color_discrete_sequence=px.colors.qualitative.Set2,
                 labels={"cluster_name": "Nhóm nhạc", box_feat: box_feat.capitalize()})
    fig.update_layout(height=420, showlegend=False, xaxis_tickangle=-20)
    st.plotly_chart(fig, use_container_width=True)

    # Thống kê định lượng
    stats = clustered.groupby("cluster_name")[box_feat].agg(["mean","median","std","min","max"]).round(3)
    stats.columns = ["Trung bình", "Trung vị", "Độ lệch chuẩn", "Min", "Max"]
    st.dataframe(stats, use_container_width=True)

    st.markdown("---")
    st.subheader("Khám phá bài hát theo nhóm")
    sel = st.selectbox("Chọn nhóm nhạc", sorted(clustered["cluster"].unique()))
    cols_show = [c for c in ["track_name", "artists", "year", "decade", "popularity",
                              "danceability", "energy", "valence", "loudness"]
                 if c in clustered.columns]
    st.dataframe(
        clustered[clustered["cluster"] == sel][cols_show]
        .sort_values("popularity", ascending=False).head(50),
        use_container_width=True,
    )


# ══════════════════════════════════════════════════════════════
# PAGE: Top Tracks
# ══════════════════════════════════════════════════════════════
elif page == "Top Tracks":
    st.title("Bảng Xếp Hạng Bài Hát")
    st.markdown("*Lọc đa chiều theo đặc trưng âm thanh — tìm bài hát phù hợp với profile nghe nhạc*")
    st.markdown("---")

    # ── Bộ lọc đa chiều ──────────────────────────────────────
    st.subheader("Bộ lọc đặc trưng âm thanh")
    df_filter = cleaned.copy()

    c1, c2 = st.columns(2)
    with c1:
        pop_range = st.slider("Độ phổ biến (Popularity)", 0, 100, (0, 100))
        dance_range = st.slider("Danceability", 0.0, 1.0, (0.0, 1.0), step=0.01)
        energy_range = st.slider("Energy", 0.0, 1.0, (0.0, 1.0), step=0.01)
    with c2:
        valence_range = st.slider("Valence (cảm xúc tích cực)", 0.0, 1.0, (0.0, 1.0), step=0.01)
        acoustic_range = st.slider("Acousticness", 0.0, 1.0, (0.0, 1.0), step=0.01)
        if "track_genre" in df_filter.columns:
            genres = ["Tất cả"] + sorted(df_filter["track_genre"].dropna().unique().tolist())
            genre_filter = st.selectbox("Genre", genres)
        else:
            genre_filter = "Tất cả"

    mask = (
        df_filter["popularity"].between(*pop_range) &
        df_filter["danceability"].between(*dance_range) &
        df_filter["energy"].between(*energy_range) &
        df_filter["valence"].between(*valence_range) &
        df_filter["acousticness"].between(*acoustic_range)
    )
    if genre_filter != "Tất cả" and "track_genre" in df_filter.columns:
        mask &= df_filter["track_genre"] == genre_filter

    df_filtered = df_filter[mask]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Kết quả tìm được", f"{len(df_filtered):,}")
    c2.metric("Popularity TB", f"{df_filtered['popularity'].mean():.1f}" if not df_filtered.empty else "—")
    c3.metric("Energy TB", f"{df_filtered['energy'].mean():.2f}" if not df_filtered.empty else "—")
    c4.metric("Danceability TB", f"{df_filtered['danceability'].mean():.2f}" if not df_filtered.empty else "—")

    st.markdown("---")

    n = st.slider("Số bài hiển thị", 10, 100, 20)
    show_cols = [c for c in ["track_name", "artists", "track_genre", "popularity",
                              "danceability", "energy", "valence", "acousticness"]
                 if c in df_filtered.columns]

    if df_filtered.empty:
        st.warning("Không có bài hát nào khớp với bộ lọc. Hãy nới rộng phạm vi.")
    else:
        top = df_filtered.nlargest(n, "popularity")[show_cols]
        st.subheader(f"Top {n} bài phổ biến nhất trong bộ lọc")
        st.dataframe(top, use_container_width=True, height=400)

        fig = px.scatter(top, x="danceability", y="energy",
                         size="popularity", color="popularity",
                         hover_data=["track_name", "artists"],
                         color_continuous_scale="Viridis",
                         labels={"danceability": "Danceability", "energy": "Energy"})
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════
# PAGE: Live Stream
# ══════════════════════════════════════════════════════════════
elif page == "Live Stream":
    st.title("Luồng Dữ Liệu Thời Gian Thực — Kafka")
    st.markdown("*Dữ liệu được stream qua Kafka → Spark Streaming → HDFS, cập nhật mỗi 20 giây*")
    st.markdown("---")

    load_streaming_data.clear()
    df_stream = load_streaming_data()

    st.caption(f"⏱ {time.strftime('%H:%M:%S')}  |  Tự động refresh sau 20s")

    if df_stream.empty:
        st.info("Chưa có streaming data. Chạy 03_kafka_producer.py + 03_spark_streaming.py trước.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Records ingested", f"{len(df_stream):,}")
        if "track_genre" in df_stream.columns:
            c2.metric("Genres", f"{df_stream['track_genre'].nunique()}")
        if "artists" in df_stream.columns:
            c3.metric("Artists", f"{df_stream['artists'].nunique():,}")
        c4.metric("Avg Popularity", f"{df_stream['popularity'].mean():.1f}"
                  if "popularity" in df_stream.columns else "—")

        st.markdown("---")

        c1, c2 = st.columns(2)
        with c1:
            st.subheader("Tracks per Genre (stream)")
            if "track_genre" in df_stream.columns:
                counts = df_stream["track_genre"].value_counts().head(15).reset_index()
                counts.columns = ["genre", "count"]
                fig = px.bar(counts, x="genre", y="count",
                             color="count", color_continuous_scale="Teal")
                fig.update_layout(height=380, xaxis_tickangle=-45)
                st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.subheader("Popularity Tier Distribution")
            if "popularity_tier" in df_stream.columns:
                tier_counts = df_stream["popularity_tier"].value_counts().reset_index()
                tier_counts.columns = ["tier", "count"]
                fig = px.pie(tier_counts, values="count", names="tier",
                             color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_layout(height=380)
                st.plotly_chart(fig, use_container_width=True)

        st.subheader("Latest 50 tracks ingested")
        show_cols = [c for c in ["track_name", "artists", "album_name",
                                  "track_genre", "popularity", "popularity_tier",
                                  "energy_level", "ingestion_time"]
                     if c in df_stream.columns]
        sort_col = "ingestion_time" if "ingestion_time" in df_stream.columns else None
        df_show = (df_stream[show_cols].sort_values(sort_col, ascending=False).head(50)
                   if sort_col else df_stream[show_cols].head(50))
        st.dataframe(df_show, use_container_width=True, height=380)

    time.sleep(20)
    st.rerun()
