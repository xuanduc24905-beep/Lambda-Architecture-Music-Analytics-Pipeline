import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pyarrow.parquet as pq
import pyarrow as pa

st.set_page_config(
    page_title="Spotify Big Data Dashboard",
    page_icon="🎵",
    layout="wide",
    initial_sidebar_state="expanded"
)

@st.cache_data
def load_data():
    try:
        cleaned = pd.read_parquet("/data/cleaned.parquet")
        clustered = pd.read_parquet("/data/clustered.parquet")
        genre_stats = pd.read_parquet("/data/genre_stats.parquet")
        cluster_stats = pd.read_parquet("/data/cluster_stats.parquet")

        numeric_cols = ["popularity", "danceability", "energy", "valence",
                        "acousticness", "instrumentalness", "liveness",
                        "loudness", "speechiness", "tempo"]
        for col in numeric_cols:
            if col in cleaned.columns:
                cleaned[col] = pd.to_numeric(cleaned[col], errors="coerce")
            if col in clustered.columns:
                clustered[col] = pd.to_numeric(clustered[col], errors="coerce")

        for col in ["avg_popularity", "avg_danceability", "avg_energy",
                    "avg_valence", "track_count"]:
            if col in genre_stats.columns:
                genre_stats[col] = pd.to_numeric(genre_stats[col], errors="coerce")
            if col in cluster_stats.columns:
                cluster_stats[col] = pd.to_numeric(cluster_stats[col], errors="coerce")

        return cleaned, clustered, genre_stats, cluster_stats
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()

with st.spinner("Loading data..."):
    cleaned, clustered, genre_stats, cluster_stats = load_data()

cluster_labels = {
    0: "High Energy", 1: "Acoustic & Calm",
    2: "Dance & Party", 3: "Dark & Melancholic",
    4: "Happy & Upbeat", 5: "Instrumental",
    6: "Vocal & Speech", 7: "Chill & Lo-fi",
}
clustered["cluster_name"] = clustered["cluster"].map(
    lambda x: cluster_labels.get(x, f"Cluster {x}")
)

st.sidebar.title("Spotify Analytics")
st.sidebar.markdown("---")
page = st.sidebar.radio("Navigation", [
    "Overview", "Genre Analysis", "Cluster Analysis", "Top Tracks",
])
st.sidebar.markdown("---")
st.sidebar.metric("Total Tracks", f"{len(cleaned):,}")
st.sidebar.metric("Total Genres", f"{cleaned['track_genre'].nunique()}")
st.sidebar.metric("Total Artists", f"{cleaned['artists'].nunique():,}")

if page == "Overview":
    st.title("Spotify Tracks — Big Data Dashboard")
    st.markdown("*114K tracks · Spark + HDFS + Hive + KMeans Clustering*")
    st.markdown("---")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Tracks", f"{len(cleaned):,}")
    c2.metric("Genres", f"{cleaned['track_genre'].nunique()}")
    c3.metric("Artists", f"{cleaned['artists'].nunique():,}")
    c4.metric("Avg Popularity", f"{cleaned['popularity'].mean():.1f}")
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top 15 Genres by Popularity")
        top = genre_stats.nlargest(15, "avg_popularity")
        fig = px.bar(top, x="avg_popularity", y="track_genre",
                     orientation="h", color="avg_popularity",
                     color_continuous_scale="Viridis")
        fig.update_layout(height=450, showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Popularity Distribution")
        fig = px.histogram(cleaned, x="popularity", nbins=50,
                           color_discrete_sequence=["#1DB954"])
        fig.update_layout(height=450)
        st.plotly_chart(fig, use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Danceability vs Energy by Cluster")
        sample = clustered.sample(min(3000, len(clustered)))
        fig = px.scatter(sample, x="danceability", y="energy",
                         color="cluster_name", opacity=0.6,
                         hover_data=["track_name", "artists"],
                         color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Valence vs Popularity by Cluster")
        sample = clustered.sample(min(3000, len(clustered)))
        fig = px.scatter(sample, x="valence", y="popularity",
                         color="cluster_name", opacity=0.6,
                         hover_data=["track_name", "artists"],
                         color_discrete_sequence=px.colors.qualitative.Set2)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

elif page == "Genre Analysis":
    st.title("Genre Analysis")
    st.markdown("---")

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Top 20 Genres by Track Count")
        top20 = genre_stats.nlargest(20, "track_count")
        fig = px.bar(top20, x="track_genre", y="track_count",
                     color="avg_popularity", color_continuous_scale="Blues")
        fig.update_layout(height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Audio Features — Top 10 Genres")
        top10 = genre_stats.nlargest(10, "avg_popularity")
        fig = go.Figure()
        for feat, color in zip(
            ["avg_danceability", "avg_energy", "avg_valence"],
            ["#1DB954", "#FF6B6B", "#4ECDC4"]
        ):
            fig.add_trace(go.Bar(
                name=feat.replace("avg_", "").title(),
                x=top10["track_genre"], y=top10[feat],
                marker_color=color
            ))
        fig.update_layout(barmode="group", height=400, xaxis_tickangle=-45)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Full Genre Stats")
    st.dataframe(genre_stats.sort_values("avg_popularity", ascending=False),
                 use_container_width=True, height=400)

elif page == "Cluster Analysis":
    st.title("KMeans Cluster Analysis")
    st.markdown("---")

    st.subheader("Cluster Summary")
    st.dataframe(cluster_stats.sort_values("prediction"), use_container_width=True)

    c1, c2 = st.columns(2)
    with c1:
        st.subheader("Cluster Size Distribution")
        counts = clustered["cluster_name"].value_counts().reset_index()
        counts.columns = ["cluster", "count"]
        fig = px.pie(counts, values="count", names="cluster",
                     color_discrete_sequence=px.colors.qualitative.Set3)
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        st.subheader("Audio Profile per Cluster")
        fig = go.Figure()
        colors = px.colors.qualitative.Set2
        for i, row in cluster_stats.iterrows():
            fig.add_trace(go.Bar(
                name=f"Cluster {int(row['prediction'])}",
                x=["Danceability", "Energy", "Valence"],
                y=[row["avg_danceability"], row["avg_energy"], row["avg_valence"]],
                marker_color=colors[i % len(colors)]
            ))
        fig.update_layout(barmode="group", height=400)
        st.plotly_chart(fig, use_container_width=True)

    st.subheader("Browse Tracks by Cluster")
    sel = st.selectbox("Select Cluster", sorted(clustered["cluster"].unique()))
    st.dataframe(
        clustered[clustered["cluster"] == sel]
        [["track_name", "artists", "track_genre", "popularity",
          "danceability", "energy", "valence"]]
        .sort_values("popularity", ascending=False).head(50),
        use_container_width=True
    )

elif page == "Top Tracks":
    st.title("Top Tracks")
    st.markdown("---")

    c1, c2 = st.columns([1, 2])
    with c1:
        genre_filter = st.selectbox("Filter by Genre",
            ["All"] + sorted(clustered["track_genre"].unique().tolist()))
        n = st.slider("Number of tracks", 10, 100, 20)

    df = clustered.copy()
    if genre_filter != "All":
        df = df[df["track_genre"] == genre_filter]

    top = df.nlargest(n, "popularity")[
        ["track_name", "artists", "track_genre", "popularity",
         "cluster_name", "danceability", "energy", "valence"]
    ]

    st.subheader(f"Top {n} Tracks")
    st.dataframe(top, use_container_width=True, height=400)

    fig = px.bar(top, x="track_name", y="popularity",
                 color="cluster_name", hover_data=["artists"],
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=400, xaxis_tickangle=-45, xaxis_title="")
    st.plotly_chart(fig, use_container_width=True)
