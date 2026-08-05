import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
from datetime import datetime

# Page Config
st.set_page_config(
    page_title="Pandémie & Mobilité Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === FOND ANIMÉ DYNAMIQUE (CSS) ===
st.markdown("""
    <style>
    body {
        background: linear-gradient(-45deg, #1f1c2c, #928dab, #0f2027, #203a43);
        background-size: 400% 400%;
        animation: gradient 15s ease infinite;
        color: white;
    }

    @keyframes gradient {
        0% {background-position: 0% 50%;}
        50% {background-position: 100% 50%;}
        100% {background-position: 0% 50%;}
    }

    .stApp {
        background: transparent;
    }

    .block-container {
        padding: 2rem;
        background-color: rgba(0, 0, 0, 0.55);
        border-radius: 15px;
    }

    h1, h2, h3, h4 {
        color: #00f2fe;
        text-shadow: 1px 1px 2px black;
    }
    </style>
""", unsafe_allow_html=True)

# Connexion PostgreSQL
def connect_to_postgres():
    try:
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="bigdata",
            user="spark_user",
            password="SecurePass123!"
        )
        return conn
    except Exception as e:
        st.error(f"Connexion PostgreSQL échouée : {e}")
        st.stop()

# Chargement des données
def fetch_data(conn):
    try:
        query = "SELECT * FROM predictions;"
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Erreur lors de la récupération des données : {e}")
        return pd.DataFrame()

# Connexion et chargement
conn = connect_to_postgres()
df = fetch_data(conn)
conn.close()

if df.empty:
    st.warning("Aucune donnée à afficher.")
else:
    df["date"] = pd.to_datetime(df["date"])
    df["created_at"] = pd.to_datetime(df["created_at"])

    st.title("Dashboard Pandémie & Mobilité")
    st.markdown("## Visualisation des prédictions issues du modèle ML basé sur la mobilité")

    # KPIs rapides
    total_rows = len(df)
    last_update = df["created_at"].max()
    countries = df["country"].nunique()

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Prédictions", f"{total_rows}")
    col2.metric("Pays couverts", f"{countries}")
    col3.metric("Dernière mise à jour", f"{last_update.strftime('%Y-%m-%d %H:%M')}")

    st.markdown("---")

    # Sélection de pays
    selected_country = st.sidebar.selectbox("Pays à explorer", sorted(df["country"].unique()))
    df_country = df[df["country"] == selected_country]

    st.subheader(f"Mobilité résidentielle - {selected_country}")
    fig = px.line(df_country, x="date", y="residential", title=f"Évolution de la mobilité résidentielle ({selected_country})", color_discrete_sequence=["#00f2fe"])
    st.plotly_chart(fig, use_container_width=True)

    # Sélection de dates
    st.sidebar.markdown("Filtrage temporel")
    min_date = df_country["date"].min().date()
    max_date = df_country["date"].max().date()
    date_range = st.sidebar.date_input("Sélectionnez une plage de dates", (min_date, max_date), min_value=min_date, max_value=max_date)

    if len(date_range) == 2:
        start_date, end_date = date_range
        df_filtered = df_country[(df_country["date"] >= pd.to_datetime(start_date)) & (df_country["date"] <= pd.to_datetime(end_date))]

        st.subheader(f"Détail de la période {start_date} ➡️ {end_date}")
        fig = px.area(df_filtered, x="date", y="residential", color="model", title="Comparaison des prédictions", color_discrete_sequence=px.colors.qualitative.Dark24)
        st.plotly_chart(fig, use_container_width=True)

    # Filtrage par modèle
    st.sidebar.markdown("Modèle de prédiction")
    selected_model = st.sidebar.selectbox("Choisissez le modèle", df["model"].unique())
    df_model = df[df["model"] == selected_model]

    st.subheader(f"Distribution - {selected_model}")
    fig = px.histogram(df_model, x="residential", nbins=30, title="Distribution des scores de mobilité résidentielle", color_discrete_sequence=["#f77062"])
    st.plotly_chart(fig, use_container_width=True)

    # Seuil interactif
    st.sidebar.markdown("Seuil de mobilité")
    threshold = st.sidebar.slider("Valeur minimale", float(df["residential"].min()), float(df["residential"].max()), step=0.5)
    high_mobility = df[df["residential"] >= threshold]

    st.subheader(f"Pays avec mobilité résidentielle > {threshold}")
    fig = px.box(high_mobility, x="country", y="residential", color="model", points="all", color_discrete_sequence=px.colors.sequential.Magma)
    st.plotly_chart(fig, use_container_width=True)

    # Données brutes en option
    with st.expander("Données brutes (preview)"):
        st.dataframe(df_model.head(50))