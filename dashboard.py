import streamlit as st
import pymongo
import pandas as pd
import time
import altair as alt

# Connexion Mongo
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["logistics_db"]
collection = db["aggregated_stats"]

st.set_page_config(page_title="Live Logistics Dashboard", layout="wide")
st.title("📊 Dashboard Logistique Temps Réel")

placeholder = st.empty()

while True:
    # Récupérer les dernières données
    data = list(collection.find().sort("batch_time", -1).limit(100))
    
    if data:
        df = pd.DataFrame(data)
        
        with placeholder.container():
            # KPIs
            total_orders = df['order_count'].sum()
            avg_risk_global = df['avg_risk'].mean()
            
            kpi1, kpi2, kpi3 = st.columns(3)
            kpi1.metric("Commandes Analysées (Fenêtre)", int(total_orders))
            kpi2.metric("Risque Moyen Global", f"{avg_risk_global:.2%}")
            kpi3.metric("Dernière mise à jour", pd.to_datetime(df['batch_time']).max().strftime('%H:%M:%S'))

            col1, col2 = st.columns(2)

            with col1:
                st.subheader("Risque par Mode d'Expédition")
                chart_risk = alt.Chart(df).mark_bar().encode(
                    x='Shipping Mode',
                    y='avg_risk',
                    color='Shipping Mode',
                    tooltip=['Shipping Mode', 'avg_risk', 'order_count']
                ).properties(height=300)
                st.altair_chart(chart_risk, use_container_width=True)

            with col2:
                st.subheader("Volume de Commandes vs Distance Moyenne")
                chart_dist = alt.Chart(df).mark_circle(size=60).encode(
                    x='avg_distance',
                    y='order_count',
                    color='Shipping Mode',
                    tooltip=['Shipping Mode', 'avg_distance', 'order_count']
                ).properties(height=300)
                st.altair_chart(chart_dist, use_container_width=True)

            st.subheader("Données Brutes (Agrégées)")
            st.dataframe(df.drop(columns=['_id']).head(10))
    else:
        placeholder.warning("En attente de données dans MongoDB...")

    time.sleep(5)