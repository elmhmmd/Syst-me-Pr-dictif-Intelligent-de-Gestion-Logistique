import streamlit as st
from pyspark.sql import SparkSession
from pyspark.ml.tuning import CrossValidatorModel
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType
import os


st.set_page_config(page_title="Prédiction Risque Livraison", page_icon="🚚", layout="centered")


@st.cache_resource
def get_spark_session():
    """Crée ou récupère une session Spark locale."""
    return SparkSession.builder \
        .appName("Streamlit_Logistics_App_Simple") \
        .master("local[*]") \
        .config("spark.ui.showConsoleProgress", "false") \
        .getOrCreate()

@st.cache_resource
def load_trained_model():
    """Charge le modèle CrossValidator sauvegardé."""
    if os.path.exists("delivery_risk_model"):
        # Charge le modèle complet (PipelineModel encapsulé dans CrossValidatorModel)
        return CrossValidatorModel.load("delivery_risk_model")
    else:
        return None

spark = get_spark_session()
model = load_trained_model()


SHIPPING_MODE_MAP = {
    "Standard Class": 0.0,
    "First Class": 1.0,
    "Second Class": 2.0,
    "Same Day": 3.0
}


st.title("🚚 Prédiction de Retard de Livraison")
st.markdown("Entrez les paramètres de la commande pour estimer le risque de retard.")

if not model:
    st.error("⚠️ Modèle introuvable ! Assurez-vous que le dossier `delivery_risk_model` est présent au même niveau que ce script.")
    st.stop()

with st.form("prediction_form"):
    st.subheader("Paramètres de la commande")
    
    col1, col2 = st.columns(2)
    
    with col1:
        days_scheduled = st.number_input(
            "Jours prévus (Scheduled)", 
            min_value=0, max_value=60, value=4,
            help="Nombre de jours prévus pour l'expédition."
        )
        
        category_id = st.number_input(
            "ID Catégorie Produit", 
            min_value=1, max_value=100, value=73,
            help="Identifiant numérique de la catégorie du produit."
        )
        
        quantity = st.number_input(
            "Quantité commandée", 
            min_value=1, max_value=1000, value=1
        )

    with col2:
        distance = st.number_input(
            "Distance (km)", 
            min_value=0.0, value=250.0, step=10.0,
            help="Distance estimée entre l'entrepôt et le client."
        )
        
        shipping_mode_label = st.selectbox(
            "Mode d'expédition", 
            options=list(SHIPPING_MODE_MAP.keys()),
            index=0
        )

    submitted = st.form_submit_button("🚀 Analyser le Risque")


if submitted:
    with st.spinner("Analyse en cours avec Spark ML..."):
        try:
            shipping_index = SHIPPING_MODE_MAP[shipping_mode_label]
            
            input_data = [(
                int(days_scheduled),
                int(category_id),
                int(quantity),
                float(shipping_index),
                float(distance)
            )]
            
            schema = StructType([
                StructField("Days for shipment (scheduled)", IntegerType(), True),
                StructField("Category Id", IntegerType(), True),
                StructField("Order Item Quantity", IntegerType(), True),
                StructField("ShippingMode_index", DoubleType(), True),
                StructField("distance", DoubleType(), True)
            ])
            
            input_df = spark.createDataFrame(input_data, schema)
            
            predictions = model.transform(input_df)
            
            result = predictions.select("prediction", "probability").collect()[0]
            pred_label = result["prediction"]
            probs = result["probability"]
            
            st.markdown("---")
            
            col_res1, col_res2 = st.columns([1, 2])
            
            with col_res1:
                if pred_label == 1.0:
                    st.error("### 🚨 RETARD PRÉVU")
                    st.metric("Niveau de confiance", f"{probs[1]*100:.1f}%")
                else:
                    st.success("### ✅ À L'HEURE")
                    st.metric("Niveau de confiance", f"{probs[0]*100:.1f}%")
            
            with col_res2:
                st.info("Détails de la probabilité :")
                st.progress(int(probs[1]*100))
                st.caption(f"Risque de retard calculé : {probs[1]:.4f}")

        except Exception as e:
            st.error(f"Une erreur est survenue lors de la prédiction : {str(e)}")