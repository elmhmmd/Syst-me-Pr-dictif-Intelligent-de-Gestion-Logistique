from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier

# 1. On définit l'indexeur (il fait partie du pipeline maintenant)
indexer = StringIndexer(
    inputCol="Shipping Mode", 
    outputCol="ShippingMode_index", 
    handleInvalid="keep" # Important pour éviter les erreurs si une nouvelle catégorie apparait
)

# 2. On définit les features (Notez que l'assembler prend la sortie de l'indexer)
feature_cols = [
    'Days for shipment (scheduled)', 
    'Category Id',                              
    'Order Item Quantity',                             
    'ShippingMode_index',  # C'est la colonne créée par l'indexer ci-dessus                
    'distance'
]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

# 3. Le classifieur
rf = RandomForestClassifier(labelCol="Late_delivery_risk", featuresCol="features")

# 4. CRUCIAL : On met TOUT dans le pipeline (Indexer -> Assembler -> Model)
pipeline = Pipeline(stages=[indexer, assembler, rf])

# 5. On divise les données (Assurez-vous que df contient la colonne brute "Shipping Mode")
train_data, test_data = df.randomSplit([0.8, 0.2], seed=42)

# 6. CrossValidator (identique à avant)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

paramGrid = ParamGridBuilder() \
    .addGrid(rf.numTrees, [20, 50]) \
    .addGrid(rf.maxDepth, [5, 10]) \
    .build()

cv = CrossValidator(
    estimator=pipeline, # On passe le pipeline complet ici
    estimatorParamMaps=paramGrid,
    evaluator=BinaryClassificationEvaluator(labelCol="Late_delivery_risk"),
    numFolds=3
)

# 7. Entraînement
print("Réentraînement du modèle avec le Pipeline complet...")
cv_model = cv.fit(train_data)

# 8. Sauvegarde du meilleur modèle
best_model = cv_model.bestModel
best_model.write().overwrite().save("best_delivery_risk_model")

print("✅ Modèle corrigé et sauvegardé !")