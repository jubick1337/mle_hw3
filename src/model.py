from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler


class KMeansModel:
    def __init__(self, k):
        self.vector_assembler = VectorAssembler(
            inputCols=[
                'calcium_100g', 'fat_100g', 'proteins_100g', 'energy_100g'  # Only most popular columns are left
            ],
            outputCol="features", handleInvalid='skip')

        self.k_means = KMeans(k=k, featuresCol='features', predictionCol='cluster')

    def fit(self, data):
        features = self.vector_assembler.transform(data)
        model = self.k_means.fit(features)
        return model.summary.predictions[['calcium_100g', 'fat_100g', 'proteins_100g', 'energy_100g', "cluster"]]
