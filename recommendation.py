# recommendation.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, explode

class ProductRecommendationSystem:
    def __init__(self, data_path):
        self.data_path = data_path
        self.spark = SparkSession.builder \
            .appName("Product Recommendation System") \
            .getOrCreate()

        self.df = self.load_data()
        self.interaction_df = self.prepare_interaction_data()
        self.model = self.train_model()

    def load_data(self):
        df = self.spark.read.json(self.data_path)
        df = df.withColumn("user_id", col("user_id").cast("integer")) \
               .withColumn("product_id", col("product_id").cast("integer"))
        
        rating_map = {
            'view': 1,
            'click': 3,
            'purchase': 5
        }

        df = df.withColumn("rating", when(col("event_type") == "view", rating_map['view'])
                           .when(col("event_type") == "click", rating_map['click'])
                           .when(col("event_type") == "purchase", rating_map['purchase']))

        return df

    def prepare_interaction_data(self):
        return self.df.select("user_id", "product_id", "rating")

    def train_model(self):
        from pyspark.ml.recommendation import ALS
        als = ALS(userCol="user_id", itemCol="product_id", ratingCol="rating", coldStartStrategy="drop")
        return als.fit(self.interaction_df)

    def get_recommendations(self, user_id):
        user_recommendations = self.model.recommendForAllUsers(5)  # Get top 5 recommendations
        products_df = self.df.select("product_id", "product_name").dropDuplicates()

        recommendations_exploded = user_recommendations.withColumn("recommendations", explode("recommendations"))
        final_recommendations = recommendations_exploded.select(
            "user_id",
            col("recommendations.product_id").alias("product_id"),
            col("recommendations.rating").alias("estimated_score")
        ).join(products_df, "product_id")
        

        # Filter recommendations for the given user
        return final_recommendations.filter(final_recommendations.user_id == user_id).collect()
    

    
    def get_bought_products(self, user_id):
        return self.df.filter((self.df.user_id == user_id) & (self.df.event_type == 'purchase')).select("product_id", "product_name").distinct().collect()

    def get_all_products(self):
        return self.df.select("product_id", "product_name").distinct().collect()