# app.py
from flask import Flask, render_template, request
from recommendation import ProductRecommendationSystem

app = Flask(__name__)

# Path to your user activity data
data_path = "C:/Users/mural/OneDrive/Desktop/SEM5/user_activity_data.txt"
recommendation_system = ProductRecommendationSystem(data_path)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/recommend', methods=['POST'])
def recommend():
    user_id = int(request.form['user_id'])
    recommendations = recommendation_system.get_recommendations(user_id)
    bought_products = recommendation_system.get_bought_products(user_id)
    all_products = recommendation_system.get_all_products()  # Get all products

    return render_template('recommendations.html', 
                           recommendations=recommendations, 
                           bought_products=bought_products,
                           all_products=all_products)  # Pass all products to the template


if __name__ == '__main__':
    app.run(debug=True)
