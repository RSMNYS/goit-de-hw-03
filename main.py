from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc, round

spark = SparkSession.builder.appName('HW3').getOrCreate()

products_df = spark.read.csv('./products.csv', header=True)
purchases_df = spark.read.csv('./purchases.csv', header=True)
users_df = spark.read.csv('./users.csv', header=True)

# 1. Show dataframes
print("Users DataFrame:")
users_df.show(5)

print("Products DataFrame:")
products_df.show(5)

print("Purchases DataFrame:")
purchases_df.show(5)

# 2. Clean data by removing rows with missing values
users_df = users_df.na.drop()
products_df = products_df.na.drop()
purchases_df = purchases_df.na.drop()

print("\nAfter removing rows with missing values:")
print(f"Users count: {users_df.count()}")
print(f"Products count: {products_df.count()}")
print(f"Purchases count: {purchases_df.count()}")

# 3. Total purchases by product category
purchases_with_products = purchases_df.join(products_df, "product_id")

purchases_with_amount = purchases_with_products.withColumn(
    "total_amount", col("quantity") * col("price")
)

total_by_category = purchases_with_amount.groupBy("category") \
    .agg(round(sum("total_amount"), 2).alias("total_spent")) \
    .orderBy(desc("total_spent"))

print("\n3. Total purchases by product category:")
total_by_category.show()

# 4. Total purchases by product category for age 18-25
purchases_with_users_products = purchases_df.join(users_df, "user_id") \
    .join(products_df, "product_id")

purchases_with_amount_users = purchases_with_users_products.withColumn(
    "total_amount", col("quantity") * col("price")
)

young_users_spending = purchases_with_amount_users.filter((col("age") >= 18) & (col("age") <= 25)) \
    .groupBy("category") \
    .agg(round(sum("total_amount"), 2).alias("total_spent_young_users")) \
    .orderBy(desc("total_spent_young_users"))

print("\n4. Total purchases by product category for ages 18-25:")
young_users_spending.show()

# 5. Percentage of purchases by category for ages 18-25
total_young_spending = young_users_spending.agg(sum("total_spent_young_users").alias("total_young_spending")).collect()[0][0]

young_spending_percentage = young_users_spending.withColumn(
    "percentage", round((col("total_spent_young_users") / total_young_spending) * 100, 2)
)

print("\n5. Percentage of purchases by category for ages 18-25:")
young_spending_percentage.orderBy(desc("percentage")).show()

# 6. Top 3 categories with highest percentage of spending by young users
top_categories = young_spending_percentage.orderBy(desc("percentage")).limit(3)

print("\n6. Top 3 categories with highest percentage of spending by young users (18-25):")
top_categories.show()

# Close Spark session
spark.stop()
