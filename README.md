# PySpark Data Engineering Assignment

## Objective
The goal of this assignment is to help students practice data engineering concepts using PySpark. They will explore, clean, transform, and aggregate data, preparing it for analysis and visualization.

---

## Dataset
Students should use a sales dataset containing fields such as:
- **Customer ID**: Unique identifier for each customer.
- **Transaction Date**: Date of the transaction.
- **Product Category**: Category of the purchased product.
- **Sales Amount**: The total sales amount for a transaction.
- **Quantity Sold**: Number of items sold.

---

## Assignment Tasks

### 1. Data Loading and Exploration
1. Load the dataset into a PySpark DataFrame.
    - Question: Write PySpark code to load the dataset into a DataFrame and display the first 10 rows.

2. Inspect the dataset.
    - Question: What is the schema of the dataset? Write code to print the schema and explain the data types of each field.
    - Question: Count the total number of rows and columns in the dataset.

---

### 2. Data Cleaning
3. Handle missing values.
    - Question: Identify columns with missing values. Write code to count the number of null values in each column.
    - Question: Replace missing numerical values with 0 and missing categorical values with "Unknown."

4. Remove duplicates.
    - Question: Write code to remove duplicate rows from the dataset. How many duplicates were removed?

5. Standardize data types.
    - Question: Convert the "Transaction Date" column to a TimestampType. Write the code to achieve this.

6. Normalize text columns.
    - Question: Write code to trim whitespace and convert all text in the "Product Category" column to lowercase.

---

### 3. Feature Engineering
7. Extract date components.
    - Question: Extract year, month, and day from the "Transaction Date" column and add them as new columns.

8. Create customer segmentation.
    - Question: Categorize customers into "High", "Medium", and "Low" value groups based on their total purchase amount. Define thresholds and explain your logic.

9. Generate sales metrics.
    - Question: Calculate the profit margin for each transaction using an additional column for "Cost." Add a new column for "Profit Margin."

---

### 4. Data Aggregation
10. Aggregate sales data.
    - Question: Group the data by "Product Category" and calculate the total sales and average sales per category.

11. Pivot the data.
    - Question: Create a pivot table to show the total sales for each "Product Category" by year.

---

### 5. Data Enrichment
12. Join external datasets.
    - Question: If provided with a holiday dataset (date and holiday name), join it with the sales dataset to identify transactions made on holidays. Write code to perform this join.

---

### 6. Advanced Transformations
13. Apply window functions.
    - Question: Write a window function to rank customers based on their total purchase amount. Display the top 5 customers.

14. Calculate moving averages.
    - Question: Write code to calculate a 7-day moving average of total sales. Display the results for the first 10 rows of the moving average.

15. Conditional transformations.
    - Question: Categorize transactions into "High", "Medium", and "Low" sales categories based on the "Sales Amount" column. Add a new column for the category.

---

### 7. Data Quality Checks
16. Validate data integrity.
    - Question: Write code to detect and handle outliers in the "Sales Amount" column. Explain your approach.
    - Question: Ensure that all rows have a valid "Transaction Date" (e.g., no future dates).

---

### 8. Output and Optimization
17. Optimize the dataset.
    - Question: Write code to partition the data by year and save it in Parquet format.

18. Save final dataset.
    - Question: Save the cleaned and transformed dataset in a file format of your choice. Explain why you chose this format.

---

### 9. Reporting and Visualization
19. Prepare for reporting.
    - Question: Write code to calculate monthly total sales and save the results as a CSV file for visualization in tools like Tableau or Power BI.

---

### 10. Machine Learning Preparation (Optional)
20. Prepare features for ML.
    - Question: Normalize numerical fields and encode categorical columns using PySpark MLlib’s StringIndexer and OneHotEncoder.
    - Question: Split the dataset into training and test sets with a 70-30 split.

---

## Submission Instructions
- Submit your PySpark notebook (.ipynb or .py format) with detailed comments explaining your logic.
- Include outputs or screenshots for key tasks.
- Bonus points for creativity and additional insights derived from the dataset.

---

## Grading Rubric
1. **Completeness**: All tasks are attempted and logically completed.
2. **Code Quality**: Code is clean, readable, and well-documented.
3. **Correctness**: Results are accurate based on the questions.
4. **Creativity**: Additional insights or advanced transformations are implemented.
5. **Optimization**: Efficient use of PySpark transformations and actions.

