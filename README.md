# End-to-End Insurance Claims ETL Pipeline

Welcome! This repository documents my journey in building a complete, automated ETL (Extract, Transform, Load) pipeline for a raw insurance claims dataset. This project wasn't just about writing code; it was an exploration into the foundational engineering that powers actuarial analysis. My goal was to bridge the gap between messy, real-world data and the clean, structured information required for meaningful reserving, pricing, and underwriting analysis.

As someone passionate about the P&C insurance space, I'm driven by a curiosity to understand the full data lifecycle. I believe that the best insights come from not only building the model but also from deeply understanding the data's journey and the transformations it undergoes. This project is a tangible result of that belief.

## Project Goal

The primary objective of this pipeline is to take a raw, un-inspected dataset of car insurance claims and programmatically transform it into a clean, reliable, and analysis-ready table in a PostgreSQL database. The final output is designed to be directly usable by an actuarial analyst to answer questions about claim frequency, severity, and loss patterns.

## The Toolkit: Technology Choices

I chose a modern, robust, and scalable tech stack for this project, mirroring what's used in professional data engineering environments.

* **Language & Libraries:** **Python** was the natural choice for its powerful data manipulation libraries.
    * **Pandas:** For the core data extraction and transformation logic.
    * **SQLAlchemy:** To provide a robust, object-oriented bridge between our Python script and the database.
* **Database:** **PostgreSQL** was chosen over a simple CSV file to simulate a real-world production environment where data needs to be structured, queryable, and persistent.
* **Containerization:** **Docker** is used to containerize both the PostgreSQL database and the Python application. This ensures perfect reproducibility—the pipeline will run the same way on any machine.
* **Automation:** **GitHub Actions** automates the entire ETL run on a daily schedule, demonstrating a key DevOps practice for keeping data systems up-to-date.

## The Journey: A Transparent Walkthrough

Every dataset tells a story, and often, the first chapter is messy. Here’s a transparent look at the steps I took to build this pipeline.

### 1. Extract: The Starting Point

* **Data Source:** The project uses the **"Car Insurance Claim" dataset from Kaggle**. I chose this dataset because it's a realistic proxy for the kind of data an analyst might receive—containing over 10,000 records with a mix of data types and, importantly, imperfections.

### 2. Transform: Wrestling with the Data

This is where the real work happened. My approach was to treat the data as an unknown quantity and build a resilient script that could handle surprises.

* **Schema Standardization:** The first step was simple but crucial: standardizing all column names to a consistent `snake_case` format.
* **Handling Surprises (The 'age' Column):** My initial script failed because the `age` column, which I expected to be numeric, contained string values. This is a classic real-world data problem! I updated the script to be more robust by first coercing the column to a numeric type (turning any text into `NaN`) and then filling those `NaN` values with the median age. This ensures the pipeline doesn't break if the source data quality changes.
* **Imputing Missing Values:** I identified missing data in `credit_score` and `annual_mileage` and filled them with the median value of each column. I chose the median over the mean as it's less sensitive to outliers, which are common in financial data.
* **Feature Engineering:** Raw data is rarely enough. I created two new, more valuable features to support analysis:
    * `age_group`: Binning policyholders into clear age brackets (`16-25`, `26-40`, etc.).
    * `had_past_accidents`: A simple but powerful binary flag (1 or 0) derived from the `past_accidents` column.

### 3. Load: Creating a Reliable Destination

The final step was to load the clean, transformed DataFrame into our PostgreSQL database. The script is configured to replace the table on each run, ensuring the data is always fresh.

## ⚙️ How to Run This Project Locally

I've made this project easy for anyone to run and explore.

1.  **Prerequisites:**
    * Ensure you have [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running.
    * Clone this repository: `git clone https://github.com/YOUR_USERNAME/claims-etl-pipeline.git`
    * Navigate into the project directory: `cd claims-etl-pipeline`

2.  **Start the Database:**
    * Run the following command in your terminal to start the PostgreSQL database in a Docker container:
        ```bash
        docker run --name claims-db -e POSTGRES_PASSWORD=mysecretpassword -p 5432:5432 -d postgres
        ```

3.  **Run the ETL Pipeline:**
    * Set up and activate a Python virtual environment.
    * Install the required packages: `pip install -r requirements.txt`
    * Execute the script: `python etl.py`
    * You will see the log output in your terminal, and upon success, the `claims_data` table will be populated in your database.

## 🌱 Learning & Future Steps

This project was a fantastic learning experience in applying data engineering principles to an actuarial context. It reinforced the importance of writing defensive, robust code that doesn't assume perfect data.

I'm always looking for ways to improve. Some future enhancements I'm considering are:

* **Data Validation:** Integrating a library like `Great Expectations` to automatically test the data quality at each step of the pipeline.
* **Advanced Orchestration:** Moving from a simple cron schedule in GitHub Actions to a more sophisticated orchestrator like Airflow or Prefect.
* **Building a Dashboard:** Connecting a BI tool like Tableau or Power BI to the final PostgreSQL table to create an interactive loss-ratio dashboard.

Thank you for exploring my project. I'm always open to feedback and new ideas!
