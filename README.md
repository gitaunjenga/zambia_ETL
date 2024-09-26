# ART ETL Pipeline

### Prerequisites
Ensure you have met the following requirements:

Python 3.7 or higher
Apache Airflow

### Installation
Clone the repository:
cd your-repo

Create a virtual environment (optional but recommended):

python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

Install the required packages:

pip install -r requirements.txt
Setup
Configure Airflow:

Initialize the Airflow database:

airflow db migrate

### Running the Project
Start the Airflow web server:

In one terminal window, start the web server:

airflow webserver --port 8080
Start the Airflow scheduler:

In another terminal window, start the scheduler:

airflow scheduler
Access the Airflow UI:

Open your web browser and go to http://localhost:8080. You should see the Airflow dashboard where you can monitor your DAGs and tasks.

Usage
ETL scripts will be the etl_pipeline directory.
All DAGs goes in the dags directory.
Customize the YAML configuration files as needed.
Trigger your DAGs from the Airflow UI.
