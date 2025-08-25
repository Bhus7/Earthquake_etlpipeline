  📘 User Manual for Earthquake ETL Pipeline Project
  
  1. Introduction

This project is an ETL pipeline (Extract, Transform, Load) that works with PySpark, PostgreSQL, and Apache Superset to study earthquake data from 2024–2025.
Extract → Gets earthquake data from the USGS website in CSV format.
Transform → Cleans the raw data and organizes it into useful columns like date, place, depth, magnitude, and country.
Load → Saves the cleaned data into a PostgreSQL database.
Visualize → Uses Apache Superset to build charts and dashboards for earthquake analysis.

  2. Prerequisites

You need to install:

Python (best with Anaconda)

Git (to download the project)

PostgreSQL (pgAdmin is optional for managing it)

Apache Superset (for charts and dashboards)

PySpark → install with pip install pyspark

psycopg2 → install with pip install psycopg2-binary


3. Download the Project

       git clone https://github.com/najibthapa1/CovidData_ETL_Pipeline.git
       cd Covid_ETL_Pipeline
   
4. Create a virtual environment (recommended)

python -m venv venv

source venv/bin/activate   # macOS/Linux users

venv\Scripts\activate      # Windows users

5. Install required Libraries

        pyspark

        psycopg2-binary

6. PostgreSQL Setup

Installing postgresql

    brew install postgresql       #for macOS 
    sudo apt install postgresql   #for Ubuntu


Setting user and password

brew services start postgresql (mac) / sudo -i -u postgres (ubuntu)

psql

alter user postgres with password 'postgres'

ALTER USER

\q

exit


Updating pg_hba.conf file

    sudo nano /etc/postgresql/16/main/pg_hba.conf

Scroll down and change the 'peer' keyword to 'md5' in the connection part

Restart the PostgreSQL service

Setting up pgAdmin


Open pgAdmin

  Go to Servers, Right Click -> Register -> Service

Fill out the fields:

  hostname: localhost

  port: 5432

  username: postgres

  password: postgres

7. Run the ETL Pipeline

Step 1: Extract

    python extract/extract.py /home/...(full path to the directory you want to save the extracted files)

    
Step 2: Transform

    python transform/transform.py /(extracted files full path)  /(full path to the directory you want to save the transformed files)
    
Step 3: Load

    python load/load.py /(transformed files full path) db_username db_password

    
After loading success, the data can be viewed from the pgAdmin -> localhost -> Tables

8. Visualize with Superset (Optional)

Make a new folder named superset and move into it using cd.

    mkdir superset && cd superset

Make a new virtual environment and install Apache Superset

    python3.11 -m venv venv
    source venv/bin/activate
    pip install apache_superset

Add parameters required by Apache Superset

     nano ~/.bashrc or nano ~/.zshrc

Add the following parameters:

    export SUPERSET_SECRET_KEY=YOUR-SECRET-KEY

    export FLASK_APP=superset

Save it & 

    source ~/.bashrc or source ~/.zshrc

Installing required libraries

    superset db upgrade
    superset fab create-admin
    superset init

Running the superset server in development mode in port 8088

    superset run -p 8088 --wuth-threads-reload --debugger


Go to webbrowser and type localhost:8088.

Enter the username and password set earlier

Connect our local postgre server to superset

Click the + icon > Data > Connect database

Select postgres, enter your credentials and press connect

Create a dataset out of daily_country (database - PostgreSQL, Schema - public, Table - daily_country)

Create charts and dashboard to save the created charts.



