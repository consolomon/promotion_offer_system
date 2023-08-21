# Promotions offer system for restaurant subscribers

### Project goals
Build a stream processing system that allows business to test a new restaurant subscription that gives subscribers exclusive promotions on restaurant meals.

### System requirements
1. Read data from Kafka in real time by using Spark Streaming and Python.
2. Get subscriber list from the Postgres database. 
3. Join data from Kafka with data from the database.
5. Send an output message to Kafka with information about promotion, user and favorites list.
6. Insert record into the database to get feedback from the user.

### Used tools and technologies
 - Apache Kafka
 - Apache Spark Streaming
 - PySpark
 - Python
 - PostgreSQL

### Repository structure
- src/scripts/ddl_postgres_tables.sql -> SQL script that creates database tables
- src/scripts/adv_campaign_app.py -> PySpark script that performs data stream processing  
