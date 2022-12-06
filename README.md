# ETL-Pipeline-using-Airflow
This project aimed to automate the extraction, transformation and loading (ETL) of data from a Postgres database to an AWS Redshift data warehouse. The process was automated using Apache Airflow.

Airflow was used to define, schedule and monitor ETL jobs. This enabled us to set up a pipeline that could run on a regular schedule, allowing us to extract data from the Postgres database, transform it as required and then load it into the AWS Redshift data warehouse.

The transformation step of the ETL process was completed using SQL queries, which allowed us to modify the data to suit the requirements of the data warehouse. We also used Airflow's UI to set up alerts and notifications that would trigger in the event of any errors or failures in the pipeline.

With the pipeline set up, we were able to have the data extracted, transformed and loaded into the data warehouse on a regular basis, ensuring that the data was up to date and accurate. This process allowed us to save time and money by streamlining the data pipeline and automating the ETL process.

Overall, this project was a success, as the data was successfully extracted from the Postgres database, transformed, and loaded into AWS Redshift.
