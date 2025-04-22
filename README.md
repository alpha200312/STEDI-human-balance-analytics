In this project, we build a data lakehouse solution for sensor data that trains a machine-learning model for the STEDI team.
Project Details
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
•	trains the user to do a STEDI balance exercise;
•	and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
•	has a companion mobile app that collects customer data and interacts with the device sensors.
Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.
The STEDI team wants to use the motion sensor data to train a machine-learning model to detect steps accurately in real time. Privacy will be a primary consideration in deciding what data can be used.
Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.
Project Summary

STEP 1 EXTRACTION
As a data engineer on the STEDI Step Trainer team, we need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model. It uses the following tools:
•	Python and Spark
•	AWS Glue
•	AWS Athena
•	AWS S3
Project Data
Customer Records (from fulfillment and the STEDI website):
contains the following fields:
•	serialnumber
•	sharewithpublicasofdate
•	birthday
•	registrationdate
•	sharewithresearchasofdate
•	customername
•	email
•	lastupdatedate
•	phone
•	sharewithfriendsasofdate
Step Trainer Records (data from the motion sensor):
contains the following fields:
•	sensorReadingTime
•	serialNumber
•	distanceFromObject
Accelerometer Records (from the mobile app):
contains the following fields:
•	timeStamp
•	user
•	x
•	y
•	z
Solution Outline

STEP 2 TRANSFORM
We build the data lake solution using AWS S3.  
The raw data is fetched from different sources and processed through the following zones.
Landing Zone - Data Ingestion
The raw data are made available in S3 buckets in the landing zone. The data from the landing zone is ingested into Glue jobs for ETL. Before processing, we create Glue tables from S3 landing buckets and then query those tables using Athena.
Trusted Zone - Getting Customer consent
Next, we sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes. Similarly, we sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone).
Curated Zone - Curated machine learning data
Here, we sanitize the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research. We, then, read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table. Finally, we create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.

