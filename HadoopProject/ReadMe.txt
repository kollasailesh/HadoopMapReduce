Sailesh Kolla

Big data Management Analytics and Management


Input files and file formats :
Hadoop map-reduce to derive some statistics from Yelp Dataset.
The dataset files are located in hdfs in the following path,
/yelpdatafall/business/business.csv.
/yelpdatafall/review/review.csv.
/yelpdatafall/user/user.csv.

Dataset Description:
The dataset comprises of three csv files, namely user.csv, business.csv and review.csv.  

Business.csv file contain basic information about local businesses. 
Business.csv file contains the following columns "business_id","full_address","categories"

'business_id': (a unique identifier for the business)
'full_address': (localized address), 
'categories': [(localized category names)]  

review.csv file contains the star rating given by a user to a business. Use user_id to associate this review with others by the same user. Use business_id to associate this review with others of the same business. 

review.csv file contains the following columns "review_id","user_id","business_id","stars"
 'review_id': (a unique identifier for the review)
 'user_id': (the identifier of the reviewed business), 
 'business_id': (the identifier of the authoring user), 
 'stars': (star rating, integer 1-5),the rating given by the user to a business

user.csv file contains aggregate information about a single user across all of Yelp
user.csv file contains the following columns "user_id","name","url"
user_id': (unique user identifier), 
'name': (first name, last initial, like 'Matt J.'), this column has been made anonymous to preserve privacy 
'url': url of the user on yelp


Reduce programs in Java to find the following information using the commands below ::


Q1. 
List each business Id that are located in â€œPalo Altoâ€� using the full_address column as the filter column. 
To run the jar file in cluster use:
hadoop jar HadoopProjectpP1Q1.jar /yelpdatafall/business/business.csv /sxk145331_P1_Q1
To see output in the cluster:
hdfs dfs -cat /sxk145331_P1_Q1/* >> sxk145331_P1_Q1.txt

//deleting file folder
hdfs dfs -rm -r /sxk145331_P1_Q1 >> sxk145331_P1_Q1.txt

Sample output: is in sxk145331_P1_Q1.txt

Q2 

Find the top ten rated businesses using the average ratings.
 
To run the jar file in cluster use:
hadoop jar HadoopProjectP1Q2.jar /yelpdatafall/review/review.csv /sxk145331_P1_Q2
To see output in the cluster:
hdfs dfs -cat /sxk145331_P1_Q2/* >> sxk145331_P1_Q2.txt

//deleting file folder
hdfs dfs -rm -r /sxk145331_P1_Q2
 
Sample output: is in sxk145331_P1_Q2.txt


Q3:
List the  business_id , full address and categories of the Top 10 businesses using the average ratings.  

This will require you to use  review.csv and business.csv files.
used reduce side join and job chaining technique to answer this problem.

To run the jar file in cluster use:
hadoop jar HadoopProjectP1Q3.jar /yelpdatafall/review/review.csv /yelpdatafall/business/business.csv /sxk145331_P1_Q3
To see output in the cluster:
hdfs dfs -cat /sxk145331_P1_Q3/* >> sxk145331_P1_Q3.txt

//deleting file folder
hdfs dfs -rm -r /sxk145331_P1_Q3
hdfs dfs -rm -r /sxk145331_tempoutput

Sample output: is in sxk145331_P1_Q3.txt





Q4: 
List the 'user id' and 'stars' of users that reviewed businesses located in Stanford 
Required files are 'business'  and 'review'.

Please use Map side join technique to answer this problem.
Hint: Please load all data in business.csv file into the distributed cache. 

To run the jar file in cluster use:
hadoop jar HadoopProjectP1Q4.jar /yelpdatafall/business/business.csv /yelpdatafall/review/review.csv /sxk145331_P1_Q4
To see output in the cluster:
hdfs dfs -cat /sxk145331_P1_Q4/* >> sxk145331_P1_Q4.txt

//deleting file folder
hdfs dfs -rm -r /sxk145331_P1_Q4

Sample output: is in sxk145331_P1_Q4.txt


Note: You can fnd the source files in HadoopHW1.zip
