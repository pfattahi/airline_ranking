#Instructions to run
----Daemon------
cd tweet_stream
nohup python3 tweet_stream2.py >out.txt &
#The command is running and saving the twitters inside the folder 'airlines_tweets'
#Copy the files to be processed
hdfs dfs -copyFromLocal  airlines_tweets
#back to the main directory
cd ..
#copying the training file
hdfs dfs -copyFromLocal airline.csv
#Executing

spark-submit --master=yarn-client --executor-memory 6G --num-executors 15 --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --py-files svm.zip process_twitter.py airlines_tweets airline.csv vap 







