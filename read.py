from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext

#Main function to handle all operations
def main(sc, sqlContext, inputs, output):
    
    #Read the CSV file and converting it to dataframe
    data = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(inputs)
    training, validation = data.randomSplit([0.8, 0.2])

    #
    train_df = training.select(training.content.alias("sentiment"), training.recommended).cache()

    positive_train = train_df.filter(train_df.recommended == "1").select(train_df.sentiment).rdd.map(lambda x: x['sentiment'])
    negative_train = train_df.filter(train_df.recommended == "0").select(train_df.sentiment).rdd.map(lambda x: x['sentiment'])

    train_df.unpersist()
    #
    valid_df = validation.select(validation.content.alias("sentiment"), validation.recommended).cache()

    postive_valid = valid_df.filter(valid_df.recommended == "1").select(valid_df.sentiment).rdd.map(lambda x: x['sentiment'])
    negative_valid = valid_df.filter(valid_df.recommended == "0").select(valid_df.sentiment).rdd.map(lambda x: x['sentiment'])
    valid_df.unpersist()

    positive_train.coalesce(1).saveAsTextFile(output + '/positive_train')
    negative_train.coalesce(1).saveAsTextFile(output + '/negative_train')
    postive_valid.coalesce(1).saveAsTextFile(output + '/postive_valid')
    negative_valid.coalesce(1).saveAsTextFile(output + '/negative_valid')
    
if __name__ == "__main__":
    conf = SparkConf().setAppName('Airline Data')
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(sc, sqlContext, inputs, output)