from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext
import json
import train_svm
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import pyspark_cassandra, uuid
from datetime import datetime as dt


#module load sparkfix
#spark-submit --master=yarn-client --executor-memory 6G --num-executors 15 --packages TargetHolding/pyspark-cassandra:0.3.5,com.databricks:spark-csv_2.11:1.5.0 --py-files svm.zip process_twitter.py airlines_tweets airline.csv vap 



"""CREATE TABLE airlines_predictions (
             airline TEXT,
             id uuid,
             processing_time TIMESTAMP,
             processing_id uuid,
             comment TEXT,
             retweets INT, 
             prediction INT,
             PRIMARY KEY (airline, id)
           );

CREATE TABLE airlines_score (
             airline TEXT,
             score INT,
             processing_id uuid,
             total_comments INT,
             positive_comments INT, 
             PRIMARY KEY (airline)
           );"""



search="americanair, virginamerica, southwestair, delta, klm, alaskaair, jetblue, emirates, asianaairlines, qatarairways, aircanada, latamairlinesus, etihadairways, JetBlue, AirAsia, flyPAL, turkishairlines, British_Airways,westjet, SingaporeAir, cathaypacific, EVAAirUS,FlyANA_official, iQantas, lufthansa, HK_Airlines, airfrance, taportugal, AeromexicoUSA".lower().replace(" ", "").split(",")



def process_training(sc, inputs):
    
    #Read the CSV file and converting it to dataframe
    sqlContext = SQLContext(sc)
    data = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(inputs)
    data_df = data.select(data.content.alias("sentiment"), data.recommended).cache()
    data = data_df.rdd.map(lambda x: "%s:::%s" % (x['sentiment'], x['recommended']))
    return data    

def search_airlines(result, processing_id):
    
    ret=[]
    ret_dict={}
    ret_dict['id']=str(uuid.uuid1())
    ret_dict['processing_time']=dt.now()
    ret_dict['processing_id']=processing_id
    ret_dict['retweets']=result[3]-1
    ret_dict['prediction']=result[2]
    ret_dict['comment']=result[1]
    
    for word in result[0]:
    	if word.lower() in search:
 	        ret_dict['id']=str(uuid.uuid1())
		ret_dict['airline']=word.lower()
                ret.append(ret_dict)
    return ret

def add_tuples(a, b):		
    return tuple(sum(p) for p in zip(a,b))    

def calculate_score((airline, (pos, total)), processing_id):
    ret={}
    
    ret['airline']=airline
    ret['score']=float(pos)/float(total)
    ret['total_comments']=total
    ret['positive_comments']=pos
    ret['processing_id']=processing_id

    return ret
    
def save_results(results, output):
    processing_id=str(uuid.uuid1())
    re_air=results.flatMap(lambda result: search_airlines(result, processing_id)).cache()    
    re_air.saveToCassandra(output, 'airlines_predictions', parallelism_level=64)

    re_air.map(lambda d: (d['airline'], (d['prediction'], 1))) \
          .reduceByKey(add_tuples) \
          .map(lambda d: calculate_score(d, processing_id)) \
          .saveToCassandra(output, 'airlines_score', parallelism_level=64)
    


def map_tweets(line):
    tweet = json.loads(line) # load it as Python dict

    try:
        ret=tweet['text'].encode('ascii', 'ignore')
        if ret.startswith('RT'):
            ret=ret[3:]
    except:
        ret='null'

    return (ret, 1)
    
def inspect(rdd):
    for l in rdd.take(20):
        print l

def process_target(sc, inputs, train_input, output):
    air_tweets=sc.textFile(inputs + "/*")
    #rt filtering retweets
    air_tweets=air_tweets.map(map_tweets) \
                         .filter(lambda (comment, num):  not comment.startswith('null'))\
                         .reduceByKey (lambda a,c: a+c )

    train_data=process_training(sc, train_input)
    results=predict_using_svm(sc, train_data, air_tweets)
    save_results(results, output)

def get_words((comment, rt)):
    d_new=train_svm.clean_str(comment).split(" ")
    return comment, rt, d_new
    

def map_predictions(row, svm_model):
 
   prediction = svm_model.predict(row['result'])

   return (row['comment'], row['original_comment'], prediction, row['retweets'])

def predict_using_svm(sc, train_data, air_tweets):
    data_schema = StructType([StructField('original_comment', StringType(), False), 
                              StructField('retweets', IntegerType(), True),
                              StructField('comment', ArrayType(StringType(), True)),])

    svm_model, w2v_model = train_svm.train_svm_w2vec(sc, train_data)
    air_tweets_df=air_tweets.map(get_words).toDF(data_schema)
    t_air_tweets=train_svm.word2vec_transform(air_tweets_df, w2v_model)
    
    results=t_air_tweets.map(lambda row: map_predictions(row, svm_model))
     
    return results   
    
if __name__ == "__main__":
    conf = SparkConf().setAppName('Airline Data')
    inputs=sys.argv[1]
    input_train = sys.argv[2]
    output = sys.argv[3]
    
    cluster_seeds = ['199.60.17.136', '199.60.17.173']
    conf = SparkConf().set('spark.cassandra.connection.host', ','.join(cluster_seeds)) 
    sc = pyspark_cassandra.CassandraSparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    process_target(sc, inputs, input_train, output)





