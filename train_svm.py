from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import SQLContext
from pyspark.ml.feature import Word2Vec
import re
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.mllib.classification import SVMWithSGD, SVMModel
from pyspark.mllib.regression import LabeledPoint

#time spark-submit --master=yarn-client --num-executors 6 --executor-memory=30gb train_svm.py airline_train.txt out | tee out.txt

embedded_size=128


def clean_str(string):
    """
    Tokenization/string cleaning for all datasets except for SST.
    Original taken from https://github.com/yoonkim/CNN_sentence/blob/master/process_data.py
    """
    string = re.sub(r"[^A-Za-z0-9(),!?\'\`]", " ", string)
    string = re.sub(r"\'s", " \'s", string)
    string = re.sub(r"\'ve", " \'ve", string)
    string = re.sub(r"n\'t", " n\'t", string)
    string = re.sub(r"\'re", " \'re", string)
    string = re.sub(r"\'d", " \'d", string)
    string = re.sub(r"\'ll", " \'ll", string)
    string = re.sub(r",", " , ", string)
    string = re.sub(r"!", " ! ", string)
    string = re.sub(r"\(", " \( ", string)
    string = re.sub(r"\)", " \) ", string)
    string = re.sub(r"\?", " \? ", string)
    string = re.sub(r"\s{2,}", " ", string)
    return string.strip().lower()



def get_line(line):
    try:
        line=line.encode('ascii', 'ignore')
        (comment, sentiment)=line.split(":::") 
        if comment is None or sentiment is None:
            raise Exception('None Value!')
        comment=clean_str(comment)
        sentiment=int(sentiment)
        return (comment, sentiment)
    except:
         return None

def get_words((a,b)):
   o=a.split(" ")  
   return o,b

def get_labeled_points(record):

   return LabeledPoint(int(record['sentiment']),record['result'])


def get_labeled_points_sample(record):
   return LabeledPoint(int(1), record['result'])


def train_word2vec(data):

   word2Vec = Word2Vec(vectorSize=embedded_size, minCount=0, inputCol="comment", outputCol="result")
   model = word2Vec.fit(data)
#   model.save(sc,"train_results/word2vec.train")

   return model

def load_word2vec(sc):

    model=Word2VecModel.load(sc, "googlew2v.bin")
    return model

def word2vec_transform(data, model):

    result = model.transform(data)
  
    return result

def load_svm(sc):

    model= SVMModel.load(sc, "target1/tmp/pythonSVMWithSGDModel")
    return model

def load_data(sc, train_data):
   data_schema = StructType([StructField('comment', ArrayType(StringType(), True)), #])
                            StructField('sentiment', IntegerType(), False), ])
   data=train_data.map(get_line).filter(lambda l: l is not None)   
   data=data.map(get_words)
   training, validation = data.randomSplit([0.8, 0.2])
   training=training.toDF(data_schema).cache()
   validation=validation.toDF(data_schema).cache()

   return training, validation

def train_svm(points):

    model = SVMWithSGD.train(points, iterations=200)

    # Save and load model
    #model.save(sc, "target3/tmp/pythonSVMWithSGDModel")
    return model

def predict_svm(points, model, string):

    labelsAndPreds = points.map(lambda p: (p.label, model.predict(p.features)))
    #print "Predictions: ", labelsAndPreds.take(10)
    trainErr = labelsAndPreds.filter(lambda (v, p): v != p).count() / float(points.count())
    print(string + str(trainErr))

def predict_samples(sqlContext, w2v_model, svm_model):

    # Input data: Each row is a bag of words from a sentence or document.
    samples = sqlContext.createDataFrame([
          ("Flight attendants rude - had multiple flight attendants bump into me with carts and bodies, with no apologies. Offered 1 complimentary soda, with optional water now and then. Terrible service and basically no amenities. Not even chips or pretzels. Seat was dirty and no TV on return flight.".split(" "), ),
          ("I travelled from Sydney Australia to SFO in business the announcements were so loud and the screen behind the seat lit up every time there was any turbulence. Why did I pay a fortune for business class when I could get no sleep whatsoever. The volume of these announcements was the same on United flight SFO to BOS and Chicago to SFO.".split(" "), ),
          ("nhappy customer, traveling back from Toronto my wife was seated in row 12 I was in row 35. No room to put hand luggage had to sit with my legs in the isle with people tripping over them and hit with service trolleys. In flight entertainment didn't work for over half the flight. I went to customer service in San Francisco to complain and was to complain on line. But I could find complaints on there home page. poor effort United Airlines.".split(" "), ),
("On 5 out of 6 flights I've taken I have been told their was minor delays meaning pilot or other employee states 5 to 10 minutes. These delays always seem to translate to 2 to 3 hours whilst the entire time someone is reassuring you it will be just a little longer. Recently made a 4 hour drive to the airport to find out my flight was delayed almost 6 hours, something they were well aware of when the aircraft left cross country flight 5 hours away. Why wasn't I emailed or called. Very often employees shift blame elsewhere too and have little to no actual information.".split(" "), )

          ], ["comment"])

    samples_wvec=word2vec_transform(samples, w2v_model)
    #print "Samples:", samples_wvec.take(10) 
    samples_point=samples_wvec.map(get_labeled_points_sample)

    predict_svm(samples_point, svm_model, "Samples error:")

def train_svm_w2vec(sc,inputs):

    training, validation=load_data(sc, inputs)
    w2v_model=train_word2vec(training)

    t_training=word2vec_transform(training, w2v_model)
    t_validation=word2vec_transform(validation, w2v_model)


    training_points=t_training.map(get_labeled_points).cache()
    validation_points=t_validation.map(get_labeled_points).cache()

    svm_model=train_svm(training_points)

    predict_svm(training_points, svm_model, "Training error:")
    predict_svm(validation_points, svm_model, "Validation error:")
    return svm_model, w2v_model


def main():
   conf = SparkConf().setAppName('Airline Data')
   sc = SparkContext(conf=conf)
   sqlContext = SQLContext(sc)
   inputs = sys.argv[1]
   output = sys.argv[2]
   
   svm_model, w2v_model = train_svm_w2vec(sc, inputs)

   predict_samples(sqlContext, w2v_model, svm_model)

if __name__ == "__main__":
   main()


