// Databricks notebook source
// MAGIC %md
// MAGIC Customer attrition, also known as customer churn, customer turnover, or customer defection, is the loss of clients or customers.
// MAGIC
// MAGIC Telephone service companies, Internet service providers, pay TV companies, insurance firms, and alarm monitoring services, often use customer attrition analysis and customer attrition rates as one of their key business metrics because the cost of retaining an existing customer is far less than acquiring a new one. Companies from these sectors often have customer service branches which attempt to win back defecting clients, because recovered long-term customers can be worth much more to a company than newly recruited clients.
// MAGIC
// MAGIC Companies usually make a distinction between voluntary churn and involuntary churn. Voluntary churn occurs due to a decision by the customer to switch to another company or service provider, involuntary churn occurs due to circumstances such as a customer's relocation to a long-term care facility, death, or the relocation to a distant location. In most applications, involuntary reasons for churn are excluded from the analytical models. Analysts tend to concentrate on voluntary churn, because it typically occurs due to factors of the company-customer relationship which companies control, such as how billing interactions are handled or how after-sales help is provided.
// MAGIC
// MAGIC Predictive analytics use churn prediction models that predict customer churn by assessing their propensity of risk to churn. Since these models generate a small prioritized list of potential defectors, they are effective at focusing customer retention marketing programs on the subset of the customer base who are most vulnerable to churn.

// COMMAND ----------

import org.apache.spark.sql.Encoders;

// COMMAND ----------

case class telecom(customerID: String, 

                   gender: String, 

                   SeniorCitizen: Int, 

                   Partner: String, 

                   Dependents: String, 

                   tenure: Int, 

                   PhoneService: String, 

                   MultipleLines: String, 

                   InternetService: String, 

                   OnlineSecurity: String, 

                   OnlineBackup: String, 

                   DeviceProtection: String, 

                   TechSupport: String, 

                   StreamingTV: String, 

                   StreamingMovies: String, 

                   Contract: String, 

                   PaperlessBilling: String, 

                   PaymentMethod: String, 

                   MonthlyCharges: Double, 

                   TotalCharges: Double, 

                   Churn: String )

// COMMAND ----------

val telecomSchema = Encoders.product[telecom].schema



// COMMAND ----------

val telecomDF = spark.read.schema(telecomSchema).option("header", "true").csv("/FileStore/tables/TelcoCustomerChurn.csv")


// COMMAND ----------


display(telecomDF)

// COMMAND ----------

telecomDF.printSchema()

// COMMAND ----------

telecomDF.createOrReplaceTempView("TelecomData")

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select Churn, count(Churn) from TelecomData group by Churn;
// MAGIC -- how many customer already churn and how many stays ? why ?

// COMMAND ----------

// MAGIC %sql
// MAGIC  
// MAGIC select gender,count(gender), Churn  from TelecomData group by Churn,gender;
// MAGIC  
// MAGIC -- how many male and female customer churns or stays ? why ?
// MAGIC -- does the gender affects the churn rate ? why ?
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select SeniorCitizen,count(SeniorCitizen), Churn  from TelecomData group by Churn,SeniorCitizen;
// MAGIC -- how many senior citizens churn or stay?
// MAGIC -- does the customer

// COMMAND ----------

// MAGIC %sql
// MAGIC select Partner,count(Partner), Churn  from TelecomData group by Churn,Partner;
// MAGIC -- how many customer who churn are single or married? why?
// MAGIC -- how many customer who stays are single or married? why?
// MAGIC -- does the customer maritual status affect the churn rate?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select Dependents,count(Dependents), Churn  from TelecomData group by Churn,Dependents;
// MAGIC
// MAGIC --how many customer with dependents churn or stay?
// MAGIC -- how many customer without dependents churn or stay?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select PhoneService,count(PhoneService), Churn  from TelecomData group by Churn,PhoneService;
// MAGIC
// MAGIC --how many customer with phone service churn or stays ? why ?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select MultipleLines,count(MultipleLines), Churn  from TelecomData group by Churn,MultipleLines;
// MAGIC -- how many customer with multiple lines packages churn or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select InternetService,count(InternetService), Churn  from TelecomData group by Churn,InternetService;
// MAGIC -- how many customer with internet services churn or stays? why?
// MAGIC -- how many customer with no internet services churns or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select OnlineSecurity,count(OnlineSecurity), Churn  from TelecomData group by Churn,OnlineSecurity;
// MAGIC -- how many customer who opt for online security plan churn or stays? why?
// MAGIC -- how mnay customer who does not opt for online security plan churn or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select OnlineBackup,count(OnlineBackup), Churn  from TelecomData group by Churn,OnlineBackup;
// MAGIC -- how many customer who opt for online backup services churn or stays? why?
// MAGIC -- how many customer who does not opt for online backup services churn or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select DeviceProtection,count(DeviceProtection), Churn  from TelecomData group by Churn,DeviceProtection;
// MAGIC -- how many customer who opt for device protection plan churn or stays? why?
// MAGIC -- how many customer who do not opt for device protection plan churn or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select TechSupport,count(TechSupport), Churn  from TelecomData group by Churn,TechSupport;
// MAGIC -- how many customer who opt for tech support plan churns or stays? why?
// MAGIC -- how many customer who does not opt for tech support plan churns or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC
// MAGIC
// MAGIC
// MAGIC select StreamingTV,count(StreamingTV), Churn  from TelecomData group by Churn,StreamingTV;
// MAGIC -- how many customer who opt for streaming package churns or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select StreamingMovies,count(StreamingMovies), Churn  from TelecomData group by Churn,StreamingMovies;
// MAGIC -- how many customer who opt for streaming movie packages churn or stays? why?
// MAGIC -- the previous one measure the correlation between the customer who opt for streaming TV packages with churn rate
// MAGIC -- this one we measure the correlation between the cutsomer who opt for streaming movie packages with churn rate
// MAGIC

// COMMAND ----------

// MAGIC %sql
// MAGIC select Contract,count(Contract), Churn  from TelecomData group by Churn,Contract;
// MAGIC --does the type of contract affect the churn rate or not? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select PaperlessBilling,count(PaperlessBilling), Churn  from TelecomData group by Churn,PaperlessBilling;
// MAGIC -- how many customer who opt for papaerless billing churns or stays?

// COMMAND ----------

// MAGIC %sql
// MAGIC select PaymentMethod,count(PaymentMethod), Churn  from TelecomData group by Churn,PaymentMethod;
// MAGIC -- does the payment method affect the customer decisions to churn or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select cast ((TotalCharges/MonthlyCharges)/12 as Int) as Tenure, count(cast ((TotalCharges/MonthlyCharges)/12 as Int)), Churn  from TelecomData group by Churn,cast ((TotalCharges/MonthlyCharges)/12 as Int);
// MAGIC -- does the tenure affect the customer decisions to churn or stays? why?

// COMMAND ----------

// MAGIC %sql
// MAGIC select cast ((TotalCharges/MonthlyCharges)/12 as Int) as Tenure, count(cast ((TotalCharges/MonthlyCharges)/12 as Int)) as counts, Churn  from TelecomData group by Churn,cast ((TotalCharges/MonthlyCharges)/12 as Int)  order by Tenure;
// MAGIC  
// MAGIC -- same as above but the result is sorted by tenure value from lowest to highest 

// COMMAND ----------

// MAGIC %md
// MAGIC we have completed part 1 of the group assignmennt, we performed EDA, descriptive and diagnostic analytics based on the above business questions
// MAGIC now we are moving towards part 2 of the assignment, data cleaning, predictive analytis and prescriptive analytics.
// MAGIC
// MAGIC we are trying to predict which customer will churn or stays? measure the accuracy of the prediction? finally we will provide our own individual recommendations to prevent customer churns

// COMMAND ----------

import org.apache.spark.sql.types._

import org.apache.spark.sql.functions._



import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.ml.feature.VectorAssembler


// COMMAND ----------

var StringfeatureCol = Array("customerID", "gender", "Partner", "Dependents", "PhoneService", "MultipleLines", "InternetService", "OnlineSecurity", "OnlineBackup", "DeviceProtection", "TechSupport", "StreamingTV", "StreamingMovies", "Contract", "PaperlessBilling", "PaymentMethod", "Churn")


//select the required attribute or columns in the dataset

// COMMAND ----------

import org.apache.spark.ml.attribute.Attribute

import org.apache.spark.ml.feature.{IndexToString, StringIndexer}

import org.apache.spark.ml.{Pipeline, PipelineModel}



val indexers = StringfeatureCol.map { colName =>

  new StringIndexer().setInputCol(colName).setHandleInvalid("skip").setOutputCol(colName + "_indexed")

}



val pipeline = new Pipeline()

                    .setStages(indexers)      



val TelDF = pipeline.fit(telecomDF).transform(telecomDF)

// COMMAND ----------

TelDF.printSchema()
// display the new dataframe
// we already perform an indexing operation for certain attributes or columns on the dataset
// those columns with string data type we already convert to double or numerical data type with indexed words behinds
// before we process the data with the logistic regression model 

// COMMAND ----------

val splits = TelDF.randomSplit(Array(0.7, 0.3))

val train = splits(0)

val test = splits(1)

val train_rows = train.count()

val test_rows = test.count()

println("Training Rows: " + train_rows + " Testing Rows: " + test_rows)
// we need to split the dataset into 2 parts, 70% training data and 30% testing data
// 7040 * 70% and 7040 * 30%
// we are using supervised learning techniques here - logistic regression
// computer need to learn from the training data first before the computer can do the prediction on th tesing data
// cmd 29

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler



val assembler = new VectorAssembler().setInputCols(Array("customerID_indexed", "gender_indexed", "SeniorCitizen", "Partner_indexed", "Dependents_indexed", "PhoneService_indexed", "MultipleLines_indexed", "InternetService_indexed", "OnlineSecurity_indexed", "OnlineBackup_indexed", "DeviceProtection_indexed", "TechSupport_indexed", "StreamingTV_indexed", "StreamingMovies_indexed", "Contract_indexed", "PaperlessBilling_indexed", "PaymentMethod_indexed", "tenure", "MonthlyCharges", "TotalCharges" )).setOutputCol("features")

val training = assembler.setHandleInvalid("skip").transform(train).select($"features", $"Churn_indexed".alias("label"))

training.show()
// select the requireed columns and combin them into a single vector
// set churn columns to be a label or target variable
// we are using supervised learning tehcniques here - logistic regression, thus we need a label
// the churn is the Y-axis, if we know X then we can predict Y (regression)
//the features is x axis and the label is y axis
// we are setting up the training dataset here

// COMMAND ----------

import org.apache.spark.ml.classification.LogisticRegression



val lr = new LogisticRegression().setLabelCol("label").setFeaturesCol("features").setMaxIter(10).setRegParam(0.3)

val model = lr.fit(training)

println("Model Trained!")
//create the logistic regression model and send the training data into the model for learning

// COMMAND ----------

val testing = assembler.transform(test).select($"features", $"Churn_indexed".alias("trueLabel"))

testing.show()
//preparing the testing daata, display the predicted vs actual value for us to compare the accuracy of the prediction later after we apply the logistic regression model on the testing data set

// COMMAND ----------

val prediction = model.transform(testing)

val predicted = prediction.select("features", "prediction", "trueLabel")

predicted.show(200)
//apply the model on the testing set and display the top 200 results for the comparison

// COMMAND ----------

val tp = predicted.filter("prediction == 1 AND truelabel == 1").count().toFloat

val fp = predicted.filter("prediction == 1 AND truelabel == 0").count().toFloat

val tn = predicted.filter("prediction == 0 AND truelabel == 0").count().toFloat

val fn = predicted.filter("prediction == 0 AND truelabel == 1").count().toFloat

val metrics = spark.createDataFrame(Seq(

 ("TP", tp),

 ("FP", fp),

 ("TN", tn),

 ("FN", fn),

 ("Precision", tp / (tp + fp)),

 ("Recall", tp / (tp + fn)))).toDF("metric", "value")

metrics.show()

//we will 


// COMMAND ----------

val evaluator = new BinaryClassificationEvaluator().setLabelCol("trueLabel").setRawPredictionCol("rawPrediction").setMetricName("areaUnderROC")

val auc = evaluator.evaluate(prediction)

println("AUC = " + (auc))

//area under ROC, this is another way to measure the accuracy of the prediction homework
// you can try use decision tree model here, we will do this in next class
