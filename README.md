# NYC-Yellow-Taxi-Trip-Duration-Prediction

Author: Yuanxu Wu, Shuyi Yu, Biqi Lin  

## Prerequisites  
1. NYU Dumbo  
2. Spark

## Data Resource of this application  
1. 2016YellowTaxiData, located at hdfs:///user/yw2983/BDAD_Project/2016YellowTaxiData  
   If you have trouble to access this dataset, please contact yw2983@nyu.edu  
2. CleanWeatherData, located at hdfs:///user/yw2983/BDAD_Project/CleanWeatherData  
   If you have trouble to access this dataset, please contact biqi.lin@nyu.edu  
3. CleanTrafficData, located at hdfs:///user/yw2983/BDAD_Project/CleanTrafficData  
   If you have trouble to access this dataset, please contact sy1144@nyu.edu  
4. linkinfo.csv, located at hdfs:///user/yw2983/BDAD_Project/linkinfo.csv  
   If you have trouble to access this dataset, please contact yw2983@nyu.edu  
5. (optional) myLinearRegressionModel, located at hdfs:///user/yw2983/myLinearRegressionModel  
   If you have trouble to access this dataset, please contact yw2983@nyu.edu  
6. (optional) myGradientBoostedRegressionModel, located at hdfs:///user/yw2983/myGradientBoostedRegressionModel  
   If you have trouble to access this dataset, please contact yw2983@nyu.edu  

## How to run the project using spark-shell (which will take around 50 minutes):  
1. trasfer the NYC_Yellow_Taxi_Trip_prediction.scala to NYU Dumbo.  
e.g. scp NYC_Yellow_Taxi_Trip_prediction.scala <userID@dumbo.es.its.nyu.edu>:~/.  

2. if you want the application must train the model again, add a train flag argument to run the application, using: spark-shell --executor-memory 15g --conf spark.driver.args="<2016TaxiDataDirectoryName> <CleanWeatherDataDirectoryName> <CleanTrafficDataDirectoryName> <linkinfoDirectoryName> train" -i NYC_Yellow_Taxi_Trip_prediction.scala  

    e.g. spark-shell --executor-memory 15g --conf spark.driver.args="BDAD_Project/2016YellowTaxiData BDAD_Project/CleanWeatherData BDAD_Project/CleanTrafficData BDAD_Project/linkinfo.csv train" -i NYC_Yellow_Taxi_Trip_prediction.scala  
   
3. if you want to use the pretrained model to run the application, you have to have the myLinearRegressionModel file and the myGradientBoostedRegressionModel file. To get these two file, please see previous section. Run the project, using: spark-shell --executor-memory 5g --conf spark.driver.args="<2016TaxiDataDirectoryName> <CleanWeatherDataDirectoryName> <CleanTrafficDataDirectoryName> <linkinfoDirectoryName>"  -i NYC_Yellow_Taxi_Trip_prediction.scala  

    e.g. spark-shell --executor-memory 5g --conf spark.driver.args="BDAD_Project/2016YellowTaxiData BDAD_Project/CleanWeatherData BDAD_Project/CleanTrafficData BDAD_Project/linkinfo.csv" -i  NYC_Yellow_Taxi_Trip_prediction.scala  

## How to run the project using spark-submit: (which will take around 5 hours)  
1. set the spark module to spark 2.2.0: module load spark/2.2.0  

2. spark-submit --num-executors 200  --executor-cores 4 --master local[16] --executor-memory 15g --driver-memory 5g --conf spark.default.parallelism=200 nyc_taxi_trip_duration_prediction_2.11-1.0.jar <2016TaxiDataDirectoryName> <CleanWeatherDataDirectoryName> <CleanTrafficDataDirectoryName> <linkinfoDirectoryName> <(optional) train>  

    e.g. spark-submit --num-executors 200  --executor-cores 4 --master local[16] --master yarn-client --executor-memory 15g --driver-memory 15g --conf spark.default.parallelism=1000 --conf spark.memory.useLegacyMode=true --conf spark.storage.memoryFraction=0.9 nyc_taxi_trip_duration_prediction_2.11-1.0.jar BDAD_Project/2016YellowTaxiData BDAD_Project/CleanWeatherData BDAD_Project/CleanTrafficData BDAD_Project/linkinfo.csv  

  
  
