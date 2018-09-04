val args = sc.getConf.get("spark.driver.args").split("\\s+")
if(args.size<4){
        println("Insufficient input filename number, should be at least four input filenames. Please check readme file or contact yw2983.nyu.edu.")
        exit()
}
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.feature.Normalizer
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
val args = sc.getConf.get("spark.driver.args").split("\\s+")
val TaxiDataFileName = args(0) //"BDAD_Project/2016YellowTaxiData"
val linkinfoFileName = args(3) //"BDAD_Project/linkinfo.csv"
val cleanWeatherDataFileName = args(1) //"BDAD_Project/CleanWeatherData"
val cleanTrafficDataFileName = args(2) //"BDAD_Project/CleanTrafficData"
val sqlContext = new SQLContext(sc)
var must_train_lr_model_flag = false
var must_train_gbr_model_flag = false
if(args.size>4 && args(4) == "train"){
	must_train_lr_model_flag = true
	must_train_gbr_model_flag = true
}
import sqlContext._
import sqlContext.implicits._
val csv = sc.textFile(TaxiDataFileName)
val headerAndRows = csv.map(line => line.split(",").map(_.trim))
val header = headerAndRows.first
val data = headerAndRows.filter(_(0) != header(0))
val tupleData = data.filter(_.size==19).map(arr=>(arr(0).toInt, arr(1), arr(2), arr(3).toInt, arr(4).toDouble, arr(5).toDouble, arr(6).toDouble,arr(7).toInt, arr(8), arr(9).toDouble, arr(10).toDouble, arr(11).toInt,arr(12).toDouble, arr(13).toDouble, arr(14).toDouble, arr(15).toDouble, arr(16).toDouble, arr(17).toDouble,arr(18).toDouble))
val df = sqlContext.createDataFrame(tupleData).toDF(header(0), header(1), header(2), header(3), header(4), header(5), header(6), header(7), header(8), header(9), header(10), header(11), header(12), header(13), header(14), header(15), header(16), header(17), header(18))
val cleanDF = df.where(df("tpep_dropoff_datetime").contains("2016")).where(df("trip_distance")>0)
val selectedCleanDF = cleanDF.select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "pickup_longitude", "pickup_latitude", "dropoff_longitude", "dropoff_latitude")
def computeDist(x1:(Double,Double),x2:(Double,Double)) = {
	(x1._1-x2._1)*(x1._1-x2._1)+(x1._2-x2._2)*(x1._2-x2._2)
}
def computeMidPoint(x1:(Double,Double),x2:(Double,Double))={
	((x1._1+x2._1)/2,(x1._2+x2._2)/2)
}
def computeNearestLinkId(x:(Double,Double), linkArr:Array[(Int, (Double, Double))]) = {
	val sortLinlArr = linkArr.map(t=>(computeDist(x,t._2),t._1)).sorted
	sortLinlArr(0)._2
}
def computeDuration(PUtimeStamp : String, DOtimeStamp:String)={
	val PUarr = PUtimeStamp.split(" ")(1).split(":")
	val DOarr = DOtimeStamp.split(" ")(1).split(":")
	val PUd = PUtimeStamp.split(" ")(0).split("-")(2)
	val DOd = DOtimeStamp.split(" ")(0).split("-")(2)
	(DOd.toInt-PUd.toInt)*24*60*60+DOarr(0).toInt*60*60+DOarr(1).toInt*60+DOarr(2).toInt-PUarr(0).toInt*60*60-PUarr(1).toInt*60-PUarr(2).toInt
}
val linkCsv = sc.textFile(linkinfoFileName)
val head = linkCsv.first
val linkRDD = linkCsv.filter(_.size>1).filter(_(0) != head(0)).map(_.split("\"").map(_.trim)).map(arr=>(arr(0),arr(1).replaceAll("\\s{2,}", " "))).map(t=>(t._1.replaceAll(",",""),t._2.split(" ")(0),t._2.split(" ").size,t._2)).map(t=>(t._1,t._2,t._4.split(" ")(t._3-2))).map(t=>(t._1.toInt,(t._2.split(",")(0).toDouble,t._2.split(",")(1).toDouble),(t._3.split(",")(0).toDouble,t._3.split(",")(1).toDouble))).map(t=>(t._1,computeMidPoint(t._2,t._3)))
val linkArr = linkRDD.collect
val taxiRDD = selectedCleanDF.rdd.map(r=>(r.getInt(0),r.getString(1),r.getString(2),r.getInt(3),r.getDouble(4),r.getDouble(5),r.getDouble(6),r.getDouble(7),r.getDouble(8)))
val linkedTaxiRDD = taxiRDD.map(t=>(t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,t._9,computeNearestLinkId((t._7,t._6),linkArr),computeNearestLinkId((t._9,t._8),linkArr)))
val labeledTaxiRDD = linkedTaxiRDD.map(t=>(t._1,t._2,t._3,t._4,t._5,t._6,t._7,t._8,t._9,t._10,t._11,computeDuration(t._2,t._3))).filter(t=>t._12>0)
val labeledTaxiDF = sqlContext.createDataFrame(labeledTaxiRDD).toDF(selectedCleanDF.columns(0),selectedCleanDF.columns(1),selectedCleanDF.columns(2),selectedCleanDF.columns(3),selectedCleanDF.columns(4),selectedCleanDF.columns(5),selectedCleanDF.columns(6),selectedCleanDF.columns(7),selectedCleanDF.columns(8),"Pickup_LinkID","Dropoff_LinkID","Duration")
def computeWeatherJoinID(timeStamp: String)={
	val Array(date,time) = timeStamp.split(" ")
	val Array(h,min,s) = time.split(":")
	val Array(y,mon,d) = date.split("-")
	var id = "1/1/16 0:51"
	val year = (y.toInt%2000).toString
	if(min.toInt>50){id = mon.toInt.toString+"/"+d.toInt.toString+"/"+year+" "+h.toInt.toString+":51"}
	else if(h.toInt>0){id = mon.toInt.toString+"/"+d.toInt.toString+"/"+year+" "+(h.toInt-1).toString+":51"}
	else if(d.toInt>1){id = mon.toInt.toString+"/"+(d.toInt-1).toString+"/"+year+" 23:51"}
	else if(mon.toInt>1){id = (mon.toInt-1).toString+"/29/"+year+" :23:51"}
	id
}
def roundTimeToFive(timeStamp:String) = {
	val Array(date,time) = timeStamp.split(" ")
	val Array(h,min,s) = time.split(":")
	var m:String = "00"
	if((min.toInt%10)>4){m= min(0)+"5"}
	else{m= min(0)+"0"}
	date+" "+h+":"+m
}
def computeTrafficJoinID(timeStamp:String) = {
	val Array(date,time) = timeStamp.split(" ")
	val Array(h,minute,s) = time.split(":")
	val Array(y,mon,d) = date.split("-")
	var m:String = "00"
	if((minute.toInt%10)>4){m= minute(0)+"5"}
	else{m= minute(0)+"0"}
	mon.toInt.toString+"/"+d.toInt.toString+"/"+y+" "+h+":"+m
}
def dirExists(hdfsDirectory: String): Boolean = {
    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)
    val exists = fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
    exists
 }
val finalTaxiRDD = labeledTaxiRDD.map(t=>(t._2,t._4,t._5,t._6,t._7,computeTrafficJoinID(t._2)+" "+t._10,computeTrafficJoinID(t._2)+" "+t._11,computeWeatherJoinID(t._2),t._12))
val cleanTrafficData = sc.textFile(cleanTrafficDataFileName).map(_.split(","))
val finalTrafficRDD = cleanTrafficData.map(arr=>(roundTimeToFive(arr(3))+" "+arr(0),arr(1)))
val joinPUtrafficData = finalTaxiRDD.keyBy(_._6).join(finalTrafficRDD).persist	// join the traffic data at pick up location
val joinPUtrafficRDD = joinPUtrafficData.map(t=>(t._2._1._1,t._2._1._2,t._2._1._3,t._2._1._4,t._2._1._5,t._2._1._7,t._2._1._8,t._2._1._9,t._2._2))
val joinDOtrafficData = joinPUtrafficRDD.keyBy(_._6).join(finalTrafficRDD).persist// join the traffic data at drop off location
val joinDOtrafficRDD = joinDOtrafficData.map(t=>(t._2._1._1,t._2._1._2,t._2._1._3,t._2._1._4,t._2._1._5,t._2._1._7,t._2._1._8,t._2._1._9,t._2._2)).persist
val cleanWeatherData = sc.textFile(cleanWeatherDataFileName).map(_.split(",")).filter(_.size>1)
val WeatherColumns = cleanWeatherData.first
val cleanWeatherRDD = cleanWeatherData.filter(_(0)!=WeatherColumns(0)).map(arr=>(arr(1),arr(2),arr(3),arr(4))).filter(_._1.split(" ")(0).split("/")(0).toInt<7).map(t=>(t._1,(t._2.toDouble,t._3.toDouble,t._4.toDouble)))
val weatherLookupMap = cleanWeatherRDD.collect.toMap
def lookupWeather(date:String, map:Map[String,(Double, Double, Double)])={
	var r = (0.0,0.0,0.0)
	try{r = map(date)}
	catch{
		case e: NoSuchElementException => print("")
	}
	r
}
val TaxiTrafficWeatherData = joinDOtrafficRDD.map(t=>(t._1,t._2,t._3,t._4,t._5,t._7,t._8,t._9,lookupWeather(t._6,weatherLookupMap))).persist
val TaxiTrafficWeatherRDD = TaxiTrafficWeatherData.map(t=>(t._1,t._2,t._3,t._4,t._5,t._6,t._7.toDouble,t._8.toDouble,t._9._1,t._9._2,t._9._3))
val saveTTWRDD = TaxiTrafficWeatherRDD.map(t=>t._1+","+t._2+","+t._3+","+t._4+","+t._5+","+t._6+","+t._7+","+t._8+","+t._9+","+t._10+","+t._11)
val TaxiTrafficWeatherDF = sqlContext.createDataFrame(TaxiTrafficWeatherRDD).toDF("Pickup_Datetime","Passanger_Count","Trip_Distance", "Pickup_Longitude","Pickup_Latitude","Trip_Duration","Pickup_Location_Traffic_Speed","Dropoff_Location_Traffic_Speed","Visibility","Precipitation","Wind_Speed")
val TrainDF = TaxiTrafficWeatherDF.select("Passanger_Count","Trip_Distance","Pickup_Location_Traffic_Speed","Dropoff_Location_Traffic_Speed","Visibility","Precipitation","Wind_Speed","Trip_Duration")
val labeledPointRDD_lr = TrainDF.rdd.map(r=>(r.getDouble(1),r.getInt(7))).map(t=>LabeledPoint(t._2, Vectors.dense(t._2,t._1))).cache()
def train_lr_model(modelFileName:String, flag:Boolean):org.apache.spark.mllib.regression.LinearRegressionModel={
	if (dirExists(modelFileName)&& !flag){
		val lr_model = LinearRegressionModel.load(sc,"myLinearRegressionModel")
		return lr_model
	}else{
		val numIterations = 30
		val stepSize = 0.00000001
		val lr_model = LinearRegressionWithSGD.train(labeledPointRDD_lr,numIterations, stepSize)
		return lr_model
	}
}
val lr_model = train_lr_model("myLinearRegressionModel",must_train_lr_model_flag)
val labelsAndPreds_lr =labeledPointRDD_lr.map { point =>
	val prediction = lr_model.predict(point.features)
	(point.label, prediction)
}
val labeledPointRDD_gbr = TrainDF.rdd.map(r=>(r.getInt(0),r.getDouble(1),r.getDouble(2),r.getDouble(3),r.getDouble(4),r.getDouble(5),r.getDouble(6),r.getInt(7))).map(t=>LabeledPoint(t._8,Vectors.dense(t._8,t._1,t._2,t._3,t._4,t._5,t._6,t._7))).repartition(10000).cache()
def train_gbr_model(modelFileName:String, flag:Boolean):org.apache.spark.mllib.tree.model.GradientBoostedTreesModel={
if (dirExists(modelFileName)&& !flag){
		val model = GradientBoostedTreesModel.load(sc,"myGradientBoostedRegressionModel")
		return model
	}else{
		val boostingStrategy = BoostingStrategy.defaultParams("Regression")
		boostingStrategy.numIterations = 3 
		boostingStrategy.treeStrategy.maxDepth = 5
		boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
		val model = GradientBoostedTrees.train(labeledPointRDD_gbr, boostingStrategy)
		return model
	}
}
val model = train_gbr_model("myGradientBoostedRegressionModel",must_train_gbr_model_flag)
val labelsAndPreds_gbr = labeledPointRDD_gbr.map { point =>
	val prediction = model.predict(point.features)
	(point.label, prediction)
}
def computeRMSLE(labelsAndPreds:org.apache.spark.rdd.RDD[(Double, Double)])={
	labelsAndPreds.map{t =>
	var t2 = 0.0
	if(t._2>0){t2 = t._2}
	(t2,t._1)
	}.map{ case(v, p) => math.pow((math.log(v+1) - math.log(p+1)), 2)}.mean()
}
val LabelAndPreds = labeledPointRDD_gbr.map { point =>
	val gbr_pred = model.predict(point.features)
	val lr_pred = lr_model.predict(Vectors.dense(point.features(0),point.features(2)))
	(point.label, gbr_pred, lr_pred)
}
val labelAndPredsDF = sqlContext.createDataFrame(LabelAndPreds).toDF("Real Label", "GBR Predict Label", "LR Predict Label")

val Array(a,b) = lr_model.weights.toArray
println("linear regression model: y="+a+"x+"+b)

println("gradient boosted regression model:")
print("tree weights: ")
model.treeWeights.foreach(x=>print(x+" "))
print("\n")
print(model.toDebugString)

val gbrRMSLE = math.sqrt(computeRMSLE(labelsAndPreds_gbr))
val lrRMSLE = math.sqrt(computeRMSLE(labelsAndPreds_lr))

println("linear regression model's RMSLE: "+lrRMSLE)
println("gradient boosted regression model's RMSLE: "+gbrRMSLE)
println("10 sample of real label and predict label: ")

labelAndPredsDF.show(10)
