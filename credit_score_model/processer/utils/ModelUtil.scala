package com.ctrip.fin.csm.processer.utils

import org.apache.spark.SparkContext
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

/**
  * Created by zhongxl on 2016/10/13.
  */
@serializable
class ModelUtil{

  //转换数据系列
  def df_to_labeledPoint(df:DataFrame,flag:String)={
    /**
      * 输入一个常规DataFrame的数据，返回一个LabeledPiont型的数据
      * 注意：输入的dataframe 必须包含 标签 列
      * */
    val args=df.drop(flag).columns.toList//注意这里
    val lb_df=df.map(row=>LabeledPoint(row.getAs[Double](flag), //attention
      Vectors.dense(Vectors_style(row,args))))
    lb_df
  }
  def df_tovectors(df:DataFrame)={
    /***
      * 输入一个常规的DataFrame的数据，返回一个Vectors
      * */

    val args=df.columns.toList
    val vector_df=df.map(row=>Vectors.dense(Vectors_style(row,args)))
    vector_df
  }
  def Vectors_style(row:Row,args:List[String])={
    /**
      * 根据columns的列名生成一个列名的数组（Array）
      * 原因是Vectors.dense只支持数组的输入形式
      * */
    val x=new Array[Double](args.length)
    var i=0
    for(arg<-args){x(i)=row.getAs(arg).toString.toDouble;i+=1;}
    x
  }

  //训练模型,基于MLLIB
  def train_model(data:RDD[LabeledPoint],sc:SparkContext,modelname:String)={
    /**
      * 使用MLLIB中的RF 和 GBT 回归分析
      * */
    val splits=data.randomSplit(Array(0.75,0.25))
    val (trainingData,testData)=(splits(0),splits(1))

    /**使用RandomForest
      * */

    val numClasses = 2 //二分类
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 30 // 树的数目
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "variance"
    val maxDepth = 10
    val maxBins = 32

    val model_rfr = RandomForest.trainRegressor(trainingData,categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    // 交叉验证集
    val labelsAndPredictions_rfr = testData.map { point =>
      val prediction = model_rfr.predict(point.features)
      (point.label, prediction)
    }
    evaluate_report(labelsAndPredictions_rfr)//评估模型
    evaluate_report_classification(labelsAndPredictions_rfr,testData)

    //模型的保存与重载
    model_rfr.save(sc,modelname+"_RF")

    /**使用GBT regression
      * */

    val boostingStrategy = BoostingStrategy.defaultParams("Regression")
    boostingStrategy.numIterations = 50 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.maxDepth = 10
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model_gbt = GradientBoostedTrees.train(trainingData, boostingStrategy)

    // 交叉验证集
    val labelsAndPredictions_gbt = testData.map { point =>
      val prediction = model_gbt.predict(point.features)
      (point.label, prediction)
    }
    //evaluate_report(labelsAndPredictions_gbt)//评估模型
    evaluate_report_classification(labelsAndPredictions_gbt,testData)

    //模型的保存与重载
    model_gbt.save(sc,modelname+"_GBT")

  }

  //模型评估
  def evaluate_report_classification(labelAndPreds:RDD[(Double,Double)],testData:RDD[LabeledPoint]):Unit={
    /**
      * 评估分类模型的结果
      * */



    val metrics=new BinaryClassificationMetrics(labelAndPreds)
    val score=metrics.scoreAndLabels
    score.foreach{ case (t,p) =>
      println(s"label: $t,prediction: $p")
    }
    val precision = metrics.precisionByThreshold
    precision.foreach { case (t, p) =>
      println(s"Threshold: $t, Precision: $p")
    }

    // Recall by threshold
    val recall = metrics.recallByThreshold
    recall.foreach { case (t, r) =>
      println(s"Threshold: $t, Recall: $r")
    }

    // Precision-Recall Curve
    val PRC = metrics.pr

    // F-measure
    val f1Score = metrics.fMeasureByThreshold
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 1")
    }

    val beta = 0.5
    val fScore = metrics.fMeasureByThreshold(beta)
    f1Score.foreach { case (t, f) =>
      println(s"Threshold: $t, F-score: $f, Beta = 0.5")
    }

    // AUPRC
    val auPRC = metrics.areaUnderPR
    println("Area under precision-recall curve = " + auPRC)

    // Compute thresholds used in ROC and PR curves
    val thresholds = precision.map(_._1)

    // ROC Curve
    val roc = metrics.roc

    // AUROC
    val auROC = metrics.areaUnderROC
    println("Area under ROC = " + auROC)
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)

  }
  def evaluate_report(labelAndPreds:RDD[(Double,Double)]):Unit={
    /***
      * 评估回归模型的结果
      * **/
    val score=labelAndPreds.foreach{case(v,p)=>
      println(s"label: $v,prediction: $p")
    }
    val testMSE=labelAndPreds.map{case(v,p)=>math.pow((v-p),2)}.mean()//使用case 时 中括号
    println("Test Mean Squared Error = " + testMSE)
  }

  //缩放数据，归一化数据,返回SCALER
  def scalerdata_DF(data:DataFrame):MinMaxScaler={

    val scaler= new MinMaxScaler()
    val scalerModel=scaler.fit(data)
    val df_data_new=scalerModel.transform(data)

    println("归一化缩放后生成的Dataframe")
    df_data_new.show(5)

    return scaler
  }

  //拼接Dataframe
  def concat_DF(data_1:DataFrame,data_2:DataFrame,sqlContext: SQLContext):DataFrame={
    val rows=data_1.rdd.zip(data_2.rdd).map{case (rowLeft,rowRight)=>Row.fromSeq(rowLeft.toSeq++rowRight.toSeq)}
    val schema = StructType(data_1.schema.fields ++ data_2.schema.fields)
    val data_12=sqlContext.createDataFrame(rows, schema)
    return data_12
  }

}
