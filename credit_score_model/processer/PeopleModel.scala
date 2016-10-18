package com.ctrip.fin.csm.processer

import com.ctrip.fin.csm.processer.utils.ModelUtil
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}

import Array._

/**
  * Created by zhongxl on 2016/10/13.
  */
class PeopleModel {
  val modelUtil= new ModelUtil()
  def training_data(trnData:DataFrame,
                    flag:String,
                    sc:SparkContext,
                    modelname:String):StandardScalerModel= {
    /** *
      * 训练数据的处理
      * */
    val people_trn = trnData.select("uid_flag", "uid_grade", "uid_dealorders", "uid_emailvalid", "uid_age", "uid_mobilevalid",
      "uid_addressvalid", "uid_isindentify", "uid_authenticated_days", "uid_signupdays",
      "uid_signmonths", "uid_lastlogindays", "uid_samemobile", "ord_success_order_cmobile_count",
      "com_mobile_count", "pro_generous_stingy_tag", "pro_base_active",
      "pro_customervalue", "pro_phone_type", "pro_validpoints", "pro_htl_consuming_capacity")

    println("这是身份特质的原始数据：")
    people_trn.show(5)
    people_trn.printSchema()
    people_trn.describe().show()

    //转换为LP格式
    val trndata_new = modelUtil.df_to_labeledPoint(people_trn, flag)

    // 归一化训练数据
    val vectors = trndata_new.map(p => p.features)
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)

    val trndata_new_scal = trndata_new.map(point =>
      LabeledPoint(point.label, scaler.transform(point.features))
    )

    // 训练模型
//    modelUtil.train_model(trndata_new_scal, sc, modelname = modelname)
//    val scalermap=Map(scaler.mean->scaler.std)
    scaler
  }

  def modelling_data(tstData:DataFrame,
                     scaler:StandardScalerModel,
                     sc:SparkContext,
                     sqlContext: SQLContext,
                     modelname:String):DataFrame={
    /***
      * 测试数据处理
      * */

    val people_tst=tstData.select("uid_grade","uid_dealorders","uid_emailvalid","uid_age","uid_mobilevalid",
      "uid_addressvalid","uid_isindentify","uid_authenticated_days","uid_signupdays",
      "uid_signmonths","uid_lastlogindays","uid_samemobile","ord_success_order_cmobile_count",
      "com_mobile_count","pro_generous_stingy_tag","pro_base_active",
      "pro_customervalue","pro_phone_type","pro_validpoints","pro_htl_consuming_capacity")


    //转换数据为Vector
    val tstdata_new=modelUtil.df_tovectors(people_tst)


    //归一化测试数据
//    val scaler3= new StandardScalerModel(scaler.std,scaler.mean)

    val tstdata_new_scal = tstdata_new.map(point=>scaler.transform(point))

//    // 测试数据的预测
    val sameGBTModel   = GradientBoostedTreesModel.load(sc,modelname+"_GBT")
    val sameRFModel    = RandomForestModel.load(sc,modelname+"_RF")
    val prediction_GBT = sameGBTModel.predict(tstdata_new_scal)
    val prediction_RF  = sameRFModel.predict(tstdata_new_scal)

//    //融合预测的数据
    import sqlContext.implicits._//声明隐式操作
    val predict_1 = prediction_RF.map(i=>i.toString).toDF("prediction_1")
    val predict_2 = prediction_GBT.map(i=>i.toString).toDF("prediction_2")

//    val score_1= prediction_GBT.zip(prediction_RF)
    val predict_1_temp = predict_1.select(predict_1("prediction_1").cast(DoubleType).as("prediction_1"))
    val predict_2_temp = predict_2.select(predict_2("prediction_2").cast(DoubleType).as("prediction_2"))
    val score_1=modelUtil.concat_DF(predict_1_temp,predict_2_temp,sqlContext)
    val score_2 = score_1.withColumn("score_RF",score_1("prediction_1")*500+350)

    //-----------自此可以直接只返回RF------------------------------------------------------//
    val score_3 = score_2.withColumn("score_GBT",score_1("prediction_2")*500+350)
    val score_df = score_3.withColumn("score_people",score_3("score_RF")*0.6+score_3("score_GBT")*0.4).select("score_people")
    //-------- --按比例混合双模型的打分----------------------------------------------------//
    score_df
//    score_1
  }
}
