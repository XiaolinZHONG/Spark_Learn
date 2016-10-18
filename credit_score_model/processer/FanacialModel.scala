package com.ctrip.fin.csm.processer

import com.ctrip.fin.csm.processer.utils.ModelUtil
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SQLContext}

/**
  * Created by zhongxl on 2016/10/13.
  */
class FanacialModel {
  val modelUtil= new ModelUtil()

  def training_data(trnData:DataFrame,
                    flag:String,
                    sc:SparkContext,
                    modelname:String):StandardScalerModel= {
    /** *
      * 训练数据的处理
      * */
    val consuming_trn = process_data(trnData,triger=true)

    println("这是还款能力的原始数据：")
    consuming_trn.show(5)
    consuming_trn.printSchema()
    consuming_trn.describe().show()

    //转换为LP格式
    val trndata_new = modelUtil.df_to_labeledPoint(consuming_trn, flag)

    // 归一化训练数据
    val vectors = trndata_new.map(p => p.features)
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    val trndata_new_scal = trndata_new.map(point =>
      LabeledPoint(point.label, scaler.transform(point.features))
    )
    // 训练模型
//    modelUtil.train_model(trndata_new_scal, sc, modelname = modelname)
    return scaler
  }

  def modelling_data(tstData:DataFrame,
                     scaler:StandardScalerModel,
                     sc:SparkContext,
                     sqlContext: SQLContext,
                     modelname:String):DataFrame={
    /***
      * 测试数据处理
      * */
    val consuming_tst=process_data(tstData,triger=false)

    //转换数据为Vector
    val tstdata_new=modelUtil.df_tovectors(consuming_tst)

    //归一化测试数据
    val tstdata_new_scal=scaler.transform(tstdata_new)

    // 测试数据的预测
    val sameGBTModel=GradientBoostedTreesModel.load(sc,modelname+"_GBT")
    val sameRFModel=RandomForestModel.load(sc,modelname+"_RF")
    val prediction_GBT  =sameGBTModel.predict(tstdata_new_scal)
    val prediction_RF  =sameRFModel.predict(tstdata_new_scal)

    //融合预测的数据
    import sqlContext.implicits._//声明隐式操作
    val predict_1      = prediction_RF.map(i=>i.toString).toDF("prediction_1")
    val predict_2      = prediction_GBT.map(i=>i.toString).toDF("prediction_2")

    val predict_1_temp = predict_1.select(predict_1("prediction_1").cast(DoubleType).as("prediction_1"))
    val predict_2_temp = predict_2.select(predict_2("prediction_2").cast(DoubleType).as("prediction_2"))

    val score_1        = predict_1_temp
      .withColumn("score_fanacial",predict_1_temp("prediction_1")*500+350).select("score_fanacial")
    //-------------------------------------------自此可以直接只返回RF----------------------------------------------------//
    //    val score_2        = score_1.withColumn("score_GBT",score_1("prediction_2")*500+350)
    //
    //
    //    val score_df       = score_2.withColumn("score",score_2("score_RF")*0.6+score_2("score_GBT")*0.4).select("score")
    //    score_df
    return  score_1

  }

  def process_data(data: DataFrame,triger:Boolean):DataFrame={
    /***
      * 通过利用DF 的特征来构建新的特征，通过添加triger来区分是对训练数据的处理还是对测试数据的处理
      * */

    val data_1=data.withColumn("cap_balance",
      data("cap_tmoney_balance")+data("cap_wallet_balance")+data("cap_wallet_balance"))

    val data_2=data_1.withColumn("bil_pays_ratio",
      data("bil_paysord_count")/data("bil_payord_count"))

    val data_3=data_2.withColumn("bil_pays_credit_ratio",
      data("bil_paysord_credit_count")/data("bil_payord_credit_count"))

    val data_4=data_3.withColumn("bil_pays_debit_ratio",
      data("bil_paysord_debit_count")/data("bil_payord_debit_count"))

    val data_5=data_4.withColumn("ord_success_first_class_order_price",
      data("ord_success_first_class_order_amount")/data("ord_success_first_class_order_count"))

    val data_6=data_5.withColumn("ord_success_htl_aboard_order_price",
      data("ord_success_htl_aboard_order_amount")/data("ord_success_htl_aboard_order_count"))

    if (triger==true){
      val data_new=data_6.na.fill(0)
        .select("uid_flag","voi_complrefund_count","fai_lackbalance","bil_refundord_count",
          "bil_ordertype_count","bil_platform_count","pro_htl_star_prefer",
          "pro_htl_consuming_capacity","pro_phone_type","ord_success_max_order_amount",
          "ord_total_order_amount","ord_success_flt_first_class_order_count",
          "ord_success_trn_max_order_amount","ord_success_htl_first_class_order_count",
          "ord_success_htl_max_order_amount","ord_success_aboard_order_count",
          "cap_balance","ord_success_htl_aboard_order_price","ord_success_first_class_order_price",
          "bil_pays_debit_ratio","bil_pays_credit_ratio","bil_pays_ratio")
      return data_new
    }
    else{
      val data_new=data_6.na.fill(0).select("voi_complrefund_count","fai_lackbalance","bil_refundord_count",
        "bil_ordertype_count","bil_platform_count","pro_htl_star_prefer",
        "pro_htl_consuming_capacity","pro_phone_type","ord_success_max_order_amount",
        "ord_total_order_amount","ord_success_flt_first_class_order_count",
        "ord_success_trn_max_order_amount","ord_success_htl_first_class_order_count",
        "ord_success_htl_max_order_amount","ord_success_aboard_order_count",
        "cap_balance","ord_success_htl_aboard_order_price","ord_success_first_class_order_price",
        "bil_pays_debit_ratio","bil_pays_credit_ratio","bil_pays_ratio")
      return data_new
    }
  }
}
