package com.ctrip.fin.csm.processer

import com.ctrip.fin.csm.processer.utils.ModelUtil
import org.apache.spark.SparkContext
import org.apache.spark.mllib.feature.{StandardScaler, StandardScalerModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Row, SQLContext,functions}

/**
  * Created by zhongxl on 2016/10/13.
  */
class ConsumingModel {
  val modelUtil= new ModelUtil()
  def training_data(trnData:DataFrame,
                    flag:String,
                    sc:SparkContext,
                    modelname:String):StandardScalerModel= {
    /** *
      * 训练数据的处理
      * */
    val consuming_trn = process_data(trnData,triger = true)

    println("这是消费特点的原始数据：")
    consuming_trn.show(5)
    consuming_trn.printSchema()
    consuming_trn.describe().show()

    //转换为LP格式
//    println("转换训练数据为LP格式")
    val trndata_new = modelUtil.df_to_labeledPoint(consuming_trn, flag)

    // 归一化训练数据
//    println("归一化训练数据")
    val vectors = trndata_new.map(p => p.features)
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(vectors)
    val trndata_new_scal = trndata_new.map(point =>
      LabeledPoint(point.label, scaler.transform(point.features))
    )
    // 训练模型
//    println("训练模型ing")
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
    val consuming_tst=process_data(tstData,triger = false)

    //转换数据为Vector
//    println("转换测试数据为向量")
    val tstdata_new=modelUtil.df_tovectors(consuming_tst)

    //归一化测试数据
//    println("归一化测试数据")
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
      .withColumn("score_consuming",predict_1_temp("prediction_1")*500+350).select("score_consuming")
    //-------------------------------------------自此可以直接只返回RF----------------------------------------------------//
    //    val score_2        = score_1.withColumn("score_GBT",score_1("prediction_2")*500+350)
    //
    //
    //    val score_df       = score_2.withColumn("score",score_2("score_RF")*0.6+score_2("score_GBT")*0.4).select("score")
    //    score_df
    return  score_1
  }

  def process_data(data:DataFrame,triger:Boolean):DataFrame={

    //平均消费水平
    val data_1=data.withColumn("ord_success_order_price",
      data("ord_success_order_amount")/data("ord_success_order_count"))


    //高星酒店消费
    val data_2=data_1.withColumn("ord_success_first_class_order_price",
      data("ord_success_first_class_order_amount")/data("ord_success_first_class_order_count"))

    //海外酒店
    val data_3=data_2.withColumn("ord_success_aboard_order_price",
      data("ord_success_aboard_order_amount")/data("ord_success_aboard_order_count"))

    //头等舱数据
    val data_4=data_3.withColumn("ord_success_flt_first_class_order_price",
      data("ord_success_flt_first_class_order_amount")/data("ord_success_flt_first_class_order_count"))

    //机票海外订单
    val data_5=data_4.withColumn("ord_success_flt_aboard_order_price",
      data("ord_success_flt_aboard_order_amount")/data("ord_success_flt_aboard_order_count"))

    //机票消费单价
    val data_6=data_5.withColumn("ord_success_flt_order_price",
      data("ord_success_flt_order_amount")/data("ord_success_flt_order_count"))

    //高星酒店
    val data_7=data_6.withColumn("ord_success_htl_first_class_order_price",
      data("ord_success_htl_first_class_order_amount")/data("ord_success_htl_first_class_order_count"))

    //海外酒店
    val data_8=data_7.withColumn("ord_success_htl_aboard_order_price",
      data("ord_success_htl_aboard_order_amount")/data("ord_success_htl_aboard_order_count"))

    //酒店消费单价
    val data_9=data_8.withColumn("ord_success_htl_order_price",
      data("ord_success_htl_order_amount")/data("ord_success_htl_order_count"))

    //火车票消费
    val data_10=data_9.withColumn("ord_success_trn_order_price",
      data("ord_success_trn_order_amount")/data("ord_success_trn_order_count"))

    if (triger==true){
      val data_new=data_10.na.fill(-1.0).select("uid_flag","pro_advanced_date","pro_htl_star_prefer","pro_ctrip_profits",
        "ord_success_max_order_amount","ord_success_avg_leadtime","ord_cancel_order_count",
        "ord_success_order_type_count","ord_success_order_acity_count","ord_success_flt_last_order_days",
        "ord_success_flt_max_order_amount","ord_success_flt_avg_order_pricerate",
        "ord_success_flt_order_acity_count","ord_success_htl_last_order_days","ord_success_htl_max_order_amount",
        "ord_success_htl_order_refund_ratio","ord_success_htl_guarantee_order_count",
        "ord_success_htl_noshow_order_count","ord_cancel_htl_order_count","ord_success_trn_last_order_days",
        "ord_success_order_price","ord_success_first_class_order_price","ord_success_aboard_order_price",
        "ord_success_flt_first_class_order_price","ord_success_flt_aboard_order_price",
        "ord_success_flt_order_price", "ord_success_htl_first_class_order_price","ord_success_htl_aboard_order_price",
        "ord_success_htl_order_price", "ord_success_trn_order_price")
      return data_new
    }
    else{
      val data_new=data_10.na.fill(0).select("pro_advanced_date","pro_htl_star_prefer","pro_ctrip_profits",
        "ord_success_max_order_amount","ord_success_avg_leadtime","ord_cancel_order_count",
        "ord_success_order_type_count","ord_success_order_acity_count","ord_success_flt_last_order_days",
        "ord_success_flt_max_order_amount","ord_success_flt_avg_order_pricerate",
        "ord_success_flt_order_acity_count","ord_success_htl_last_order_days","ord_success_htl_max_order_amount",
        "ord_success_htl_order_refund_ratio","ord_success_htl_guarantee_order_count",
        "ord_success_htl_noshow_order_count","ord_cancel_htl_order_count","ord_success_trn_last_order_days",
        "ord_success_order_price","ord_success_first_class_order_price","ord_success_aboard_order_price",
        "ord_success_flt_first_class_order_price","ord_success_flt_aboard_order_price",
        "ord_success_flt_order_price", "ord_success_htl_first_class_order_price","ord_success_htl_aboard_order_price",
        "ord_success_htl_order_price", "ord_success_trn_order_price")
      return data_new
    }

  }

  def calculate(data:DataFrame,sqlContext: SQLContext,colname:String)={
    /***
      * spark RDD 格式的数据转化为DataFrame格式时，需要声明一个隐式操作;
      * 声明隐式操作的生成DF方法目前只支持（INT LONG STRING）
      * */
    import sqlContext.implicits._//声明隐式操作
    val data_temp=data.map(col=>(col.getDouble(0),col.getDouble(1)))
      .map(newcol=>(newcol._2/newcol._1))
        .map(i=>i.toString)
        .toDF(colname)

    // 将字符串的格式转回Double
    val data_temp2=data_temp.select(data_temp(colname).cast(DoubleType).as(colname))

    //填补DF 中的null值为1，
    // 如果是许多列那么需要fill(1.0,seq("colname"))
    val data_new=data_temp2.na.fill(1.0)
    data_new
  }
}
