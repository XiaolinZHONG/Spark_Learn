package com.ctrip.fin.csm


import com.ctrip.fin.csm.processer._
import com.ctrip.fin.csm.processer.utils.ModelUtil
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}



/**
  * Created by zhongxl on 2016/10/13.
  */
object CsmStarter {
  def main(args: Array[String]): Unit = {


    // 声明
    val conf = new SparkConf()
      .setAppName("Credit Score Model")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val startTime=System.currentTimeMillis()//获取开始时间

    /***
      * 读取training数据:
      * 格式：csv
      **/
    val adress="D:/project_csm/bigtable_2B_xc_new.csv"
    val df=read_data(adress,sqlContext).cache()

    /***
      * 训练打分模型
      * **/
    //创建5个类
    val peopleModel     = new PeopleModel()
    val consumingModel  = new ConsumingModel()
    val fanacialModel   = new FanacialModel()
    val interactionModel= new InteractionModel()
    val relationModel   = new RelationModel()

    // 数据分5份训练

    //people

    val scaler_people=peopleModel
      .training_data(df,"uid_flag",sc,"/tmp/mypeoplemodel")

    //cosuming

    val scaler_consuming=consumingModel
      .training_data(df,"uid_flag",sc,"/tmp/myconsumingmodel")

    //fanacial

    val scaler_fanacial=fanacialModel
      .training_data(df,"uid_flag",sc,"/tmp/myfanacialmodel")

    //relation

    val scaler_relation=relationModel
      .training_data(df,"uid_flag",sc,"/tmp/myrelationmodel")

    //interaction

    val scaler_interaction=interactionModel
      .training_data(df,"uid_flag",sc,"/tmp/myinteractionmodel")

    val endTime=System.currentTimeMillis() //获取结束时间


    //读取测试数据
    val data_tst=read_data("D:/project_csm/tst_data.csv",sqlContext).cache()
    data_tst.show()

    /***
      * 测试数据打分
      * **/

    val startTime_m = System.currentTimeMillis()//获取开始时间

    //身份特质打分
    val score_people      = peopleModel
      .modelling_data(data_tst,scaler_people,sc,sqlContext,"/tmp/mypeoplemodel")
    //消费特点打分
    val score_consuming   = consumingModel
      .modelling_data(data_tst,scaler_consuming,sc,sqlContext,"/tmp/myconsumingmodel")
    //还款能力打分
    val score_fanacial    = fanacialModel
      .modelling_data(data_tst,scaler_fanacial,sc,sqlContext,"/tmp/myfanacialmodel")
    //人脉关系打分
    val score_relation    = relationModel
      .modelling_data(data_tst,scaler_relation,sc,sqlContext,"/tmp/myrelationmodel")
    //交互特点打分
    val score_interaction = interactionModel
      .modelling_data(data_tst,scaler_interaction,sc,sqlContext,"/tmp/myinteractionmodel")

    /***
      * 汇总打分
      * **/

    val modelUtil= new ModelUtil()
//
    val score_12=modelUtil.concat_DF(score_people,score_consuming,sqlContext)
    val score_123=modelUtil.concat_DF(score_12,score_fanacial,sqlContext)
    val score_1234=modelUtil.concat_DF(score_123,score_interaction,sqlContext)
    val score_all=modelUtil.concat_DF(score_1234,score_relation,sqlContext)

    val score=score_all.withColumn("score_all",
      score_all("score_people")*0.3+
      score_all("score_consuming")*0.25 +
      score_all("score_fanacial")*0.2+
      score_all("score_interaction")*0.15+
      score_all("score_relation")*0.1)

    score.show()


    val endTime_m=System.currentTimeMillis()//获取结束时间
    println("Scala训练模型运行时间： "+(endTime - startTime)+"ms")
    println("Scala打分模型运行时间： "+(endTime_m - startTime_m)+"ms")

    val score_with_label= modelUtil
      .concat_DF(score.select("score_all"),data_tst.select("score"),sqlContext)
//
//    score_with_label.show()

//    val landp=score_with_label.withColumn("score_label",(score_with_label("score_all")-350)/500).select("uid_flag","score_label")
//
//    landp.show()
    score_with_label.write.format("com.databricks.spark.csv").option("header","true").save("/tmp/myscore.csv")

  }
  def read_data(adress:String,sqlContext: SQLContext)={
    val df= sqlContext.read
      .format("csv")//注意这里默认安装了CSV的包
      .option("header", "true")//header
      .option("inferSchema", "true") //规范格式类型
      .load(adress)
    println("下面是读取的数据："+df.count())
    df.show(5)//显示前5行
    df.printSchema()
    df
  }
}
