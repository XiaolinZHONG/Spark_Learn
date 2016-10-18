
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by zhongxl on 2016/10/17.
  */

object tst2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("Credit Score Model")
      .setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val myfile=sc.textFile("D:/project_csm/bigtable_2B_xc_new.csv")

    val schemaString="uid_grade,uid_age,uid_lastlogindays,uid_dealorders,uid_emailvalid," +
      "uid_mobilevalid,uid_addressvalid,uid_isindentify,uid_authenticated_days,uid_samemobile," +
      "uid_signupdays,uid_signmonths,uid_haspaypwd,pro_advanced_date,pro_generous_stingy_tag," +
      "pro_htl_star_prefer,pro_htl_consuming_capacity,pro_phone_type,pro_validpoints," +
      "pro_base_active,pro_secretary,pro_agent,pro_ctrip_profits,pro_customervalue," +
      "pro_generousindex_fltn,pro_lastyearprofits_fltn,pro_pricesensitivity_fltn,pro_ismarketing," +
      "ord_success_last_order_days,ord_success_max_order_amount,ord_success_order_count," +
      "ord_success_order_quantity,ord_success_order_amount,ord_success_avg_leadtime," +
      "ord_total_order_count,ord_total_order_quantity,ord_total_order_amount,ord_cancel_order_count," +
      "ord_refund_order_count,ord_success_first_class_order_count,ord_success_first_class_order_amount," +
      "ord_success_order_type_count,ord_success_self_order_count,ord_success_self_order_amount," +
      "ord_success_aboard_order_count,ord_success_aboard_order_amount,ord_success_order_acity_count," +
      "ord_success_order_cmobile_count,ord_success_flt_last_order_days,ord_success_flt_first_class_order_count," +
      "ord_success_flt_first_class_order_amount,ord_success_flt_max_order_amount,ord_success_flt_avg_order_pricerate," +
      "ord_success_flt_aboard_order_count,ord_success_flt_aboard_order_amount,ord_success_flt_order_count," +
      "ord_success_flt_order_amount,ord_success_flt_order_acity_count,ord_cancel_flt_order_count," +
      "ord_refund_flt_order_count,ord_success_htl_last_order_days,ord_success_htl_first_class_order_count," +
      "ord_success_htl_first_class_order_amount,ord_success_htl_max_order_amount,ord_success_htl_order_refund_ratio," +
      "ord_success_htl_aboard_order_count,ord_success_htl_aboard_order_amount,ord_success_htl_guarantee_order_count," +
      "ord_success_htl_noshow_order_count,ord_success_htl_avg_order_pricerate,ord_success_htl_order_count," +
      "ord_success_htl_order_amount,ord_cancel_htl_order_count,ord_refund_htl_order_count,ord_success_pkg_last_order_days," +
      "ord_success_pkg_first_class_order_count,ord_success_pkg_first_class_order_amount,ord_success_pkg_max_order_amount," +
      "ord_success_pkg_aboard_order_count,ord_success_pkg_aboard_order_amount,ord_success_pkg_order_count," +
      "ord_success_pkg_order_amount,ord_cancel_pkg_order_count,ord_refund_pkg_order_count,ord_success_trn_last_order_days," +
      "ord_success_trn_first_class_order_count,ord_success_trn_first_class_order_amount,ord_success_trn_max_order_amount," +
      "ord_success_trn_order_count,ord_success_trn_order_amount,ord_cancel_trn_order_count,ord_refund_trn_order_count," +
      "com_passenger_count,com_idno_count,com_mobile_count,com_has_child,fai_lackbalance,fai_risk,fai_putwrong," +
      "fai_invalid,bil_payord_count,bil_paysord_count,bil_refundord_count,bil_payord_credit_count," +
      "bil_payord_debit_count,bil_paysord_credit_count,bil_paysord_debit_count,bil_ordertype_count," +
      "bil_platform_count,cap_tmoney_balance,cap_wallet_balance,cap_refund_balance,cap_total_balance," +
      "cap_withdrow_count,cap_withdrow_amount,acc_loginday_count,acc_pwd_count,acc_paypwd_count," +
      "acc_bindmobile_count,voi_complaint_count,voi_complrefund_count,voi_comment_count,uid_flag"

    val fields=schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true))
    val schema = StructType(fields)

    val noheader=myfile.filter(line=> !line.contains("uid_grade"))

    val myfile2=noheader.map(_.split(",")).map(p=>Row.fromSeq(p.toSeq))

    val df=sqlContext.createDataFrame(myfile2,schema)
    df.show()

//    val df_tst=read_tst("D:/project_csm/tst_data.csv",sc,sqlContext)
//    df_tst.show()



  }
  def read_tst(adress:String,sc:SparkContext,sqlContext: SQLContext):DataFrame={
    /***
      * 读取csv,txt文件，返回SPARK DATA_FRAME
      * csv或txt文件，不能带index列；
      * 同时数据的columns name 必须单独写成下面的格式；
      * 不可以在中间随意增加columns；
      *
      * 返回值为spark dataframe，但是可能会包含原来文件的header。
      * */
    val myfile=sc.textFile(adress)
    val schemaString="uid_grade,uid_age,uid_lastlogindays,uid_dealorders,uid_emailvalid,uid_mobilevalid," +
      "uid_addressvalid,uid_isindentify,uid_authenticated_days,uid_signupdays,uid_signmonths,pro_advanced_date," +
      "pro_generous_stingy_tag,pro_htl_star_prefer,pro_htl_consuming_capacity,pro_phone_type,pro_validpoints," +
      "pro_base_active,pro_secretary,pro_agent,pro_ctrip_profits,pro_customervalue,pro_generousindex_fltn," +
      "pro_lastyearprofits_fltn,pro_pricesensitivity_fltn,ord_success_last_order_days,ord_success_max_order_amount," +
      "ord_success_order_count,ord_success_order_quantity,ord_success_order_amount,ord_success_avg_leadtime," +
      "ord_total_order_count,ord_total_order_quantity,ord_total_order_amount,ord_cancel_order_count," +
      "ord_refund_order_count,ord_success_first_class_order_count,ord_success_first_class_order_amount," +
      "ord_success_order_type_count,ord_success_self_order_count,ord_success_self_order_amount," +
      "ord_success_aboard_order_count,ord_success_aboard_order_amount,ord_success_order_acity_count," +
      "ord_success_order_cmobile_count,ord_success_flt_last_order_days,ord_success_flt_first_class_order_count," +
      "ord_success_flt_first_class_order_amount,ord_success_flt_max_order_amount,ord_success_flt_avg_order_pricerate," +
      "ord_success_flt_aboard_order_count,ord_success_flt_aboard_order_amount,ord_success_flt_order_count," +
      "ord_success_flt_order_amount,ord_success_flt_order_acity_count,ord_cancel_flt_order_count," +
      "ord_success_htl_last_order_days,ord_success_htl_first_class_order_count,ord_success_htl_first_class_order_amount," +
      "ord_success_htl_max_order_amount,ord_success_htl_order_refund_ratio,ord_success_htl_aboard_order_count," +
      "ord_success_htl_aboard_order_amount,ord_success_htl_guarantee_order_count,ord_success_htl_noshow_order_count," +
      "ord_success_htl_avg_order_pricerate,ord_success_htl_order_count,ord_success_htl_order_amount," +
      "ord_cancel_htl_order_count,ord_refund_htl_order_count,ord_success_pkg_last_order_days," +
      "ord_success_pkg_first_class_order_count,ord_success_pkg_first_class_order_amount,ord_success_pkg_max_order_amount," +
      "ord_success_pkg_aboard_order_count,ord_success_pkg_aboard_order_amount,ord_success_pkg_order_count," +
      "ord_success_pkg_order_amount,ord_cancel_pkg_order_count,ord_refund_pkg_order_count,ord_success_trn_last_order_days," +
      "ord_success_trn_first_class_order_count,ord_success_trn_first_class_order_amount,ord_success_trn_max_order_amount," +
      "ord_success_trn_order_count,ord_success_trn_order_amount,ord_cancel_trn_order_count,ord_refund_trn_order_count," +
      "com_passenger_count,com_idno_count,com_mobile_count,com_has_child,fai_lackbalance,fai_risk,fai_putwrong," +
      "fai_invalid,bil_payord_count,bil_paysord_count,bil_refundord_count,bil_payord_credit_count,bil_payord_debit_count," +
      "bil_paysord_credit_count,bil_paysord_debit_count,bil_ordertype_count,bil_platform_count,cap_tmoney_balance," +
      "cap_wallet_balance,cap_refund_balance,cap_total_balance,cap_withdrow_count,cap_withdrow_amount,acc_loginday_count," +
      "acc_pwd_count,acc_paypwd_count,acc_bindmobile_count,voi_complaint_count,voi_complrefund_count,voi_comment_count," +
      "uid_samemobile,uid_haspaypwd,pro_ismarketing,ord_refund_flt_order_count,score"

// 这样生成的数据的格式全部是字符串 String,不利于后面的数据处理。
    val fields=schemaString.split(",").map(fieldName=>StructField(fieldName,StringType,true))
    val schema = StructType(fields)

    val noheader=myfile.filter(line=> !line.contains("uid_grade"))
    val myfile2=noheader.map(_.split(",")).map(p=>Row.fromSeq(p.toSeq))

//下面的方法生成的是全部是Double型的数据，便于后面的数据处理。

    val fields_double=schemaString.split(",").map(fieldName=>StructField(fieldName,DoubleType,true))
    val schema_double = StructType(fields)

    val noheader_double=myfile.filter(line=> !line.contains("uid_grade"))

    val myfile2_double=noheader.map(_.split(",")).map(p=>Row.fromSeq(p.map(x=>x.toDouble).toSeq))

    val df=sqlContext.createDataFrame(myfile2,schema)
    val df_double=sqlContext.createDataFrame(myfile2_double,schema_double)

    return df

  }

}
