package com.sundogsoftware.spark.csv

import com.sundogsoftware.spark._2_FriendsByAgeDataset.FakeFriends
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Date
import java.sql.Timestamp

object ReadCustomerCSV {
  case class Customer(
                       mbr_id: String,
                       mbr_mobile: String,
                       mini_prog_open_id_1: String,
                       mini_prog_open_id_2: String,
                       mini_prog_open_id_3: String,
                       mini_prog_open_id_4: String,
                       mini_prog_open_id_5: String,
                       mini_prog_open_id_6: String,
                       mini_prog_open_id_7: String,
                       mini_prog_open_id_8: String,
                       mini_prog_open_id_9: String,
                       mini_prog_open_id_10: String,
                       mp_open_id_1: String,
                       mp_open_id_2: String,
                       mp_open_id_3: String,
                       mp_open_id_4: String,
                       mp_open_id_5: String,
                       mp_open_id_6: String,
                       mp_open_id_7: String,
                       mp_open_id_8: String,
                       mp_open_id_9: String,
                       mp_open_id_10: String,
                       union_id: String,
                       nickname: String,
                       province: String,
                       city: String,
                       mbr_sex: String,
                       mbr_birth: String,
                       mbr_marriage_status: String,
                       brand_name: String,
                       regist_src: String,
                       referee_mbr_id: String,
                       mbr_shop_name: String,
                       regist_explain: String,
                       mbr_level_name: String,
                       mbr_state: String,
                       mbr_level_start_time: String,
                       mbr_level_stop_time: String,
                       mbr_point_cnt: String,
                       fav_cat_2_name_1y: String,
                       fav_cat_2_amt_1y: String,
                       snd_fav_cat_2_name_1y: String,
                       cat_2_cnt_1y: String,
                       regist_time: String,
                       last_login_time: String,
                       msg_create_time: String,
                       msg_update_time: String,
                       ts: String,
                       dt: String
                     )

  case class CustomerOrder(
                            sku_cnt: String,
                            item_number: String,
                            deduct_point_cnt: String,
                            goods_amount: String,
                            actual_pay_amt: String,
                            deduct_point_amt: String,
                            is_contain_gift: String,
                            is_combination_ord: String,
                            is_distr_ord: String,
                            is_paid_mbr: String,
                            is_return: String,
                            is_exchange: String,
                            is_problem: String,
                            id: String,
                            orig_ord_code: String,
                            buyer_number_id: String,
                            buyer_number_id_type: String,
                            buyer_nick_name: String,
                            receiver_province_name: String,
                            receiver_city_name: String,
                            receiver_area_name: String,
                            receiver_name: String,
                            receiver_phone: String,
                            receiver_mobile: String,
                            prom_name: String,
                            coupon_code: String,
                            coupon_name: String,
                            ord_status: String,
                            ord_channel: String,
                            ord_type: String,
                            presell_type: String,
                            shop_id: String,
                            shop_name: String,
                            problem_type: String,
                            problem_desc: String,
                            invalid_reason: String,
                            invalid_time: String,
                            ord_create_time: String,
                            pay_time_last: String,
                            confirm_time: String,
                            c_year: String,
                            c_month: String,
                            c_day: String,
                            ts: String,
                            dt: Date,
                          )


  case class Employee(empno: String, ename: String, designation: String, manager: String, hire_date: String, sal: String, deptno: String)

  def main(args: Array[String]): Unit = {

    // Create a SparkSession using every core of the local machine
    val spark = SparkSession
      .builder
      .appName("quanmian")
      .master("local[*]")
      .getOrCreate()

    // Load each line of the source data into an Dataset
    import spark.implicits._
    val ds = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/quanmian_customer.csv")
      .as[Customer]

    // Select only age and numFriends columns
    val friendsByAge = ds.select("mbr_id", "mbr_mobile", "last_login_time", "ts", "dt")
    friendsByAge.show()

  }
}
