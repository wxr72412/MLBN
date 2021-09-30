package BNLV_learning.Global

import org.apache.spark.rdd.RDD

/**
  * Created by Gao on 2016/8/30.
  */
object reference {                                      //全局变量对象
  var NodeNum: Int = _                                  //BN节点数
  var LatentNum: Int = _                                //隐变量节点数
  var NodeNum_Total: Int = _                                  //BN节点数
  var LatentNum_Total: Int = _                                //隐变量节点数
  var Latent_r = new Array[Int](LatentNum_Total)              //隐变量的势

  var inFile: RDD[String] = null
  var inFile_Data: RDD[String] = null

  var c:Int = _
  var SampleNum: Long = _                               //原始数据文档中的样本数,由EM计算得出
  var TestSampleNum: Long = _                           //测试数据文档中的样本数
  var Data: RDD[String] = null                    //样本RDD
  var MendedData: RDD[String] = null                    //补后样本RDD

  var MendedData_Num: Long = _                               //原始数据文档中的样本数,由EM计算得出


  /** 初始参数生成的三种方式: Random、WeakConstraint1、WeakConstraint3
    * ThetaGeneratedKind = 0 —— Random
    * ThetaGeneratedKind = 1 —— WeakConstraint1
    * ThetaGeneratedKind = 2 —— WeakConstraint3
    */
  var ThetaGeneratedKind = 0                         //模型初始参数的生成方式


//  var BIC = 0.01 //非增量 movielen-1m
  var BIC = 0.1 //非增量 clothing fit data

//  var BIC_latent = 0.26 //增量 chest_clinic
//  var BIC_latent = 0.01 //非增量 movielen-1m
  var BIC_latent = 0.001 //非增量 clothing fit data

  val BIC_data_number = 1.0

//  val cluster = "spark"
  val cluster = "local"

  var R_type = "1"
  var Family_type = "1"
  var BIC_type = "BIC+QBIC"
  var PD_log = Double.NegativeInfinity


  var EM_threshold:Double = _                            //EM收敛阈值
  var EM_threshold_origin = 0.001                            //EM收敛阈值
  val EM_threshold_chestclinic = 0.1

  var SEM_threshold:Double = 0.1                        //SEM收敛阈值
  var SEM_threshold_origin:Double = 0.00001                        //SEM收敛阈值
//  var SEM_threshold_origin = 0.00001                            //SEM收敛阈值

  val EM_iterationNum = 10                      //EM迭代次数或迭代次数上限
  val SEM_iteration = 10                          //SEM迭代次数或迭代次数上限
  val EM2_iterationNum = 1                      //子图合并时，EM迭代次数或迭代次数上限  chest_clinic 需要更改 到10
  val SEM2_iteration = 1                           //子图合并时，SEM迭代次数或迭代次数上限 chest_clinic 需要更改 到10


  val incremental_iteration = 10                      //增量学习迭代次数或迭代次数上限

//	var SubGraph_threshold = 0.0125                            //影响度ID阈值
  var SubGraph_threshold = 0.0125                           //影响度ID阈值

  var miu:Double = 0.0
  var sigma:Double = 2.0

  var inference_type = "MLBN"

  var data_type = "ml-1m"
//  var data_type = "Clothing-Fit-Data"
  var userNum: Int = _
  var usercount: Int = _
  var user_Attribution_num: Int = _

}
