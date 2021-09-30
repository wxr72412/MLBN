package BNLV_learning.Inference

import java.io.FileWriter
import java.text.NumberFormat
import java.util.Date

import BNLV_learning.BayesianNetwork
import BNLV_learning.Global._
import BNLV_learning.Inference.BN_Inference.Enumeration_Process
import BNLV_learning.Input.BN_Input
import BNLV_learning.Learning.OtherFunction
import BNLV_learning.Output.BN_Output
import breeze.numerics.{pow, sqrt}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


/**
  * Created by Gao on 2017/4/15.
  */
class InferenceVariable extends Serializable {       //与推理相关的变量格式
    var sequence: Int = _                               //变量的序号
    var value: Int = _                                  //变量的取值
}

class Posterior extends Serializable {               //变0量的后验概率格式
    var value: Int = _                                  //变量取值
    var values = new Array[Int](1)                                  //变量取值
    var probability: Double = _                         //后验概率值
}

class InferenceResult(val QueryVariable_Cardinality: Int) extends Serializable {         //推理结果格式——userID,QueryResult
    //var QueryVariable_Cardinality: Int = _            //查询变量的势
    var userID: String = _                              //用户id
    val QueryResult = new Array[Posterior](QueryVariable_Cardinality)   //存储所有查询变量取值对应的后验概率，QueryResult(i).probability = P(QueryVariable.value = i+1| Evidences)
}

class PreferenceBuffer {         //Top-k用户偏好格式——userID,前k个item类型偏好
    var userID: String = _                              //用户id
    val preference = new ArrayBuffer[Int]               //存储该用户的前k个item类型偏好
}

class TestCriterion_1 {         //模型测试1指标格式
    var recall: Double = _                              //召回率
    var precision: Double = _                           //准确率
    var F_score: Double = _                             //F-score
}

class TestCriterion_2 {         //模型测试2指标格式
    var MAE: Double = _                              //平均绝对误差
    var RMSE: Double = _                             //均方根误差
}

object BN_Inference {

    val nf = NumberFormat.getNumberInstance()
    nf.setMaximumFractionDigits(4)                       //设置输出格式保留小数位数

    //  /** 根据证据变量与查询变量进行一次BN推理的过程 */
    //  def inferenceProcess(bn: BayesianNetwork) {
    //
    //    val t_start = new Date()                                  //获取起始时间对象
    //    val input = new BN_Input
    //    val output = new BN_Output
    //
    //    var outLog = new FileWriter(logFile.inference_log)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
    //    outLog.write("BNLV模型推理日志\r\n")
    //    outLog.write("BNLV结构：\r\n")
    //    outLog.close()
    //
    //    input.structureFromFile(inputFile.inference_structure, separator.structure, bn)  //从文件中读取表示BN结构的邻接矩阵到bn.structure中
    //    output.structureAppendFile(bn, logFile.inference_log)
    //
    //    bn.qi_computation()                          //计算BN中每个变量其父节点的组合情况数qi
    //    bn.create_CPT()                              //创建CPT
    //    input.CPT_FromFile(inputFile.inference_theta, separator.theta, bn)      //从文件中读取参数θ到动态三维数组bn.theta
    //
    //    outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
    //    outLog.write("BNLV参数CPT:\r\n")
    //    outLog.close()
    //    output.CPT_FormatAppendFile(bn, logFile.inference_log)
    //
    //    val EvidenceNum = 3                                          //证据变量个数
    //    val Evidences = new Array[InferenceVariable](EvidenceNum)    //创建一个证据变量数组存储每个证据变量的序号及其取值
    //    for (i <- 0 until EvidenceNum)                                //创建数组中每个对象
    //      Evidences(i) =  new InferenceVariable
    //
    //    val QueryVariable = new InferenceVariable                     //查询变量
    //
    //    //设置证据变量与查询变量
    //    Evidences(0).sequence = 1; Evidences(0).value = 1
    //    Evidences(1).sequence = 2; Evidences(1).value = 1
    //    Evidences(2).sequence = 3; Evidences(2).value = 3
    //    QueryVariable.sequence = 4;  //给定查询变量的序号
    //
    //    outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
    //    outLog.write("证据变量: ")
    //    for (i <- 0 until (EvidenceNum - 1) )
    //      outLog.write("V" + Evidences(i).sequence + " = " + Evidences(0).value + ", ")
    //    outLog.write("V" + Evidences(EvidenceNum - 1).sequence + " = " + Evidences(EvidenceNum - 1).value + "\r\n")
    //    outLog.write("查询变量: V" + QueryVariable.sequence + "\r\n\r\n")
    //    outLog.close()
    //
    //    val QueryResult = new Array[Posterior](bn.r(QueryVariable.sequence - 1) )   //存储所有查询变量取值对应的后验概率
    //    for (i <- 0 until bn.r(QueryVariable.sequence - 1) )                        //创建数组中每个对象
    //      QueryResult(i) =  new Posterior
    //
    //    for (i <- 1 to bn.r(QueryVariable.sequence - 1) ) {                      //依次计算不同查询变量取值对应的后验概率
    //      QueryVariable.value = i
    //      QueryResult(i - 1).value  = QueryVariable.value
    //      QueryResult(i - 1).probability = Enumeration_Process(bn, Evidences, EvidenceNum, QueryVariable)
    //    }
    //    val sorted_QueryResult =  QueryResult.sortWith(_.probability > _.probability)  //sorted_QueryResult存储所有查询变量取值对应的后验概率,按其概率值进行排序
    //
    //    val t_end = new Date()                              //getTime获取得到的数值单位：毫秒数
    //    val runtime = t_end.getTime - t_start.getTime
    //    outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
    //    outLog.write("推理结果：\r\n")
    //    for (i <- 1 to bn.r(QueryVariable.sequence - 1) )
    //      outLog.write("查询变量 = "+ i + " ： " + QueryResult(i - 1).probability + "\r\n")
    //
    //    outLog.write("\r\nTopN-推理结果：\r\n")
    //    for (i <- 1 to bn.r(QueryVariable.sequence - 1) )
    //      outLog.write("查询变量 = "+ sorted_QueryResult(i - 1).value + " ： " + sorted_QueryResult(i - 1).probability + "\r\n")
    //
    //    outLog.write("\r\nBNLV模型推理时间: " + runtime + " 毫秒\r\n\r\n")
    //    outLog.close()
    //
    //    println("参数学习完成!")
    //  }

    /** 根据样本自动进行BN推理的过程 */
    def PerferenceInferenceProcess(bn: BayesianNetwork) {

        val t_start = new Date()                                  //获取起始时间对象
        val input = new BN_Input
        val output = new BN_Output

        var outLog = new FileWriter(logFile.inference_log)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.write("BNLV模型推理日志\r\n")
        outLog.write("训练数据集: " + inputFile.inference_learning_Data + "\r\n")
        outLog.write("测试数据集: " + inputFile.inference_test_Data + "\r\n")
        outLog.write("BNLV结构：\r\n")
        outLog.close()

        input.structureFromFile(inputFile.inference_structure, separator.structure, bn)  //从文件中读取表示BN结构的邻接矩阵到bn.structure中
        output.structureAppendFile(bn, logFile.inference_log)

        bn.qi_computation()                          //计算BN中每个变量其父节点的组合情况数qi
        bn.create_CPT()                              //创建CPT
        input.CPT_FromFile(inputFile.inference_theta, separator.theta, bn)      //从文件中读取参数θ到动态三维数组bn.theta

        /*
			var ThetaNum = 0                                        //期望统计量或参数的总数量
			for (i <- 0 until bn.NodeNum)
			  ThetaNum += bn.q(i) * bn.r(i)
			val Theta_ijk = new Array[Double](ThetaNum)             //模型参数顺序表——类似M_ijk表
			//读取文件形式存储的BN对象为初始模型
			val bnFromFile = input.ObjectFromFile(intermediate.BN_object)
			if (OtherFunction.isEqualGraph(bn.structure, bnFromFile.structure, bn.NodeNum)){  //若bn与bnFromFile的结构相等，则将bnFromFile的CPT赋给bn
			  bnFromFile.CPT_to_Theta_ijk(Theta_ijk)
			  bn.Theta_ijk_to_CPT(Theta_ijk)
			}
			else {
			  println("当前BN结构与文件对象的结构不一致")
			  System.exit(0)
			}
			*/


        //    outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        //    outLog.write("BNLV参数CPT:\r\n")
        //    outLog.close()
        //    output.CPT_FormatAppendFile(bn, logFile.inference_log)


        /********************更换训练数据集一定要记得修改EvidenceNum与Evidences*****************/
        val EvidenceNum = 3                                          //证据变量个数
        val Evidences = new Array[InferenceVariable](EvidenceNum)    //创建一个证据变量数组存储每个证据变量的序号及其取值
        for (i <- 0 until EvidenceNum)                                //创建数组中每个对象
            Evidences(i) =  new InferenceVariable
        val QueryVariable = new InferenceVariable                     //查询变量

        //println("请输入证据变量与查询变量")
        /*
		Evidences(0).sequence = 1; //Evidences(0).value = 1
		Evidences(1).sequence = 3; //Evidences(1).value = 1
		Evidences(2).sequence = 4; //Evidences(2).value = 3
		QueryVariable.sequence = 5;  //给定查询变量的序号
		*/
        // 按策略推理
//        Evidences(0).sequence = 1; //U1
//        Evidences(1).sequence = 2; //U2
//        Evidences(2).sequence = 3; //U3
//        Evidences(3).sequence = 4; //R
//        Evidences(4).sequence = 5; //I1
//        Evidences(5).sequence = 6; //I2
//        QueryVariable.sequence = 7;  //L1
        // 直接推理
		Evidences(0).sequence = 1; //U1
		Evidences(1).sequence = 2; //U2
		Evidences(2).sequence = 3; //U3
		//  QueryVariable.sequence = 7;  //L1
		QueryVariable.sequence = 6;  //I1
		println("=====================训练集  推理=====================")
        val inFile = SparkConf.sc.textFile(inputFile.inference_learning_Data, SparkConf.partitionNum)  //读取原始数据为RDD
        reference.SampleNum = inFile.count()                         //原始数据文档中的样本数
        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("训练数据集 : " + reference.SampleNum + "\r\n\r\n")
        outLog.close()

        val sampleInferenceRDD = inFile.map {    //将每行原始样本映射处理为相应的推理结果
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
                val lineStringArray = line.split(separator.data)
                val lineIntArray = lineStringArray map ( _.toInt )
                for (i <- 0 until EvidenceNum)
                    Evidences(i).value = lineIntArray(Evidences(i).sequence)

                //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)
                val SampleQueryResult = new InferenceResult(bn.r(QueryVariable.sequence - 1))
                SampleQueryResult.userID = lineStringArray(0)
//                println(SampleQueryResult.userID)
                for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
                    SampleQueryResult.QueryResult(i) =  new Posterior
                //        println("============userID============")
                //        println(SampleQueryResult.userID)
                //        println("============Evidence============")
                //        for (i <- 0 until EvidenceNum)
                //          println(Evidences(i).value)
                //        System.exit(0)
                val QueryVariable_probability = new Array[Double](bn.r(QueryVariable.sequence - 1)) //存放查询变量的联合概率  P(Q=q,E)
                Enumeration_Process(bn, Evidences, EvidenceNum, QueryVariable, QueryVariable_probability, bn.r(QueryVariable.sequence - 1))
                val sum = QueryVariable_probability.sum
                for (i <- 1 to bn.r(QueryVariable.sequence - 1) ) {                      //依次计算不同查询变量取值对应的后验概率
                    QueryVariable.value = i
                    SampleQueryResult.QueryResult(i - 1).value  = QueryVariable.value
                    SampleQueryResult.QueryResult(i - 1).probability = QueryVariable_probability(i-1) / sum
                }
                SampleQueryResult
        }
        //SampleQueryResult存储每行样本对应的推理结果——各用户偏好属性值对应的后验概率
        var SampleQueryResult = sampleInferenceRDD.collect()    //执行action，将数据提取到Driver端。
//            println("============SampleQueryResult============")
//            println(SampleQueryResult.length)
//            for (q <- SampleQueryResult){
//              println("============userID============")
//              println(q.userID)
//              println("============QueryResult============")
//              for (qq <- q.QueryResult){
//                  println(qq.value + " " + qq.probability)
//              }
//            }
		println("=====================训练集  推理  按userID排序=====================")
        SampleQueryResult = SampleQueryResult.sortWith(_.userID.toInt < _.userID.toInt)
//            println("============SampleQueryResult============")
//            println(SampleQueryResult.length)
//            for (i <- 0 to 10){
//              println("============userID============")
//                val q = SampleQueryResult(i)
//              println(q.userID)
//              println("============QueryResult============")
//              for (qq <- q.QueryResult){
//                  println(qq.value + " " + qq.probability)
//              }
//            }
//            System.exit(0)

        val weight_QueryResult = new Array[Posterior](bn.r(QueryVariable.sequence - 1) )   //存储所有查询变量各取值对应的加权后验概率，weight_QueryResult(i).probability = 评分频率 * P(QueryVariable.value = i+1| Evidences)
        for (i <- 0 until bn.r(QueryVariable.sequence - 1) ) {
            weight_QueryResult(i) =  new Posterior
            weight_QueryResult(i).value = i + 1
            weight_QueryResult(i).probability = 0.0
        }

        //outLog = new FileWriter(logFile.inference, true)    //以追加写入的方式创建FileWriter对象
        //outLog.write("推理结果1：\r\n")

        /** 数组缓冲版本 */
        val PreferenceResult = new ArrayBuffer[InferenceResult]        //存储每个用户的加权偏好程度(p1, p2, ...)
        var current_UserID = SampleQueryResult(0).userID                //记录当前进行偏好程度计算的用户
        var UserNum = 1              //样本中的用户数
        var userRatingTimes = 0      //记录一个用户的评分行为次数
        for (i <- 0 until reference.SampleNum.toInt) {
            if (SampleQueryResult(i).userID == current_UserID) {
                userRatingTimes += 1
                for (j <- 0 until bn.r(QueryVariable.sequence - 1) )
                    weight_QueryResult(j).probability += SampleQueryResult(i).QueryResult(j).probability
            }
            else {  //SampleQueryResult(i).userID != current_UserID——user_id发生变化
                for (j <- 0 until bn.r(QueryVariable.sequence - 1))                     //计算上一个用户的加权偏好
                    weight_QueryResult(j).probability /= userRatingTimes

                val UserPreference = new InferenceResult(bn.r(QueryVariable.sequence - 1))  //存储用户的加权偏好程度(p1, p2, ...)
                for (k <- 0 until UserPreference.QueryVariable_Cardinality) {
                    UserPreference.QueryResult(k) = new Posterior
                    UserPreference.QueryResult(k).value = k + 1
                    UserPreference.QueryResult(k).probability = 0.0
                }
                UserPreference.userID = SampleQueryResult(i - 1).userID
                for (k <- 0 until UserPreference.QueryVariable_Cardinality)
                    UserPreference.QueryResult(k).probability = weight_QueryResult(k).probability
                PreferenceResult += UserPreference

                current_UserID = SampleQueryResult(i).userID
                UserNum += 1
                userRatingTimes = 1
                for (j <- 0 until bn.r(QueryVariable.sequence - 1) ) {
                    weight_QueryResult(j).probability = 0.0      //重置查询变量各取值对应的加权后验概率值
                    weight_QueryResult(j).probability += SampleQueryResult(i).QueryResult(j).probability
                }
            }
            if (i == (reference.SampleNum.toInt - 1) ) {
                for (j <- 0 until bn.r(QueryVariable.sequence - 1))
                    weight_QueryResult(j).probability /= userRatingTimes

                val UserPreference = new InferenceResult(bn.r(QueryVariable.sequence - 1))  //存储用户的加权偏好程度(p1, p2, ...)
                for (k <- 0 until UserPreference.QueryVariable_Cardinality) {
                    UserPreference.QueryResult(k) = new Posterior
                    UserPreference.QueryResult(k).value = k + 1
                    UserPreference.QueryResult(k).probability = 0.0
                }
                UserPreference.userID = SampleQueryResult(i).userID
                for (k <- 0 until UserPreference.QueryVariable_Cardinality)
                    UserPreference.QueryResult(k).probability = weight_QueryResult(k).probability
                PreferenceResult += UserPreference
            }
        }

        val sorted_PreferenceResult = new Array[InferenceResult](UserNum)  //存储每个用户的加权偏好程度(p1, p2, ...),按程度从大到小排序
        for (i <- 0 until UserNum) {                                       //创建数组中每个对象
            sorted_PreferenceResult(i) = new InferenceResult(bn.r(QueryVariable.sequence - 1))
            for (j <- 0 until PreferenceResult(i).QueryVariable_Cardinality) {
                sorted_PreferenceResult(i).QueryResult(j) = new Posterior
                sorted_PreferenceResult(i).QueryResult(j).value = j + 1
                sorted_PreferenceResult(i).QueryResult(j).probability = 0.0
            }
        }
		println("=====================训练集  按概率大小排序=====================")
        for (i <- 0 until UserNum) {
            sorted_PreferenceResult(i).userID = PreferenceResult(i).userID
            val sorted_QueryResult =  PreferenceResult(i).QueryResult.sortWith(_.probability > _.probability)  //sorted_QueryResult存储所有查询变量取值对应的后验概率,按其概率值进行排序
            for (j <- 0 until bn.r(QueryVariable.sequence - 1) ) {
                sorted_PreferenceResult(i).QueryResult(j).value = sorted_QueryResult(j).value
                sorted_PreferenceResult(i).QueryResult(j).probability = sorted_QueryResult(j).probability
            }
        }
        //    println("============sorted_PreferenceResult============")
        //    println(sorted_PreferenceResult.length)
        //    for (q <- sorted_PreferenceResult){
        //        println("============userID============")
        //        println(q.userID)
        //        println("============QueryResult============")
        //        for (qq <- q.QueryResult){
        //            println(qq.value + " " + qq.probability)
        //        }
        //    }
        //    System.exit(0)

        val t_end = new Date()                              //getTime获取得到的数值单位：毫秒数
        val runtime = (t_end.getTime - t_start.getTime)/1000.0

        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("\r\nBNLV模型推理时间: " + runtime + " 秒\r\n\r\n")
        outLog.write("trainingSet_userNum: " + UserNum + " \r\n\r\n")
        outLog.close()
		println("=====================测试集  推理=====================")
        /** 模型测试指标1 */
        model_evaluation_1(UserNum, inputFile.inference_test_Data, sorted_PreferenceResult, bn.r(QueryVariable.sequence - 1))


        /** 输出每个用户相应的偏好类型及其概率值——已按概率值进行降序排序 */
//        var outResult = new FileWriter(outputFile.SamplePreferenceResult)    //以非追加写入的方式创建FileWriter对象
        //    for (i <- 0 until UserNum) {
        //      outResult.write(sorted_PreferenceResult(i).userID)
        //      for (j <- 0 until bn.r(QueryVariable.sequence - 1) )
        //        outResult.write( "," + sorted_PreferenceResult(i).QueryResult(j).value + "(" + sorted_PreferenceResult(i).QueryResult(j).probability + ")" )
        //      outResult.write("\r\n")
        //    }
        //    outResult.close()

        var outResult = new FileWriter(outputFile.PreferenceResult_Learning)    //以非追加写入的方式创建FileWriter对象
        for (i <- 0 until UserNum) {
            outResult.write(sorted_PreferenceResult(i).userID)
            for (j <- 0 until bn.r(QueryVariable.sequence - 1) )
                outResult.write( "," + sorted_PreferenceResult(i).QueryResult(j).value + "(" + nf.format(sorted_PreferenceResult(i).QueryResult(j).probability) + ")" )
            outResult.write("\r\n")
        }
        outResult.close()

        println("偏好推理完成!")
    }

    def ratingInferenceProcess(bn: BayesianNetwork): Unit = {

        val input = new BN_Input
        val output = new BN_Output

        var outLog = new FileWriter(logFile.inference_log)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.write("BNLV模型推理日志\r\n")
        outLog.write("测试数据集: " + inputFile.inference_test_Data + "\r\n")
        outLog.write("BNLV结构：\r\n")
        outLog.close()
        outLog = new FileWriter(logFile.inference_temporary)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.write("推理结果\r\n")
        outLog.close()

        input.structureFromFile(inputFile.inference_structure, separator.structure, bn)  //从文件中读取表示BN结构的邻接矩阵到bn.structure中
        output.structureAppendFile(bn, logFile.inference_log)
        bn.qi_computation()                          //计算BN中每个变量其父节点的组合情况数qi
        bn.create_CPT()                              //创建CPT
        input.CPT_FromFile(inputFile.inference_theta, separator.theta, bn)      //从文件中读取参数θ到动态三维数组bn.theta

//        var flag = 0
//        for (i <- 0 until bn.NodeNum)
//            for (j <- 0 until bn.q(i)) {
//                flag = 0
//                for (k <- 0 until bn.r(i)) {
//                    if(bn.theta(i)(j)(k) == 0)
//                        flag = 1
//                }
//                if(flag == 1){
//                    println((i+1)+" "+ (j+1))
//                    for (k <- 0 until bn.r(i)) {
//                        print(bn.theta(i)(j)(k) +  " ")
//                    }
//                    println()
//                }
//            }
//        System.exit(0)
        /** 模型测试指标2 */
        //        model_evaluation_2(bn, inputFile.inference_test_Data, "mean")
        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("R max" + "\r\n")
        outLog.close()
        model_evaluation_R(bn, inputFile.inference_test_Data, "max")

        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("R mean" + "\r\n")
        outLog.close()
        model_evaluation_R(bn, inputFile.inference_test_Data, "mean")

        println("评分推理完成！")
    }

    def ratingInferenceProcess_strategy(bn: BayesianNetwork): Unit = {

        val input = new BN_Input
        val output = new BN_Output

        var outLog = new FileWriter(logFile.inference_log)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.write("BNLV模型推理日志\r\n")
        outLog.write("测试数据集: " + inputFile.inference_test_Data + "\r\n")
        outLog.write("BNLV结构：\r\n")
        outLog.close()
        outLog = new FileWriter(logFile.inference_temporary)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.write("推理结果\r\n")
        outLog.close()

        /** 模型测试指标2 */
        //        model_evaluation_2(bn, inputFile.inference_test_Data, "mean")
        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("R max" + "\r\n")
        outLog.close()
        model_evaluation_R_strategy(bn, inputFile.inference_test_Data, "max")

//        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
//        outLog.write("R mean" + "\r\n")
//        outLog.close()
//        model_evaluation_R_strategy(bn, inputFile.inference_test_Data, "mean")
//
//        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
//        outLog.write("R mean.round" + "\r\n")
//        outLog.close()
//        model_evaluation_R_strategy(bn, inputFile.inference_test_Data, "mean.round")

        println("评分推理完成！")
    }

    def InferenceProcess(bn: BayesianNetwork) {
        println("推理1")
        val input = new BN_Input
        val output = new BN_Output
        input.structureFromFile(inputFile.inference_structure, separator.structure, bn)  //从文件中读取表示BN结构的邻接矩阵到bn.structure中
        output.structureAppendFile(bn, logFile.inference_log)
        bn.qi_computation()                          //计算BN中每个变量其父节点的组合情况数qi
        bn.create_CPT()                              //创建CPT
        input.CPT_FromFile(inputFile.inference_theta, separator.theta, bn)      //从文件中读取参数θ到动态三维数组bn.theta


        var outLog = new FileWriter(logFile.inference_temporary)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.write("推理结果\r\n")
        outLog.close()
        println("推理2")

        /** ******************更换【测试数据集】一定要记得修改EvidenceNum与Evidences *****************/
        val EvidenceNum = 5 //证据变量个数
        val Evidences = new Array[InferenceVariable](EvidenceNum) //创建一个证据变量数组存储每个证据变量的序号及其取值
        for (i <- 0 until EvidenceNum) //创建数组中每个对象
            Evidences(i) = new InferenceVariable
        val QueryVariableNum = 2 //查询变量个数
        val QueryVariable = new Array[InferenceVariable](QueryVariableNum) //创建一个证据变量数组存储每个查询变量的序号及其取值
        for (i <- 0 until QueryVariableNum) //创建数组中每个对象
            QueryVariable(i) = new InferenceVariable
//////////////////////////////////////////////////////////////////////
//        Evidences(0).sequence = 1; //U1
//        Evidences(1).sequence = 2; //U2
//		Evidences(2).sequence = 3; //U3
//        Evidences(3).sequence = 5; //I1
//        //        Evidences(4).sequence = 6; //I2
//        QueryVariable.sequence = 4; //R 评分值变量的序号
        //////////////////////////////////////////////////////////////////////
        Evidences(0).sequence = 1; //U1
        Evidences(1).sequence = 2; //U2
        Evidences(2).sequence = 3; //U3
        Evidences(3).sequence = 5; //I1
        Evidences(4).sequence = 8; //I2
        QueryVariable(0).sequence = 4; //R1
        QueryVariable(1).sequence = 7; //R2
        //////////////////////////////////////////////////////////////////////
//        Evidences(0).sequence = 1; //U1
//        Evidences(1).sequence = 2; //U2
//        Evidences(2).sequence = 3; //U3
//        Evidences(3).sequence = 6; //L1
//        QueryVariable.sequence = 5; //I1
        //////////////////////////////////////////////////////////////////////
//        Evidences(0).sequence = 1; //U1
//        Evidences(1).sequence = 2; //U2
//        Evidences(2).sequence = 3; //U3
//        Evidences(3).sequence = 5; //I1
//        Evidences(4).sequence = 6; //L1
//        QueryVariable.sequence = 4; //R
        //////////////////////////////////////////////////////////////////////
        println("推理3")
        val inFile = SparkConf.sc.textFile(inputFile.InferenceProcess_Data, SparkConf.partitionNum) //读取测试数据为RDD对象
        reference.TestSampleNum = inFile.count() //测试数据文档中的样本数
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // 填充数据
        val Ln = QueryVariableNum
        val Lr = new Array[Int](Ln)
        for(i <- 0 until Ln){
            Lr(i) = bn.r(QueryVariable(i).sequence-1)
        }
        var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
        for(i <- 0 until Ln){
            c *= Lr(i)
        }

        var Lx = Array.ofDim[Int](Ln + 1)
        val MendedQueryVariable = new Array[Array[Int]](c)
        for (i <- 0 until c) {
            MendedQueryVariable(i) = new Array[Int](Ln + 1)
        }
        var i = 0
        while (Lx(0) == 0) {
            Lx.copyToArray(MendedQueryVariable(i))
            i+= 1
            Lx(Ln) += 1
            for (i <- 0 until Ln) { //0,1,2,....,Ln-1
                if (Lx(Ln - i) == Lr(Ln - i - 1)) {
                    Lx(Ln - i) = 0
                    Lx(Ln - i - 1) += 1
                }
            }
        }

        for (i <- 0 until c) {
            for (j <- 0 until MendedQueryVariable(i).length) {
                MendedQueryVariable(i)(j) += 1
            }
        }
//        System.exit(0)
//        println("推理4")
        val sampleInferenceRDD = inFile.map { //将每行原始样本映射处理为相应的推理结果
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
                val lineStringArray = line.split(separator.data)
                val lineIntArray = lineStringArray map (_.toInt)

                //                println(line)
                for (i <- 0 until EvidenceNum) {
                    Evidences(i).value = lineIntArray(Evidences(i).sequence)
                    //                    print(i + " " + Evidences(i).value + "   ")
                }
                //                println()
                //                System.exit(0)
                //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)
                val SampleQueryResult = new InferenceResult(c)
                SampleQueryResult.userID = lineStringArray(0)
                for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
                    SampleQueryResult.QueryResult(i) = new Posterior

                val QueryVariable_probability = new Array[Double](c) //存放查询变量的联合概率  P(Q=q,E)
//                println("c: " + c)
//                println("Evidences: ")
//                for (i <- 0 until EvidenceNum)
//                    println(Evidences(i).sequence + " " + Evidences(i).value)
//                println("QueryVariable: ")
//                for (i <- 0 until QueryVariableNum)
//                    println(QueryVariable(i).sequence + " " + QueryVariable(i).value)
//                println("MendedQueryVariable: ")
//                for (i <- 0 until c) {
//                    for (j <- 0 until MendedQueryVariable(i).length) {
//                        print(MendedQueryVariable(i)(j) + " ")
//                    }
//                    println()
//                }
//                System.exit(0)
                Enumeration_Process1(bn, Evidences, EvidenceNum, QueryVariable, QueryVariableNum, QueryVariable_probability, c, MendedQueryVariable)
//                println()
//                for (i <- 0 until c) {
//                    println(QueryVariable_probability(i))
//                }
//                System.exit(0)
                val sum = QueryVariable_probability.sum
                for (i <- 0 until c) { //依次计算不同查询变量取值对应的后验概率
                    SampleQueryResult.QueryResult(i).values = new Array[Int](QueryVariableNum+1)
                    MendedQueryVariable(i).copyToArray(SampleQueryResult.QueryResult(i).values)
                    SampleQueryResult.QueryResult(i).probability = QueryVariable_probability(i) / sum
//                    for(j <- 1 until  QueryVariableNum+1){
//                        println(SampleQueryResult.QueryResult(i).values(j))
//                    }
//                    println(SampleQueryResult.QueryResult(i).probability)
                }
                (SampleQueryResult, line) //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
        }

        println("推理5")
        val testSet_PredictedResultRDD = sampleInferenceRDD.map {   //将每行样本推理结果映射处理为(predicted_RatingValue, real_RatingValue)
            line =>  //line存储每行测试样本数据映射的(SampleQueryResult, real_RatingValue)
                var predicted_RatingValue = 0.0                             //基于本条样本推理计算获得的评分预测值
                val outLog = new FileWriter(logFile.inference_temporary, true)    //以追加写入的方式创建FileWriter对象
                outLog.write(line._2)
                outLog.write("\r\n")
                // 不排序
                var sorted_QueryResult =  line._1.QueryResult
                for (j <- 0 until c ) {
                    outLog.write("::")
                    for (k <- 1 until  QueryVariableNum+1) //创建数组中每个对象
                        outLog.write("::" + sorted_QueryResult(j).values(k) + " ")
                    outLog.write(" " + sorted_QueryResult(j).probability)
                }
                outLog.write("\r\n")
                // 不排序 排序
                sorted_QueryResult =  sorted_QueryResult.sortWith(_.probability > _.probability)  //sorted_QueryResult存储所有查询变量取值对应的后验概率,按其概率值进行排序
                for (j <- 0 until c ) {
                    outLog.write("::")
                    for (k <- 1 until  QueryVariableNum+1) //创建数组中每个对象
                        outLog.write("::" + sorted_QueryResult(j).values(k) + " ")
                    outLog.write(" " + sorted_QueryResult(j).probability)
//                    outLog.write("::" + sorted_QueryResult(j).value + " " + sorted_QueryResult(j).probability)
                }
                outLog.write("\r\n")
                outLog.close()
        }
        println("推理6")
        val testSet_PredictedResult = testSet_PredictedResultRDD.collect()            //执行action，将数据提取到Driver端。
        println("推理7")
    }




    /** 根据样本推理结果计算召回率、精确率以及F-score进行模型评估 */
    def model_evaluation_1(trainingSet_userNum: Int, TestData: String, sorted_PreferenceResult: Array[InferenceResult], QueryVariable_r: Int) {

        val inFile = SparkConf.sc.textFile(TestData, SparkConf.partitionNum)     //读取测试数据为RDD对象
        reference.TestSampleNum = inFile.count()                                 //测试数据文档中的样本数
        val outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("测试数据集 : " + reference.TestSampleNum + "\r\n\r\n")
        outLog.close()
        val testSet_pairRDD = inFile.map {    //将每行测试样本映射处理为元组（user_id，item类型(整数形式)）
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                val lineStringArray = line.split(separator.data)
                //lineStringArray(0) = user_id, lineStringArray(1) = item类型，lineStringArray(2) = item评分值，lineStringArray(3) = 时间戳
                //(lineStringArray(0).toInt, lineStringArray(1).toInt)   //（user_id(整数形式)，item类型(整数形式)）,整型user_id便于使用sortByKey()按user_id进行排序

                /********************更换数据集一定要记得修改*****************/
                //lineStringArray(0) = user_id, lineStringArray(1) = U1，lineStringArray(2) = U2，lineStringArray(3) = item类型, lineStringArray(4) = item评分值, lineStringArray(5) = U3
                (lineStringArray(0).toInt, (lineStringArray(5).toInt, lineStringArray(4).toInt))   //（user_id(整数形式)，item类型(整数形式)）,整型user_id便于使用sortByKey()按user_id进行排序
        }
        //    println("============testSet_pairRDD============")
        //    for (q <- testSet_pairRDD){
        //      println(q)
        //    }
        //      ============testSet_pairRDD============
        //      (1,(3,3))
        //      (3,(5,5))
        //      (3,(5,5))
        //      (1,(12,5))
        //      (1,(17,4))
        //      (1,(1,5))
        //      (1,(2,1))
        //      (1,(3,5))
        //      (1,(4,1))
        //      (2,(1,5))
        //      (2,(7,4))
        val testSetRDD =  testSet_pairRDD.groupByKey()                           //按user_id将该用户所评价的item类型汇集到一起——(user_id, [item类型1，item类型2, ..., item类型n])
        //    println("============testSetRDD============")
        //    for (q <- testSetRDD){
        //      println(q)
        //    }
        //      ============testSetRDD============
        //      (3,CompactBuffer((5,5), (5,5)))
        //      (2,CompactBuffer((1,5), (7,4)))
        //      (1,CompactBuffer((3,3), (12,5), (17,4), (1,5), (2,1), (3,5), (4,1)))

        val test_Preference = testSetRDD.map {
            line =>
                val Array1 = line._2.toArray
                val Array2 = new Array[Double](QueryVariable_r)
//                 加权平均
                val user_rating_num = Array1.length
                for(a <- Array1){
///////////////////////////////////////////////////////////////////
//                    if(a._2 == 5)
//                        Array2(a._1-1) += a._2 + 0.2
//                    else if(a._2 == 4)
//                        Array2(a._1-1) += a._2 + 0.1
//                    else if(a._2 == 3)
//                        Array2(a._1-1) += a._2
//                    else if(a._2 == 2)
//                        Array2(a._1-1) += a._2 - 0.1
//                    else if(a._2 == 1)
//                        Array2(a._1-1) += a._2 - 0.2
///////////////////////////////////////////////////////////////////
                    Array2(a._1-1) += a._2
/////////////////////////////////////////////////////////////////////
                }
//                for(i <- 0 until QueryVariable_r){
//                    Array2(i) /= user_rating_num
//                }
                // 偏好
//                for(a <- Array1){
//                    if(a._2 == 5)
//                        Array2(a._1-1) = Array2(a._1-1) + 5
//                    else if(a._2 == 4)
//                        Array2(a._1-1) = Array2(a._1-1) + 4
//                    else if(a._2 == 3)
//                        Array2(a._1-1) = Array2(a._1-1) + 3
//                    else if(a._2 == 2)
//                        Array2(a._1-1) = Array2(a._1-1) - 1
//                    else if(a._2 == 1)
//                        Array2(a._1-1) = Array2(a._1-1) - 2
//                }
                // 平均分
//                val Array3 = new Array[Int](QueryVariable_r)
//                for(a <- Array1){
//                  Array3(a._1-1) += 1
//                  Array2(a._1-1) += a._2
//                }
//                for(i <- 0 until QueryVariable_r){
//                    if(Array3(i) == 0)
//                        Array2(i) = 0
//                    else
//                        Array2(i) /= Array3(i)
//                }
//                // 评分频率
//                for(a <- Array1){
//                  Array2(a._1-1) += 1.0
//                }
                // 排序
                var Array4 = new Array[(Int,Double)](QueryVariable_r)  //存储用户的加权偏好程度
                for (i <- 0 until QueryVariable_r) {
                    Array4(i) = (i+1,Array2(i))
                }
                Array4 = Array4.sortWith(_._2 > _._2)  //sorted_QueryResult存储所有查询变量取值对应的后验概率,按其概率值进行排序
//                println(line._1 + " 评分数:" + Array1.length)
                (line._1, Array4, Array1.length)
        }
//            println("============test_Preference============")
//            test_Preference.collect()
//            for(t <- test_Preference){
//              print(t._1 + " -> ")
//              for(tt <- t._2){
//                print(tt._1 + "-" + tt._2 +" , ")
//              }
//              println()
//            }
        //      ============test_Preference============
        //      2 -> 1-2.5 , 7-2.0 , 2-0.0 , 3-0.0 , 4-0.0 , 5-0.0 , 6-0.0 , 8-0.0 , 9-0.0 , 10-0.0 , 11-0.0 , 12-0.0 , 13-0.0 , 14-0.0 , 15-0.0 , 16-0.0 , 17-0.0 ,
        //      3 -> 5-5.0 , 1-0.0 , 2-0.0 , 3-0.0 , 4-0.0 , 6-0.0 , 7-0.0 , 8-0.0 , 9-0.0 , 10-0.0 , 11-0.0 , 12-0.0 , 13-0.0 , 14-0.0 , 15-0.0 , 16-0.0 , 17-0.0 ,
        //      1 -> 3-1.1428571428571428 , 1-0.7142857142857143 , 12-0.7142857142857143 , 17-0.5714285714285714 , 2-0.14285714285714285 , 4-0.14285714285714285 , 5-0.0 , 6-0.0 , 7-0.0 , 8-0.0 , 9-0.0 , 10-0.0 , 11-0.0 , 13-0.0 , 14-0.0 , 15-0.0 , 16-0.0 ,
		println("=====================测试集  推理完成=====================")
        val test_Preference_collect = test_Preference.collect()
        var outResult = new FileWriter(outputFile.PreferenceResult_Test)    //以非追加写入的方式创建FileWriter对象
        for(t <- test_Preference_collect){
            outResult.write(t._1 + " -> ")
            for(tt <- t._2){
                outResult.write("( " + tt._1 + "，" + tt._2 +" ),")
            }
            outResult.write("\r\n")
        }
        outResult.close()

        val test_Preference_11 = test_Preference.map{
            line =>
                var Array = new ArrayBuffer[Int]()
                var i = 0
                while(Array.size < 11 && i < QueryVariable_r){
                    if(line._2(i)._2 > 0 )
                        Array += line._2(i)._1
                    i += 1
                }
                (line._1,(Array, line._3))
        }
        val testSetRDD_sortByIntUserID_11 = test_Preference_11.sortByKey()
        val testSetRDD_sortByUserID_11 = testSetRDD_sortByIntUserID_11.map {           //将user_id转换为String类型
            pair =>
                (pair._1.toString, pair._2)
        }
        testSetRDD_sortByUserID_11.persist(StorageLevel.MEMORY_ONLY)
        val testSet_userNum_11 = testSetRDD_sortByUserID_11.count().toInt              //从testSet_pairRDD中统计测试集中的用户数，注：testSet_userNum可能小于trainingSet_userNum
        val testSet_11 = testSetRDD_sortByUserID_11.collect()                          //执行action，将数据提取到Driver端；testSet——T(u)
        testSetRDD_sortByUserID_11.unpersist()
//              println("============test_Preference_11============")
//              for(t <- 0 until testSet_userNum_11){
//                  println(testSet_11(t)._1 + " -> " + testSet_11(t)._2)
//              }
//        println(testSet_userNum_11)
//        println(testSet_11.length)
//              System.exit(0)
        //============test_Preference_11============
        //3 -> ArrayBuffer(5)
        //2 -> ArrayBuffer(1, 7)
        //1 -> ArrayBuffer(3, 1, 12, 17, 2, 4)
        var top_k = 3
        for (top_i <- 1 to 11) {  //计算top_k = 3, 5, 7, 9, 11时的召回率、准确率以及F-score
			println("=====================top_k : " + top_i + " =====================")
            if (top_i % 2 != 0){
                top_k = top_i
                val test_Preference_topk = test_Preference.map{
                    line =>
                        var Array = new ArrayBuffer[Int]()
                        var i = 0
                        while(Array.size < top_k && i < QueryVariable_r){
                            if(line._2(i)._2 > 0 )
                                Array += line._2(i)._1
                            i += 1
                        }
                        (line._1,Array)
                }
                //    test_Preference_topk.collect()
//                    println("============test_Preference_topk============")
//                    for(t <- test_Preference_topk){
//                        println(t._1 + " -> " + t._2)
//                    }

                //    ============test_Preference_topk============
                //2 -> ArrayBuffer(1, 7)
                //3 -> ArrayBuffer(5)
                //1 -> ArrayBuffer(3, 1, 12)

                val testSetRDD_sortByIntUserID = test_Preference_topk.sortByKey()

                //    println("============testSetRDD_sortByIntUserID============")
                //    for (q <- testSetRDD_sortByIntUserID){
                //      println(q)
                //    }
                val testSetRDD_sortByUserID = testSetRDD_sortByIntUserID.map {           //将user_id转换为String类型
                    pair =>
                        (pair._1.toString, pair._2)
                }

                testSetRDD_sortByUserID.persist(StorageLevel.MEMORY_ONLY)
                val testSet_userNum = testSetRDD_sortByUserID.count().toInt              //从testSet_pairRDD中统计测试集中的用户数，注：testSet_userNum可能小于trainingSet_userNum
                val testSet = testSetRDD_sortByUserID.collect()                          //执行action，将数据提取到Driver端；testSet——T(u)
                testSetRDD_sortByUserID.unpersist()

                    println("============testSet============")
                    println("testSet_userNum : " + testSet_userNum)
                    println("testSet_userNum_11 : " + testSet_userNum_11)
                //    for (q <- testSet){
                //      println(q)
                //    }
                //      for (q <- testSet_11){
                //          println(q)
                //      }
                //      testSet_userNum : 3
                //      testSet_userNum_11 : 3
                //      (1,ArrayBuffer(3, 1, 12))
                //      (2,ArrayBuffer(1, 7))
                //      (3,ArrayBuffer(5))
                //      (1,ArrayBuffer(3, 1, 12, 17, 2, 4))
                //      (2,ArrayBuffer(1, 7))
                //      (3,ArrayBuffer(5))
                val top_k_Preference = new Array[PreferenceBuffer](trainingSet_userNum)   //top_k_Preference(i).preference记录用户i+1前k个item类型偏好的集合,即R(u)
                for (i <-0 until trainingSet_userNum) {
                    top_k_Preference(i) = new PreferenceBuffer
                    top_k_Preference(i).userID = sorted_PreferenceResult(i).userID
                    var set_length = top_k

                    for (j <- 0 until testSet_userNum) {
                        if(testSet(j)._1 == sorted_PreferenceResult(i).userID){
                            set_length = testSet(j)._2.length
                        }
                    }
                    for (j <- 0 until set_length) {
                        top_k_Preference(i).preference += sorted_PreferenceResult(i).QueryResult(j).value
                    }
                }
//                println("============top_k_Preference============")
//                for (q <- top_k_Preference){
//                  print(q.userID + " : ")
//                  for (qq <- q.preference){
//                    print(qq +" ")
//                  }
//                  println()
//                }
                //      ============top_k_Preference============
                //      1 : 6 4 7
                //      2 : 1 16 14
                val testResult = new TestCriterion_1          //记录三种测试指标recall,precision和F_score
                /** 训练集的推理结果和测试集的中间结果testSet需按照userID进行了升序排序 */
                val user_num_upper_bound_Array = Array(100,300,500, 700, 900,1100,1300,1500,1700,6400)
                for(user_num_upper_bound <- user_num_upper_bound_Array){
                    breakable {
                        var i = 0;       //记录top_k_Preference的下标
                        var j = 0;       //记录testSet的下标
                        var user_num = 0; //记录参与计算的用户数
                        var Numerator = 0.0                         //测试指标公式中的分子
                        var RecallDenominator = 0.0                 //召回率公式中的分母
                        var PrecisionDenominator = 0.0              //准确率公式中的分母
                        while (i < trainingSet_userNum) {
//                            println("user_num_upper_bound: " + user_num_upper_bound + "  trainingSet_userNum: " + i)
//                            println("userID: " + testSet(j)._1 + "  " + testSet_11(j)._1 + " " + top_k_Preference(i).userID)
//                            println("评分数: " + testSet_11(j)._2._2)
                            if (testSet(j)._1 == top_k_Preference(i).userID && testSet_11(j)._1 == top_k_Preference(i).userID) { //当top_k_Preference中的用户id与testSet中的用户id一致时
                                if(testSet_11(j)._2._2 >= 50) {
                                    val NumeratorSet = (testSet(j)._2.toSet & top_k_Preference(i).preference.toSet) //NumeratorSet = R(userID)∩T(userID)
                                    //                                println("========================")
                                    //                                println(testSet(j)._1 + "  " + top_k_Preference(i).userID)
                                    //                                println(testSet(j)._2.toSet)
                                    //                                println(testSet_11(j)._2.toSet)
                                    //                                println(top_k_Preference(i).preference.toSet)
                                    //                                println(NumeratorSet)
                                    //                                println(NumeratorSet.size)
                                    //                                println(testSet(j)._2.toSet.size)
                                    //                                println(top_k_Preference(i).preference.toSet.size)
                                    //                                println(testSet_11(j)._2.toSet.size)
                                    //        println(top_k)
                                    //                                System.exit(0)
                                    //        println("========================")
                                    Numerator += NumeratorSet.size
                                    RecallDenominator += testSet_11(j)._2._1.toSet.size
                                    PrecisionDenominator += top_k_Preference(i).preference.toSet.size //PrecisionDenominator此时考虑按照测试集中的用户数进行计算

                                    user_num += 1
                                    if (user_num >= user_num_upper_bound) {
                                        testResult.recall = Numerator / RecallDenominator
                                        testResult.precision = Numerator / PrecisionDenominator
                                        testResult.F_score = 2 * testResult.precision * testResult.recall / (testResult.precision + testResult.recall)

                                        val outLog = new FileWriter(logFile.inference_log, true) //以追加写入的方式创建FileWriter对象
                                        outLog.write("user_num ： " + user_num + "\r\n")
                                        outLog.write("Top-" + top_k + " 模型测试指标:\r\n")
                                        outLog.write("Recall: " + nf.format(testResult.recall) + " = " + Numerator + " / " + RecallDenominator + "\r\n")
                                        outLog.write("Precision: " + nf.format(testResult.precision) + " = " + Numerator + " / " + PrecisionDenominator + "\r\n")
                                        outLog.write("F-score: " + nf.format(testResult.F_score) + "\r\n\r\n")
                                        outLog.close()
                                        break()
                                    }
                                }
                                i += 1
                                j += 1
                            } else if (testSet(j)._1 > top_k_Preference(i).userID) { //testSet(j)._1 > top_k_Preference(i).userID——top_k_Preference(i).userID这一用户在测试集中未出现
                                i += 1 //搜索训练集结果top_k_Preference中的下一用户
                            } else { //testSet(j)._1 < top_k_Preference(i).userID——top_k_Preference(i).userID这一用户在测试集中未出现
                                j += 1 //搜索测试集中间结果testSet中的下一用户
                            }
                        }
                        testResult.recall = Numerator / RecallDenominator
                        testResult.precision = Numerator / PrecisionDenominator
                        testResult.F_score = 2 * testResult.precision * testResult.recall / (testResult.precision + testResult.recall)

                        val outLog = new FileWriter(logFile.inference_log, true) //以追加写入的方式创建FileWriter对象
                        outLog.write("user_num ： " + user_num + "\r\n")
                        outLog.write("Top-" + top_k + " 模型测试指标:\r\n")
                        outLog.write("Recall: " + nf.format(testResult.recall) + " = " + Numerator + " / " + RecallDenominator + "\r\n")
                        outLog.write("Precision: " + nf.format(testResult.precision) + " = " + Numerator + " / " + PrecisionDenominator + "\r\n")
                        outLog.write("F-score: " + nf.format(testResult.F_score) + "\r\n\r\n")
                        outLog.close()
                        break()
                    }


                }
            }
        }


    }

    /** 根据测试样本推理结果计算MAE与RMSE进行模型评估
      * PredictedRatingType = “mean”——预测评分值为推理结果的加权平均值
      * PredictedRatingType = “max”——预测评分值为推理结果中最大后验概率相应的评分变量取值
      */
    def model_evaluation_2(bn: BayesianNetwork, TestData: String, PredictedRatingType: String) {

        if (PredictedRatingType != "mean" && PredictedRatingType != "max") {
            Console.err.println("方法\"model_evaluation_2\"中存在错误！Error: PredictedRatingType取值错误")
            return
        }
        /********************更换【测试数据集】一定要记得修改EvidenceNum与Evidences*****************/
        val EvidenceNum = 4                                          //证据变量个数
        val Evidences = new Array[InferenceVariable](EvidenceNum)    //创建一个证据变量数组存储每个证据变量的序号及其取值
        for (i <- 0 until EvidenceNum)                                //创建数组中每个对象
            Evidences(i) =  new InferenceVariable
        val QueryVariable = new InferenceVariable                     //查询变量

//        Evidences(0).sequence = 1; //U1
//        Evidences(1).sequence = 2; //U2
//        Evidences(2).sequence = 3; //U3
//        Evidences(3).sequence = 5; //I1
//        Evidences(4).sequence = 6; //I2
//        QueryVariable.sequence = 4;  //R 评分值变量的序号

        Evidences(0).sequence = 1; //U1
        Evidences(1).sequence = 2; //U2
        Evidences(2).sequence = 3; //U3
        Evidences(3).sequence = 5; //I1
        QueryVariable.sequence = 4;  //R 评分值变量的序号

        val inFile = SparkConf.sc.textFile(TestData, SparkConf.partitionNum)     //读取测试数据为RDD对象
        reference.TestSampleNum = inFile.count()                                 //测试数据文档中的样本数
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        val sampleInferenceRDD = inFile.map {    //将每行原始样本映射处理为相应的推理结果
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
                val lineStringArray = line.split(separator.data)
                val lineIntArray = lineStringArray map ( _.toInt )

//                println(line)
                for (i <- 0 until EvidenceNum){
                    Evidences(i).value = lineIntArray(Evidences(i).sequence)
//                    print(i + " " + Evidences(i).value + "   ")
                }
//                println()
//                System.exit(0)
                //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)
                val SampleQueryResult = new InferenceResult(bn.r(QueryVariable.sequence - 1))
                SampleQueryResult.userID = lineStringArray(0)
                for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
                    SampleQueryResult.QueryResult(i) =  new Posterior

                val QueryVariable_probability = new Array[Double](bn.r(QueryVariable.sequence - 1)) //存放查询变量的联合概率  P(Q=q,E)
                Enumeration_Process(bn, Evidences, EvidenceNum, QueryVariable, QueryVariable_probability, bn.r(QueryVariable.sequence - 1))
                val sum = QueryVariable_probability.sum
                for (i <- 1 to bn.r(QueryVariable.sequence - 1) ) {                      //依次计算不同查询变量取值对应的后验概率
                    QueryVariable.value = i
                    SampleQueryResult.QueryResult(i - 1).value  = QueryVariable.value
                    SampleQueryResult.QueryResult(i - 1).probability = QueryVariable_probability(i-1) / sum
//                    println(QueryVariable.value +" " + QueryVariable_probability(i-1) + " " + SampleQueryResult.QueryResult(i - 1).probability)
                }
                val real_RatingValue = lineIntArray(QueryVariable.sequence)             //样本中的真实评分值
//                println(real_RatingValue)
                (SampleQueryResult, real_RatingValue, line)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
        }
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//        val sampleInferenceRDD = inFile.map {    //将每行原始样本映射处理为相应的推理结果
//            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
//                //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
//                val lineStringArray = line.split(separator.data)
//                val lineIntArray = lineStringArray map ( _.toInt )
//                for (i <- 0 until EvidenceNum)
//                    Evidences(i).value = lineIntArray(Evidences(i).sequence)
//
//                //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)
//                val SampleQueryResult = new InferenceResult(bn.r(QueryVariable.sequence - 1))
//                SampleQueryResult.userID = lineStringArray(0)
//                for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
//                    SampleQueryResult.QueryResult(i) =  new Posterior
//
//                for (i <- 1 to bn.r(QueryVariable.sequence - 1) ) {                      //依次计算不同查询变量取值对应的后验概率
//                    QueryVariable.value = i
//                    SampleQueryResult.QueryResult(i - 1).value  = QueryVariable.value
//                    SampleQueryResult.QueryResult(i - 1).probability = Enumeration_Process1(bn, Evidences, EvidenceNum, QueryVariable)
//                }
//
//                val real_RatingValue = lineIntArray(QueryVariable.sequence)             //样本中的真实评分值
//                (SampleQueryResult, real_RatingValue)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
//        }
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        val testSet_PredictedResultRDD = sampleInferenceRDD.map {   //将每行样本推理结果映射处理为(predicted_RatingValue, real_RatingValue)
                line =>  //line存储每行测试样本数据映射的(SampleQueryResult, real_RatingValue)
                    var predicted_RatingValue = 0.0                             //基于本条样本推理计算获得的评分预测值
                if ("mean" == PredictedRatingType) {                       //预测评分值为推理结果的加权平均值
                    for (i <- 0 until bn.r(QueryVariable.sequence - 1) ) {                      //依次计算不同查询变量取值对应的后验概率
                        predicted_RatingValue += line._1.QueryResult(i).value * line._1.QueryResult(i).probability
                    }
                }
                else {  //"max" == PredictedRatingType——预测评分值为推理结果中最大后验概率相应的评分变量取值
//                    var MaxPosterior_index = 0
//                    for (i <- 0 until bn.r(QueryVariable.sequence - 1) - 1 ) {
//                        if (line._1.QueryResult(MaxPosterior_index).probability < line._1.QueryResult(i + 1).probability)
//                            MaxPosterior_index = i + 1
//                    }
//                    predicted_RatingValue = line._1.QueryResult(MaxPosterior_index).value

                    val sorted_QueryResult =  line._1.QueryResult.sortWith(_.probability > _.probability)  //sorted_QueryResult存储所有查询变量取值对应的后验概率,按其概率值进行排序
                    val outLog = new FileWriter(logFile.inference_temporary, true)    //以追加写入的方式创建FileWriter对象
                    outLog.write(line._3)

                    for (j <- 0 until bn.r(QueryVariable.sequence - 1) ) {
                        outLog.write("::" + sorted_QueryResult(j).value + " " + sorted_QueryResult(j).probability)
                    }
                    outLog.write("\r\n")
                    outLog.close()
                    predicted_RatingValue = sorted_QueryResult(0).value
//                    println(MaxPosterior_index + " " + predicted_RatingValue)
                }
//                println(predicted_RatingValue + " " + line._2)

                (predicted_RatingValue, line._2)               //(predicted_RatingValue, real_RatingValue)
        }
        val testSet_PredictedResult = testSet_PredictedResultRDD.collect()            //执行action，将数据提取到Driver端。
//        println("=====================testSet_PredictedResult=======================")
//        for(t <- testSet_PredictedResult){
//            println(t._1 + " " + t._2)
//        }
//        System.exit(0)

        val testResult = new TestCriterion_1          //记录三种测试指标recall,precision和F_score
        var Numerator = 0.0                         //测试指标公式中的分子
        var RecallDenominator = 0.0                 //召回率公式中的分母
        var PrecisionDenominator = 0.0              //准确率公式中的分母

        val testResult2 = new TestCriterion_2              //记录二种测试指标MAE和RMSE
        var MAE_Numerator = 0.0                           //MAE公式中的分子
        var RMSE_Numerator = 0.0                          //RMSE公式中的分子
        val Denominator = reference.TestSampleNum         //测试指标公式中的分母

        for (i <- 0 until reference.TestSampleNum.toInt) {
            val difference = testSet_PredictedResult(i)._1 - testSet_PredictedResult(i)._2
            MAE_Numerator += difference.abs
            RMSE_Numerator += pow(difference, 2)
//            println(difference + "=" + testSet_PredictedResult(i)._1 + "-" + testSet_PredictedResult(i)._2)
//            println(MAE_Numerator)
//            println(RMSE_Numerator)
            var flag1 = 0
            var flag2 = 0
            // 4、5
            if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5|| testSet_PredictedResult(i)._1 == 3){  //  推理出的评分值
                flag1 = 1
                PrecisionDenominator += 1
            }
            if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5 || testSet_PredictedResult(i)._2 == 3){ //  实际的评分值
                flag2 = 1
                RecallDenominator += 1
            }
            if(flag1 ==1 &&  flag2 == 1){  //  推理出的评分 == 实际的评分
                Numerator += 1
            }
            // 3、4、5
//            if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5 || testSet_PredictedResult(i)._1 == 3){  //  推理出的评分值
//                flag1 = 1
//                PrecisionDenominator += 1
//            }
//
//            if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5 || testSet_PredictedResult(i)._2 == 3){ //  实际的评分值
//                flag2 = 1
//                RecallDenominator += 1
//            }
//            if(flag1 ==1 &&  flag2 == 1){  //  推理出的评分 == 实际的评分
//                Numerator += 1
//            }
            // 准确值
//            PrecisionDenominator = Denominator
//            RecallDenominator = Denominator
//            if(testSet_PredictedResult(i)._1 == testSet_PredictedResult(i)._2){  //  推理出的评分值
//                Numerator += 1
//            }

//            println(testSet_PredictedResult(i)._1 + " " + testSet_PredictedResult(i)._2 + " " + flag1 + " " + flag2)
//            println(Numerator + " " + PrecisionDenominator + " " + RecallDenominator)
        }

        testResult2.MAE = MAE_Numerator / Denominator
        testResult2.RMSE = sqrt(RMSE_Numerator / Denominator)

        testResult.recall = Numerator / RecallDenominator
        testResult.precision = Numerator / PrecisionDenominator
        testResult.F_score = 2 * testResult.precision * testResult.recall / (testResult.precision + testResult.recall)

        val outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("证据数量：" + EvidenceNum + "\r\n")
        outLog.write("测试集数量：" + Denominator + "\r\n")
        outLog.write("模型测试指标2——预测评分类型：" + PredictedRatingType + "\r\n")
        outLog.write("MAE: " + nf.format(testResult2.MAE) + "\r\n")
        outLog.write("RMSE: " + nf.format(testResult2.RMSE) + "\r\n\r\n")

        outLog.write("Recall: " + nf.format(testResult.recall) + " = " + Numerator + " / " + RecallDenominator + "\r\n")
        outLog.write("Precision: " + nf.format(testResult.precision) + " = " + Numerator + " / " + PrecisionDenominator + "\r\n")
        outLog.write("F-score: " + nf.format(testResult.F_score) + "\r\n\r\n")
        outLog.close()

    }


    /** 根据测试样本推理结果计算MAE与RMSE进行模型评估
      * PredictedRatingType = “mean”——预测评分值为推理结果的加权平均值
      * PredictedRatingType = “max”——预测评分值为推理结果中最大后验概率相应的评分变量取值
      */
    def model_evaluation_R(bn: BayesianNetwork, TestData: String, PredictedRatingType: String) {

        if (PredictedRatingType != "mean" && PredictedRatingType != "max") {
            Console.err.println("方法\"model_evaluation_2\"中存在错误！Error: PredictedRatingType取值错误")
            return
        }
        /********************更换【测试数据集】一定要记得修改EvidenceNum与Evidences*****************/
        val EvidenceNum = 5 //证据变量个数
        val Evidences = new Array[InferenceVariable](EvidenceNum) //创建一个证据变量数组存储每个证据变量的序号及其取值
        for (i <- 0 until EvidenceNum) //创建数组中每个对象
            Evidences(i) = new InferenceVariable
        val QueryVariableNum = 2 //查询变量个数
        val QueryVariable = new Array[InferenceVariable](QueryVariableNum) //创建一个证据变量数组存储每个查询变量的序号及其取值
        for (i <- 0 until QueryVariableNum) //创建数组中每个对象
            QueryVariable(i) = new InferenceVariable

        Evidences(0).sequence = 1; //U1
        Evidences(1).sequence = 2; //U2
        Evidences(2).sequence = 3; //U3
        Evidences(3).sequence = 5; //I1
        Evidences(4).sequence = 8; //I2
        QueryVariable(0).sequence = 4; //R1
        QueryVariable(1).sequence = 7; //R2

        val inFile = SparkConf.sc.textFile(TestData, SparkConf.partitionNum)     //读取测试数据为RDD对象
        reference.TestSampleNum = inFile.count()                                 //测试数据文档中的样本数
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // 填充数据
        // 仅填充评分全为 1 2 3 4 5
        val Ln = QueryVariableNum
        var c = bn.r(QueryVariable(0).sequence - 1)
        val MendedQueryVariable = new Array[Array[Int]](c)
        for (i <- 0 until c) {
            MendedQueryVariable(i) = new Array[Int](Ln + 1)
        }
        for (i <- 0 until c) {
            for (j <- 0 until MendedQueryVariable(i).length) {
                MendedQueryVariable(i)(j) += 1 + i
            }
        }
        val sampleInferenceRDD = inFile.map {    //将每行原始样本映射处理为相应的推理结果
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
                val lineStringArray = line.split(separator.data)
                val lineIntArray = lineStringArray map ( _.toInt )

                //                println(line)
                for (i <- 0 until EvidenceNum){
                    Evidences(i).value = lineIntArray(Evidences(i).sequence)
                    //                    print(i + " " + Evidences(i).value + "   ")
                }
                //                println()
                //                System.exit(0)
                //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)
                val SampleQueryResult = new InferenceResult(c)
                SampleQueryResult.userID = lineStringArray(0)
                for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
                    SampleQueryResult.QueryResult(i) =  new Posterior

                val QueryVariable_probability = new Array[Double](c) //存放查询变量的联合概率  P(Q=q,E)
//                                println("c: " + c)
//                                println("Evidences: ")
//                                for (i <- 0 until EvidenceNum)
//                                    println(Evidences(i).sequence + " " + Evidences(i).value)
//                                println("QueryVariable: ")
//                                for (i <- 0 until QueryVariableNum)
//                                    println(QueryVariable(i).sequence + " " + QueryVariable(i).value)
//                                println("MendedQueryVariable: ")
//                                for (i <- 0 until c) {
//                                    for (j <- 0 until MendedQueryVariable(i).length) {
//                                        print(MendedQueryVariable(i)(j) + " ")
//                                    }
//                                    println()
//                                }
//                                System.exit(0)
                Enumeration_Process1(bn, Evidences, EvidenceNum, QueryVariable, QueryVariableNum, QueryVariable_probability, c, MendedQueryVariable)
//                                println()
//                                for (i <- 0 until c) {
//                                    println(QueryVariable_probability(i))
//                                }
//                                System.exit(0)
                val sum = QueryVariable_probability.sum
                for (i <- 0 until c) { //依次计算不同查询变量取值对应的后验概率
                    SampleQueryResult.QueryResult(i).values = new Array[Int](QueryVariableNum+1)
                    MendedQueryVariable(i).copyToArray(SampleQueryResult.QueryResult(i).values)
                    if(sum == 0)
                        SampleQueryResult.QueryResult(i).probability = 1.0 / 5
                    else
                        SampleQueryResult.QueryResult(i).probability = QueryVariable_probability(i) / sum
                    //                    println(QueryVariable.value +" " + QueryVariable_probability(i-1) + " " + SampleQueryResult.QueryResult(i - 1).probability)
                }
                val real_RatingValue = lineIntArray(QueryVariable(0).sequence)             //样本中的真实评分值
                //                println(real_RatingValue)
                (SampleQueryResult, real_RatingValue, line)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
        }

        val testSet_PredictedResultRDD = sampleInferenceRDD.map {   //将每行样本推理结果映射处理为(predicted_RatingValue, real_RatingValue)
            line =>  //line存储每行测试样本数据映射的(SampleQueryResult, real_RatingValue)
                var predicted_RatingValue = 0.0                             //基于本条样本推理计算获得的评分预测值
                if ("mean" == PredictedRatingType) {                       //预测评分值为推理结果的加权平均值
                    for (i <- 0 until c ) {                      //依次计算不同查询变量取值对应的后验概率
//                        println(line._1.QueryResult(i).values(1) +"  " + line._1.QueryResult(i).probability)
                        predicted_RatingValue += line._1.QueryResult(i).values(1) * line._1.QueryResult(i).probability
                    }

//                    val outLog = new FileWriter(logFile.inference_temporary, true)    //以追加写入的方式创建FileWriter对象
//                    outLog.write(line._3)
//                    for (j <- 0 until c ) {
//                        outLog.write("::")
//                        for (k <- 1 until  QueryVariableNum+1) //创建数组中每个对象
//                            outLog.write("::" + line._1.QueryResult(j).values(k) + " ")
//                        outLog.write(" " + line._1.QueryResult(j).probability)
//                    }
//                    outLog.write("\r\n")
//                    outLog.close()

                }
                else {  //"max" == PredictedRatingType——预测评分值为推理结果中最大后验概率相应的评分变量取值
                    //                    var MaxPosterior_index = 0
                    //                    for (i <- 0 until c - 1 ) {
                    //                        if (line._1.QueryResult(MaxPosterior_index).probability < line._1.QueryResult(i + 1).probability)
                    //                            MaxPosterior_index = i + 1
                    //                    }
                    //                    predicted_RatingValue = line._1.QueryResult(MaxPosterior_index).value

                    val sorted_QueryResult =  line._1.QueryResult.sortWith(_.probability > _.probability)  //sorted_QueryResult存储所有查询变量取值对应的后验概率,按其概率值进行排序

//                    val outLog = new FileWriter(logFile.inference_temporary, true)    //以追加写入的方式创建FileWriter对象
//                    outLog.write(line._3)
//                    for (j <- 0 until c ) {
//                        outLog.write("::")
//                        for (k <- 1 until  QueryVariableNum+1) //创建数组中每个对象
//                            outLog.write("::" + sorted_QueryResult(j).values(k) + " ")
//                        outLog.write(" " + sorted_QueryResult(j).probability)
//                    }
//                    outLog.write("\r\n")
//                    outLog.close()
                    predicted_RatingValue = sorted_QueryResult(0).values(1)
//                    println( "predicted_RatingValue " + predicted_RatingValue)
                }
                //                println(predicted_RatingValue + " " + line._2)

                (predicted_RatingValue, line._2)               //(predicted_RatingValue, real_RatingValue)
        }
        val testSet_PredictedResult = testSet_PredictedResultRDD.collect()            //执行action，将数据提取到Driver端。
//                println("=====================testSet_PredictedResult=======================")
//                for(t <- testSet_PredictedResult){
//                    println(t._1 + " " + t._2)
//                }
//                System.exit(0)

        val testResult = new TestCriterion_1          //记录三种测试指标recall,precision和F_score
        var Numerator = 0.0                         //测试指标公式中的分子
        var RecallDenominator = 0.0                 //召回率公式中的分母
        var PrecisionDenominator = 0.0              //准确率公式中的分母

        val testResult2 = new TestCriterion_2              //记录二种测试指标MAE和RMSE
        var MAE_Numerator = 0.0                           //MAE公式中的分子
        var RMSE_Numerator = 0.0                          //RMSE公式中的分子
        val Denominator = reference.TestSampleNum         //测试指标公式中的分母

        val outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("证据数量：" + EvidenceNum + "\r\n")
        outLog.write("测试集数量：" + Denominator + "\r\n")
        outLog.write("模型测试指标2——预测评分类型：" + PredictedRatingType + "\r\n")
        outLog.write( "\r\n")
        outLog.close()

        for (i <- 0 until reference.TestSampleNum.toInt) {
            val difference = testSet_PredictedResult(i)._1 - testSet_PredictedResult(i)._2
            MAE_Numerator += difference.abs
            RMSE_Numerator += pow(difference, 2)
            //            println(difference + "=" + testSet_PredictedResult(i)._1 + "-" + testSet_PredictedResult(i)._2)
            //            println(MAE_Numerator)
            //            println(RMSE_Numerator)
            var flag1 = 0
            var flag2 = 0
            // 4、5
//            if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5|| testSet_PredictedResult(i)._1 == 3){  //  推理出的评分值
//                flag1 = 1
//                PrecisionDenominator += 1
//            }
//            if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5 || testSet_PredictedResult(i)._2 == 3){ //  实际的评分值
//                flag2 = 1
//                RecallDenominator += 1
//            }
			if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5){  //  推理出的评分值
				flag1 = 1
				PrecisionDenominator += 1
			}
			if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5){ //  实际的评分值
				flag2 = 1
				RecallDenominator += 1
			}
            if(flag1 ==1 &&  flag2 == 1){  //  推理出的评分 == 实际的评分
                Numerator += 1
            }
            // 3、4、5
            //            if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5 || testSet_PredictedResult(i)._1 == 3){  //  推理出的评分值
            //                flag1 = 1
            //                PrecisionDenominator += 1
            //            }
            //
            //            if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5 || testSet_PredictedResult(i)._2 == 3){ //  实际的评分值
            //                flag2 = 1
            //                RecallDenominator += 1
            //            }
            //            if(flag1 ==1 &&  flag2 == 1){  //  推理出的评分 == 实际的评分
            //                Numerator += 1
            //            }
            // 准确值
            //            PrecisionDenominator = Denominator
            //            RecallDenominator = Denominator
            //            if(testSet_PredictedResult(i)._1 == testSet_PredictedResult(i)._2){  //  推理出的评分值
            //                Numerator += 1
            //            }

            //            println(testSet_PredictedResult(i)._1 + " " + testSet_PredictedResult(i)._2 + " " + flag1 + " " + flag2)
            //            println(Numerator + " " + PrecisionDenominator + " " + RecallDenominator)
            var current_num = i + 1
            if(current_num%50000 == 0){
                testResult2.MAE = MAE_Numerator / current_num
                testResult2.RMSE = sqrt(RMSE_Numerator / current_num)

                testResult.recall = Numerator / RecallDenominator
                testResult.precision = Numerator / PrecisionDenominator
                testResult.F_score = 2 * testResult.precision * testResult.recall / (testResult.precision + testResult.recall)

                val outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
                outLog.write("当前数据量：" + (i+1) + "\r\n")
                outLog.write("MAE: " + nf.format(testResult2.MAE) + " = " + MAE_Numerator + " / " + current_num +  "\r\n")
                outLog.write("RMSE: " + nf.format(testResult2.RMSE) + " = sqrt(" + RMSE_Numerator + " / " + current_num +  ")\r\n\r\n")
                if(PredictedRatingType == "max"){
                    outLog.write("Recall: " + nf.format(testResult.recall) + " = " + Numerator + " / " + RecallDenominator + "\r\n")
                    outLog.write("Precision: " + nf.format(testResult.precision) + " = " + Numerator + " / " + PrecisionDenominator + "\r\n")
                    outLog.write("F-score: " + nf.format(testResult.F_score) + "\r\n\r\n")
                }
                outLog.close()
            }

        }
    }

    /** 根据测试样本推理结果计算MAE与RMSE进行模型评估
      * PredictedRatingType = “mean”——预测评分值为推理结果的加权平均值
      * PredictedRatingType = “max”——预测评分值为推理结果中最大后验概率相应的评分变量取值
      */
    def model_evaluation_R_strategy(bn: BayesianNetwork, TestData: String, PredictedRatingType: String) {
        println("---------------11111111111111111----------------------")
        if (PredictedRatingType != "max" && PredictedRatingType != "mean"&& PredictedRatingType != "mean.round") {
            Console.err.println("方法存在错误！Error: PredictedRatingType取值错误")
            return
        }
        /********************更换【测试数据集】一定要记得修改EvidenceNum与Evidences*****************/
        if(reference.data_type == "ml-1m"){
            reference.user_Attribution_num = 294   //用户属性取值组合数
            reference.usercount = 6040
        }else if(reference.data_type == "Clothing-Fit-Data") {
            reference.user_Attribution_num = 117649 //用户属性取值组合数
            reference.usercount = 77347
        }
        // 对每种用户属性类型，构建三维数组，   每维偏好 的 每个取值 的 5种评分
        val P_R = new Array[Array[Array[Array[Double]]]](reference.user_Attribution_num+1)        //定义一个四维可变数组theta
        // P_R(user_Attribution_num+1)(reference.LatentNum_Total+1)(reference.Latent_r(i)+1)(rating+1)
        // P_R(294+1)(1+1)(18+1)(6+1)
//        println(user_Attribution_num)
//        println(reference.LatentNum_Total)
//        println(reference.Latent_r(0))
//        System.exit(0)
        for (i <- 1 until reference.user_Attribution_num+1) {
            P_R(i) = new Array[Array[Array[Double]]](reference.LatentNum_Total+1)
//            println(i)
            for (j <- 1 until reference.LatentNum_Total+1){
                P_R(i)(j) = new Array[Array[Double]](reference.Latent_r(j-1)+1)
//                println(i + " " + j)
                for (k <- 1 until reference.Latent_r(j-1)+1){
                    P_R(i)(j)(k) = new Array[Double](5+1)
//                    println(i + " " + j + " " + k)
                }
            }
        }
        // 每个用户属性类型，构建三维数组，   每维偏好 的 每个取值 的 5种评分 概率
        val P_R_6040 = new Array[Array[Array[Array[Double]]]](reference.usercount+1)
        // 每个用户属性类型，构建三维数组，   每维偏好 的 每个取值 的 5种评分 评分次数
        val P_R_number_6040 = new Array[Array[Array[Array[Int]]]](reference.usercount+1)
        for (i <- 1 until reference.usercount+1) {
            P_R_6040(i) = new Array[Array[Array[Double]]](reference.LatentNum_Total+1)
            P_R_number_6040(i) = new Array[Array[Array[Int]]](reference.LatentNum_Total+1)
            //            println(i)
            for (j <- 1 until reference.LatentNum_Total+1){
                P_R_6040(i)(j) = new Array[Array[Double]](reference.Latent_r(j-1)+1)
                P_R_number_6040(i)(j) = new Array[Array[Int]](reference.Latent_r(j-1)+1)
                //                println(i + " " + j)
                for (k <- 1 until reference.Latent_r(j-1)+1){
                    P_R_6040(i)(j)(k) = new Array[Double](5+1)
                    P_R_number_6040(i)(j)(k) = new Array[Int](5+1)
                    //                    println(i + " " + j + " " + k)
                }
            }
        }
//        System.exit(0)
        // 取出每个OPBN
        var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
        if(reference.data_type == "ml-1m"){
            for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
                bn_Array(i) = new BayesianNetwork(reference.NodeNum)
                bn_Array(i).r(0) = 2; bn_Array(i).r(1) = 7; bn_Array(i).r(2) = 21;
                bn_Array(i).r(3) = 5;
                bn_Array(i).r(4) = reference.Latent_r(i);
                bn_Array(i).r(5) = reference.Latent_r(i);
                bn_Array(i).l(0) = "U"; bn_Array(i).l(1) = "U"; bn_Array(i).l(2) = "U";
                bn_Array(i).l(3) = "R";
                bn_Array(i).l(4) = "I";
                bn_Array(i).l(5) = "L";
                bn_Array(i).latent(5) = 1;
            }
        }else if(reference.data_type == "Clothing-Fit-Data") {
            for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
                bn_Array(i) = new BayesianNetwork(reference.NodeNum)
                bn_Array(i).r(0) = 7; bn_Array(i).r(1) = 7; bn_Array(i).r(2) = 7;bn_Array(i).r(3) = 7; bn_Array(i).r(4) = 7; bn_Array(i).r(5) = 7;
                bn_Array(i).r(6) = 5;
                bn_Array(i).r(7) = reference.Latent_r(i);
                bn_Array(i).r(8) = reference.Latent_r(i);
                bn_Array(i).l(0) = "U"; bn_Array(i).l(1) = "U"; bn_Array(i).l(2) = "U";bn_Array(i).l(3) = "U"; bn_Array(i).l(4) = "U"; bn_Array(i).l(5) = "U";
                bn_Array(i).l(6) = "R";
                bn_Array(i).l(7) = "I";
                bn_Array(i).l(8) = "L";
                bn_Array(i).latent(8) = 1;
            }
        }






        for(i <- 0 until reference.LatentNum_Total){
            //修改文件名
            var temp_string = (i+1).toString + ".txt"
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//            outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
//            outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
            outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            val input = new BN_Input
            //从文件中读取表示BN初始结构的邻接矩阵到bn.structure中
            input.structureFromFile(outputFile.optimal_structure, separator.structure, bn_Array(i))
            bn_Array(i).qi_computation()
            bn_Array(i).create_CPT()
            //从文件中读取参数θ到动态三维数组bn.theta
            input.CPT_FromFile(outputFile.optimal_theta, separator.theta, bn_Array(i))
        }
        println("---------------222222222222222222----------------------")

        var EvidenceNum = 4 //证据变量个数
        if(reference.data_type == "ml-1m"){
            EvidenceNum = 4
        }else if(reference.data_type == "Clothing-Fit-Data") {
            EvidenceNum = 7
        }
        var Evidences = new Array[InferenceVariable](EvidenceNum) //创建一个证据变量数组存储每个证据变量的序号及其取值
        if(reference.data_type == "ml-1m"){
            for (i <- 0 until EvidenceNum) //创建数组中每个对象
                Evidences(i) = new InferenceVariable
            Evidences(0).sequence = 1; //U1
            Evidences(1).sequence = 2; //U2
            Evidences(2).sequence = 3; //U3
            Evidences(3).sequence = 5; //I
        }else if(reference.data_type == "Clothing-Fit-Data") {
            for (i <- 0 until EvidenceNum) //创建数组中每个对象
                Evidences(i) = new InferenceVariable
            val QueryVariable = new InferenceVariable                     //查询变量
            Evidences(0).sequence = 1; //U1
            Evidences(1).sequence = 2; //U2
            Evidences(2).sequence = 3; //U3
            Evidences(3).sequence = 4; //U4
            Evidences(4).sequence = 5; //U5
            Evidences(5).sequence = 6; //U6
            Evidences(6).sequence = 8; //I
        }
        var QueryVariable = new InferenceVariable                     //查询变量
//        QueryVariable.sequence = 4;  //R
        if(reference.data_type == "ml-1m"){
            QueryVariable.sequence = 4;  //R
        }else if(reference.data_type == "Clothing-Fit-Data") {
            QueryVariable.sequence = 7;  //R
        }

        val inFile = SparkConf.sc.textFile(TestData, SparkConf.partitionNum)     //读取测试数据为RDD对象
        reference.TestSampleNum = inFile.count()                                 //测试数据文档中的样本数
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//        val P_R_flag = new Array[Int](user_Attribution_num)        //定义一个三维可变数组theta
        reference.SampleNum = inFile.count()                         //原始数据文档中的样本数
        var outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("训练数据集 : " + reference.SampleNum + "\r\n\r\n")
        outLog.close()
        // 得到每个测试用户的用户属性
        val user_Data_6040 = SparkConf.sc.textFile(inputFile.inference_learning_User, SparkConf.partitionNum)  //读取原始数据为RDD
        val user_Data_6040_RDD = user_Data_6040.map {    //将每行原始样本映射处理为相应的推理结果
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                val lineStringArray = line.split(separator.data).map( _.toInt )
                val userID = lineStringArray(0)
                var user_attribution = 0
                if(reference.data_type == "ml-1m"){
                    user_attribution = (lineStringArray(1)-1)*7*21 + (lineStringArray(2)-1)*21 + lineStringArray(3)
                }else if(reference.data_type == "Clothing-Fit-Data") {
                    user_attribution = (lineStringArray(1)-1)*7*7*7*7*7 + (lineStringArray(2)-1)*7*7*7*7 + (lineStringArray(3)-1)*7*7*7 + (lineStringArray(4)-1)*7*7 + (lineStringArray(5)-1)*7 + lineStringArray(6)
                }
                (userID, user_attribution)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
        }
        val user_Data_6040_collection = user_Data_6040_RDD.collect()
        println("---------------333333333333333----------------------")
//        0 1 158 第一个用户 第158种用户属性组合
//        1 2 143
//        for(i <- 0 until user_Data_6040_collection.length){
//            println(i + " " + user_Data_6040_collection(i)._1 + " " + user_Data_6040_collection(i)._2)
//        }
//        System.exit(0)
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // 对每个OPBN，计算
        for (j <- 0 until reference.LatentNum_Total) {
            val bn = bn_Array(j)
            var learning_User = SparkConf.sc.textFile(inputFile.inference_learning_User_strategy, SparkConf.partitionNum)  //每种用户取值组合的用户属性
            learning_User = learning_User.map {    //将每行原始样本映射处理为相应的推理结果
                line =>
                    var line1 = ""
                    for (k <- 0 until reference.Latent_r(j)-1){
                        line1 += line + separator.data + (k + 1).toString + separator.data + (k + 1).toString + separator.tab // 填充评分的位置
                    }
                    line1 += line + separator.data + reference.Latent_r(j).toString + separator.data + reference.Latent_r(j).toString
                    line1
            }
            learning_User = learning_User.flatMap(line => line.split(separator.tab))

//            294,2,7,21,1,1
//            294,2,7,21,2,2
//            ...
//            294,2,7,21,17,17
//            294,2,7,21,18,18
//            learning_User.foreach(println(_))
//            System.exit(0)

            val sampleInferenceRDD = learning_User.map {    //将每行原始样本映射处理为相应的推理结果
                line =>
                    //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
                    val lineStringArray = line.split(separator.data)
                    val lineIntArray = lineStringArray map ( _.toInt )
                    for (i <- 0 until EvidenceNum) {
                        Evidences(i).value = lineIntArray(Evidences(i).sequence)
//                        println(Evidences(i).value)
                    }
//                    println(line)
//                    System.exit(0)
                    //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)

                    val SampleQueryResult = new InferenceResult(bn.r(QueryVariable.sequence - 1))
                    SampleQueryResult.userID = lineStringArray(0) + separator.data + lineIntArray(Evidences(EvidenceNum-1).sequence) //以 ID,属性  为标记， 用于填表
                    //            1,1  第1种用户组合对M1=1的5种评分概率
                    //            1,2  第1种用户组合对M1=2的5种评分概率
                    //            。。。
                    //            294,18 第294种用户组合对M1=18的5种评分概率
//                    println(SampleQueryResult.userID)
//                    System.exit(0)
                    for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
                        SampleQueryResult.QueryResult(i) =  new Posterior
//                            println("============userID============")
//                            println(SampleQueryResult.userID)
//                            println("============Evidence============")
//                            for (i <- 0 until EvidenceNum)
//                              println(Evidences(i).value)
//                            System.exit(0)
                    val QueryVariable_probability = new Array[Double](bn.r(QueryVariable.sequence - 1)) //存放查询变量的联合概率  P(Q=q,E)
                    if(reference.inference_type == "MLBN") {
                        Enumeration_Process(bn, Evidences, EvidenceNum, QueryVariable, QueryVariable_probability, bn.r(QueryVariable.sequence - 1))
                    }else if (reference.inference_type == "PNN") {

                    }
//                    for (i <- 0 until bn.r(QueryVariable.sequence - 1)) {
//                        println(QueryVariable_probability(i))
//                    }
//                    System.exit(0)
                    val sum = QueryVariable_probability.sum
                    for (i <- 1 to bn.r(QueryVariable.sequence - 1) ) {                      //依次计算不同查询变量取值对应的后验概率
                        QueryVariable.value = i
                        SampleQueryResult.QueryResult(i - 1).value  = QueryVariable.value
                        if(sum == 0)
                            SampleQueryResult.QueryResult(i - 1).probability = 1.0 / 5
                        else
                            SampleQueryResult.QueryResult(i - 1).probability = QueryVariable_probability(i-1) / sum

                    }
                    SampleQueryResult
            }
            //SampleQueryResult存储每行样本对应的推理结果——各用户偏好属性值对应的后验概率
            var SampleQueryResult = sampleInferenceRDD.collect()    //执行action，将数据提取到Driver端。
            println("---------------44444444444444444----------------------")
//            println("============SampleQueryResult============")
//            println(SampleQueryResult.length)
//            for (q <- SampleQueryResult){
//                println("============userID============")
//                println(q.userID)
//                println("============QueryResult============")
//                for (qq <- q.QueryResult){
//                    println(qq.value + " " + qq.probability)
//                }
//            }
////            ============userID============
////            294,17
////            ============QueryResult============
////            1 0.056840182964498207
////            2 0.10347232156300407
////            3 0.28025781975718
////            4 0.37114278463268524
////            5 0.18828689108263255
////            ============userID============
////            294,18
////            ============QueryResult============
////            1 0.027876871649756222
////            2 0.05131554955433919
////            3 0.1790203428290585
////            4 0.30029796270031806
////            5 0.4414892732665281
//            println(SampleQueryResult.length)
//            System.exit(0)
            ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            // 每类用户对某一电影类型的每一取值的5种评分的  概率
//            ============userID============
//            userID,I
//            294,17
//            ============QueryResult============
//            value probability
//            1 0.056840182964498207
//            2 0.10347232156300407
//            3 0.28025781975718
//            4 0.37114278463268524
//            5 0.18828689108263255

            for (q <- SampleQueryResult){
                val userID = q.userID.split(separator.data)(0).toInt
                val I = q.userID.split(separator.data)(1).toInt
//                println(userID + " " + I)
                // P_R(user_Attribution_num+1)(reference.LatentNum_Total+1)(reference.Latent_r(i)+1)(rating+1)
                // P_R(294+1)(1+1)(18+1)(5+1)
                for (qq <- q.QueryResult){
                    P_R(userID)(j+1)(I)(qq.value) = qq.probability
//                    println(qq.value + " " + P_R(userID)(j+1)(I)(qq.value))
                }
            }
//            for (userID <- 1 until user_Attribution_num+1) {
//                for (j <- 1 until reference.LatentNum_Total+1){
//                    for (k <- 1 until reference.Latent_r(j-1)+1){
//                        println(userID + " " + j + " " + k + ":::")
//                        for (l <- 1 until 5+1){
//                            print(P_R(userID)(j)(k)(l) + " ")
//                        }
//                        println()
//                    }
//                }
//            }
//            System.exit(0)

            // 每个用户对某一电影类型的每一取值的5种评分的  次数
            //        val introduction_Data = SparkConf.sc.textFile(inputFile.inference_introduction_Data, SparkConf.partitionNum)
            //        val introduction_Data = SparkConf.sc.textFile(inputFile.inference_test_Data, SparkConf.partitionNum)
            val introduction_Data = SparkConf.sc.textFile(inputFile.inference_learning_Data, SparkConf.partitionNum)
            val inference_introduction_Data_RDD = introduction_Data.map {    //将每行原始样本映射处理为相应的推理结果
                line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                    val lineStringArray = line.split(separator.data).map( _.toInt )
                    val userID = lineStringArray(0)

//                    // learning_data
//                    // ID,U,U,U,R,M1,M2
//                    // 1,2,1,11,5,7,7
                    var I = lineStringArray(5+j)
                    if(reference.data_type == "ml-1m"){
                        I = lineStringArray(5+j)
                    }else if(reference.data_type == "Clothing-Fit-Data") {
                        I = lineStringArray(8+j)
                    }

                    // inference_data
                    // ID,U,U,U,R,M,L,R,M,L
                    // 0,1,2,3, 4,5,6,7,8,9
                    // 1,1,7,14,3,3,1,3,8,1
//                    val I = lineStringArray(5+3*j)

                    var real_RatingValue = lineStringArray(4)
                    if(reference.data_type == "ml-1m"){
                        real_RatingValue = lineStringArray(4)
                    }else if(reference.data_type == "Clothing-Fit-Data") {
                        real_RatingValue = lineStringArray(7)
                    }
                    (userID, I, real_RatingValue)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
            }



            val inference_introduction_Data_collection = inference_introduction_Data_RDD.collect()
            println("---------------555555555555555555----------------------")

            // 统计每个用户对每种电影属性每种评分的次数
            for(p <- inference_introduction_Data_collection){
//                println("userID" + " " + p._1)
//                println("I" + " " + p._2)
//                println("real_RatingValue" + " " + p._3)
                P_R_number_6040(p._1)(j+1)(p._2)(p._3) += 1
            }
//            System.exit(0)
            // 每个用户对某一电影类型的每一取值的5种评分的  概率
            for (userID <- 1 until reference.usercount +1) {
                //        user_Data_6040_collection
                //        0 1 158 第一个用户 第158种用户属性组合
                //        1 2 143
                val user_attrition = user_Data_6040_collection(userID-1)._2  // 每个用户属于294种用户类型中的哪一个
                for (j <- 1 until reference.LatentNum_Total+1){  //第几个电影属性
                    for (k <- 1 until reference.Latent_r(j-1)+1){ //电影属性的势
                        for (l <- 1 until 5+1){ //5个评分值
//                            P_R_6040(userID)(j)(k)(l) = P_R(user_attrition)(j)(k)(l)  //有策略 但均 0次评分次数
                            P_R_6040(userID)(j)(k)(l) = P_R(user_attrition)(j)(k)(l) * (P_R_number_6040(userID)(j)(k)(l) + 1)
                        }
                    }
                }
            }
        }
//        for (userID <- 1 until 1+1) {
//            for (j <- 1 until reference.LatentNum_Total+1){
//                for (k <- 1 until reference.Latent_r(j-1)+1){
//                    println(userID + " " + j + " " + k + ":::")
//                    for (l <- 1 until 5+1){
//                        println(P_R_number_6040(userID)(j)(k)(l) + " " + P_R_6040(userID)(j)(k)(l))
//                    }
//                    println()
//                }
//            }
//        }
//        System.exit(0)
        // 得到每条测试样本的评分的  估计值 实际值
        val testSet_PredictedResultRDD = inFile.map {
            line =>
            val lineStringArray = line.split(separator.data).map( _.toInt )
            val userID = lineStringArray(0)
            var P_R_Array = new Array[Double](6)
            // learning_data
            // ID,U,U,U,R,M1,M2
            // 1,2,1,11,5,7,7
//            for(i <- 1 to 5){
//                P_R_Array(i) = 1.0
//                for (j <- 1 until reference.LatentNum_Total+1){
//                    val I = lineStringArray(5+j-1)
//                    P_R_Array(i) *= P_R_6040(userID)(j)(I)(i)
//                }
//            }
            // inference_data
            // ID,U,U,U,R,M,L,R,M,L
            // 0,1,2,3, 4,5,6,7,8,9
            // 1,1,7,14,3,3,1,3,8,1
            for(i <- 1 to 5){
                P_R_Array(i) = 1.0
                for (j <- 1 until reference.LatentNum_Total+1){
                    var I = lineStringArray(5+3*(j-1))
                    if(reference.data_type == "ml-1m"){
                        I = lineStringArray(5+3*(j-1))
                    }else if(reference.data_type == "Clothing-Fit-Data") {
                        I = lineStringArray(8+3*(j-1))
                    }
                    P_R_Array(i) *= P_R_6040(userID)(j)(I)(i)  // P(R=1|U,M1=1,M2=2) = P(R=1|U,M1=1)*P(R=1|U,M2=2)
                }
            }
            val sum = P_R_Array.sum
            for(i <- 1 to 5){
                P_R_Array(i) /= sum
            }
            var predicted_RatingValue = 0.0
            if ("max" == PredictedRatingType){ //"max" == PredictedRatingType——预测评分值为推理结果中最大后验概率相应的评分变量取值
                var max_P_R = 0.0
                for(i <- 1 to 5){
                    if(P_R_Array(i) > max_P_R){
                        predicted_RatingValue = i
                        max_P_R = P_R_Array(i)
                    }
                }
            }else if ("mean" == PredictedRatingType) { //预测评分值为推理结果的加权平均值
                for(i <- 1 to 5){
                    predicted_RatingValue += P_R_Array(i)*i
//                    predicted_RatingValue = predicted_RatingValue.round
                }
            }else if ("mean.round" == PredictedRatingType) { //预测评分值为推理结果的加权平均值
                for(i <- 1 to 5){
                    predicted_RatingValue += P_R_Array(i)*i
                    predicted_RatingValue = predicted_RatingValue.round
                }
            }


            var real_RatingValue = lineStringArray(4)
            if(reference.data_type == "ml-1m"){
                real_RatingValue = lineStringArray(4)
            }else if(reference.data_type == "Clothing-Fit-Data") {
                real_RatingValue = lineStringArray(7)
            }
            (predicted_RatingValue, real_RatingValue)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
        }
        val testSet_PredictedResult = testSet_PredictedResultRDD.collect()
        println("---------------666666666666666666666----------------------")

//        //                println("=====================testSet_PredictedResult=======================")
//        //                for(t <- testSet_PredictedResult){
//        //                    println(t._1 + " " + t._2)
//        //                }
//        //                System.exit(0)
//
        val testResult = new TestCriterion_1          //记录三种测试指标recall,precision和F_score
        var Numerator = 0.0                         //测试指标公式中的分子
        var RecallDenominator = 0.0                 //召回率公式中的分母
        var PrecisionDenominator = 0.0              //准确率公式中的分母

        val testResult2 = new TestCriterion_2              //记录二种测试指标MAE和RMSE
        var MAE_Numerator = 0.0                           //MAE公式中的分子
        var RMSE_Numerator = 0.0                          //RMSE公式中的分子
        val Denominator = reference.TestSampleNum         //测试指标公式中的分母

        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("证据数量：" + EvidenceNum + "\r\n")
        outLog.write("测试集数量：" + Denominator + "\r\n")
        outLog.write("模型测试指标2——预测评分类型：" + PredictedRatingType + "\r\n")
        outLog.write( "\r\n")
        outLog.close()

        var current_num = 0
        // 根据每条测试样本的评分的  估计值 实际值  计算相关的指标
        for (i <- 0 until reference.TestSampleNum.toInt) {
            val difference = testSet_PredictedResult(i)._1 - testSet_PredictedResult(i)._2
            MAE_Numerator += difference.abs
            RMSE_Numerator += pow(difference, 2)
            //            println(difference + "=" + testSet_PredictedResult(i)._1 + "-" + testSet_PredictedResult(i)._2)
            //            println(MAE_Numerator)
            //            println(RMSE_Numerator)
            var flag1 = 0
            var flag2 = 0
            // 4、5
            //            if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5|| testSet_PredictedResult(i)._1 == 3){  //  推理出的评分值
            //                flag1 = 1
            //                PrecisionDenominator += 1
            //            }
            //            if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5 || testSet_PredictedResult(i)._2 == 3){ //  实际的评分值
            //                flag2 = 1
            //                RecallDenominator += 1
            //            }
            if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5){  //  推理出的评分值
                flag1 = 1
                PrecisionDenominator += 1
            }
            if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5){ //  实际的评分值
                flag2 = 1
                RecallDenominator += 1
            }
            if(flag1 ==1 &&  flag2 == 1){  //  推理出的评分 == 实际的评分
                Numerator += 1
            }
            // 3、4、5
            //            if(testSet_PredictedResult(i)._1 == 4 || testSet_PredictedResult(i)._1 == 5 || testSet_PredictedResult(i)._1 == 3){  //  推理出的评分值
            //                flag1 = 1
            //                PrecisionDenominator += 1
            //            }
            //
            //            if(testSet_PredictedResult(i)._2 == 4 || testSet_PredictedResult(i)._2 == 5 || testSet_PredictedResult(i)._2 == 3){ //  实际的评分值
            //                flag2 = 1
            //                RecallDenominator += 1
            //            }
            //            if(flag1 ==1 &&  flag2 == 1){  //  推理出的评分 == 实际的评分
            //                Numerator += 1
            //            }
            // 准确值
            //            PrecisionDenominator = Denominator
            //            RecallDenominator = Denominator
            //            if(testSet_PredictedResult(i)._1 == testSet_PredictedResult(i)._2){  //  推理出的评分值
            //                Numerator += 1
            //            }

            //            println(testSet_PredictedResult(i)._1 + " " + testSet_PredictedResult(i)._2 + " " + flag1 + " " + flag2)
            //            println(Numerator + " " + PrecisionDenominator + " " + RecallDenominator)
            current_num += 1
//            if(current_num%50000 == 0){
//                testResult2.MAE = MAE_Numerator / current_num
//                testResult2.RMSE = sqrt(RMSE_Numerator / current_num)
//
//                testResult.recall = Numerator / RecallDenominator
//                testResult.precision = Numerator / PrecisionDenominator
//                testResult.F_score = 2 * testResult.precision * testResult.recall / (testResult.precision + testResult.recall)
//
//                val outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
//                outLog.write("当前数据量：" + (i+1) + "\r\n")
//                outLog.write("MAE: " + nf.format(testResult2.MAE) + " = " + MAE_Numerator + " / " + current_num +  "\r\n")
//                outLog.write("RMSE: " + nf.format(testResult2.RMSE) + " = sqrt(" + RMSE_Numerator + " / " + current_num +  ")\r\n\r\n")
//                if(PredictedRatingType == "max"){
//                    outLog.write("Recall: " + nf.format(testResult.recall) + " = " + Numerator + " / " + RecallDenominator + "\r\n")
//                    outLog.write("Precision: " + nf.format(testResult.precision) + " = " + Numerator + " / " + PrecisionDenominator + "\r\n")
//                    outLog.write("F-score: " + nf.format(testResult.F_score) + "\r\n\r\n")
//                }

        }
        testResult2.MAE = MAE_Numerator / current_num
        testResult2.RMSE = sqrt(RMSE_Numerator / current_num)
        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("当前数据量：" + (current_num) + "\r\n")
        outLog.write("MAE: " + nf.format(testResult2.MAE) + " = " + MAE_Numerator + " / " + current_num +  "\r\n")
        outLog.write("RMSE: " + nf.format(testResult2.RMSE) + " = sqrt(" + RMSE_Numerator + " / " + current_num +  ")\r\n\r\n")
        outLog.close()

    }


    /** 枚举法——BN精确推理
      * 根据证据变量取值，计算查询变量取值的后验概率
      */
    //  def Enumeration_Process(bn: BayesianNetwork, Evidences: Array[InferenceVariable], EvidenceNum: Int, QueryVariable: InferenceVariable):Double = {
    def Enumeration_Process(bn: BayesianNetwork, Evidences: Array[InferenceVariable], EvidenceNum: Int, QueryVariable: InferenceVariable, QueryVariable_probability: Array[Double], QueryVariable_r: Int) = {
        var Numerator = 0.0       //概率推理公式中的分子

        val Node = new Array[Int](bn.NodeNum)           //存储证据变量与查询变量的具体取值，初始值为-1
        for (i <- 0 until bn.NodeNum)
            Node(i) = -1
        for (i <- 0 until EvidenceNum)
            Node(Evidences(i).sequence - 1) = Evidences(i).value
        //    Node(QueryVariable.sequence - 1) = QueryVariable.value
        Node(QueryVariable.sequence - 1) = 1
        /** 找出既非证据变量也非查询变量的节点序号，存储于MendedNode数组中 */
        if (bn.NodeNum - EvidenceNum - 1 > 0) {
            var MendedSampleNum = 1                           //MendedSampleNum记录与查询变量当前取值相关的补后样本数
            val MendedNode = new Array[Int](bn.NodeNum - EvidenceNum - 1)
            for (i <- 0 until MendedNode.length)
                MendedNode(i) = -1
            var v = 0
            for (i <- 0 until bn.NodeNum)
                if (-1 == Node(i)) {
                    MendedNode(v) = i
                    v += 1
                }

            /** 根据数组MendedNode以及其中对应节点的势，将证据变量取值与查询变量取值修补为完整样本
              * 利用补后样本可得出概率推理中求和算子中每个具体因式对应的联合概率分布
              */
            for (i <- 0 until MendedNode.length)
                MendedSampleNum *= bn.r(MendedNode(i))
//                  println("============MendedNode============")
//                  for (i <- 0 until MendedNode.length)
//                    println(MendedNode(i))
//                  println("============MendedSampleNum============")
//                  println(MendedSampleNum)
//                  System.exit(0)
            val MendedSample = new Array[Array[Int]](MendedSampleNum)    //存储补后样本对应的节点值——MendedSample[0]存储第一行补后样本对应的所有节点值——MendedSample[0][0]存储第一行中的第一个节点的取值
            for (i <- 0 until MendedSampleNum) {
                MendedSample(i) = new Array[Int](bn.NodeNum)
            }
            for (i <- 0 until (MendedNode.length - 1) ) {
                Node(MendedNode(i)) = 1
            }

            var i = MendedNode.length - 1
            Node(MendedNode(i)) = 0                              //待修补节点的初始值为0
            for (v <- 0 until MendedSampleNum) {
                Node(MendedNode(i)) += 1
                breakable {
                    while (i >= 0) {
                        if (Node(MendedNode(i)) <= bn.r(MendedNode(i)) ) {
                            Node.copyToArray(MendedSample(v))           //将当前补后样本存储于MendedSample[v]中
                            i = MendedNode.length - 1                   //始终保持从MendedNode数组中的最后一个节点开始进1修补
                            break()
                        }
                        else {       //当Node(MendedNode(i)) > bn.r(MendedNode(i)时，将当前节点值置为1；然后，向前一节点的值进1。
                            Node(MendedNode(i)) = 1
                            i -= 1
                            Node(MendedNode(i)) += 1
                        }
                    }
                }
            }
            //输出查询变量当前取值对应的补后样本
//            println(MendedSampleNum)
//              for (i <- 0 until MendedSampleNum) {
//                for (j <- 0 until bn.NodeNum)
//                  print(MendedSample(i)(j) + ",")
//                println()
//              }
//              System.exit(0)

            /** 计算每行补后样本MendedSample[i]对应的联合概率分布和概率推理公式中的分子 */
            for (i <- 0 until MendedSampleNum) {
                val theta_temp = new Array[Double](bn.NodeNum)   //存储补后样本对应的n个θijk的值
//                for(t <-MendedSample(i))
//                    print(t + ",")
                ijk_computation(MendedSample(i), bn, theta_temp)  //计算每行补后样本所对应的参数θijk,其中theta_temp (θijk) = BN参数
                var theta_product = 1.0                          //存储补后样本对应的联合概率分布值，即n个θijk的乘积
                for (j <- 0 until bn.NodeNum)
                    theta_product *= theta_temp(j)
                Numerator += theta_product
            }
            QueryVariable_probability(0) = Numerator

            /** 计算概率推理公式中的分母
              * 查询变量非当前取值相关的补后样本，称为剩余补后样本
              */
            for (t <- 2 to QueryVariable_r){
                Numerator = 0.0
                for (i <- 0 until MendedSampleNum) {
                    MendedSample(i)(QueryVariable.sequence - 1) = t
                    val theta_temp = new Array[Double](bn.NodeNum)   //存储补后样本对应的n个θijk的值
                    ijk_computation(MendedSample(i), bn, theta_temp)  //计算每行补后样本所对应的参数θijk,其中theta_temp (θijk) = BN参数
                    var theta_product = 1.0                          //存储补后样本对应的联合概率分布值，即n个θijk的乘积
                    for (j <- 0 until bn.NodeNum)
                        theta_product *= theta_temp(j)
                    Numerator += theta_product
                }
                QueryVariable_probability(t-1) = Numerator
            }
        }
        else if (bn.NodeNum - EvidenceNum - 1 == 0) {      //此时分子中无修补样本
            val sample = new Array[Int](bn.NodeNum)          //存储样本对应的节点值，sample[0]存储第一个节点的取值
            Node.copyToArray(sample)
            /** 计算样本sample对应的联合概率分布和概率推理公式中的分子 */
            for (t <- 1 to QueryVariable_r){
                sample(QueryVariable.sequence - 1) = t
                val theta_temp = new Array[Double](bn.NodeNum)   //存储补后样本对应的n个θijk的值
                ijk_computation(sample, bn, theta_temp)  //计算每行补后样本所对应的参数θijk,其中theta_temp (θijk) = BN参数
                var theta_product = 1.0                          //存储补后样本对应的联合概率分布值，即n个θijk的乘积
                for (j <- 0 until bn.NodeNum)
                    theta_product *= theta_temp(j)
                QueryVariable_probability(t-1) = theta_product
            }
        }
        else {  //bn.NodeNum - EvidenceNum - 1 < 0
            Console.err.println("方法\"Enumeration_Process\"中存在错误！Error: bn.NodeNum - EvidenceNum - 1 < 0")
        }
    }

    /** 计算每行补后样本所对应的n个θijk的值 */
    def ijk_computation(t_int: Array[Int], bn: BayesianNetwork, theta_temp: Array[Double]) {
        /*
		t_int——存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
		theta_temp[NUM]——存储每行补后数据对应的n个θijk
		本函数中用t[c][i]表示第c+1行补后样本中的第i+1个变量
		  */
        var int_i = 0;                    //每行样本每个属性值对应的ijk,int_i——θijk中的i值——整数类型, 从0开始计算
        var int_j = 0; var int_k = 0
        val NodeNum = bn.NodeNum

        for (i <- 0 until NodeNum) {  // for1
            int_i = i                                   //int_i——θijk中的i值——数组整数类型
            val pa_t = new Array[Int](NodeNum - 1)     //存放t[c][i]的父节点集,每个节点至多有NUM-1个父节点
            var pa = 0                                    //扫描structure中t[c][i]的父节点列,pa为当前扫描到的父节点
            var k = 0
            while (k < NodeNum - 1 && pa < NodeNum) {
                if (1 == bn.structure(pa)(i)) {
                    pa_t(k) = pa
                    k += 1
                }
                pa += 1
            }
            //存放t[c][i]的父节点pa
            //t[c][i]的父节点共有k个——pa_t[0]...pa_t[k-1]
            /** 计算m_j */
            if (k == 0) {       //k==0说明t[c][i]无父节点
                int_j = 0         //int_j——θijk中的j值——数组整数类型
            }
            else {
                var temp1 = 0; var temp2 = 0       //用于存储计算中间值的临时变量
                for (t <- 0 until k - 1) {
                    temp1 = 1
                    for (j <- t + 1 until k)          //后k-t-1个父节点的势的乘积
                        temp1 *= bn.r(pa_t(j))
                    temp2 += (t_int(pa_t(t)) - 1) * temp1
                }
                temp2 += t_int(pa_t(k - 1))               //temp2即为t[c][i]的父节点的组合情况取值——θijk中的j
                int_j = temp2 - 1                         //int_j——θijk中的j值——数组整数类型
            }
            int_k = t_int(i) - 1                        //int_k——θijk中的k值——数组整数类型
//            println("int_i : " + int_i)
//            println("int_j : " + int_j)
//            println("int_k : " + int_k)
            theta_temp(i) = bn.theta(int_i)(int_j)(int_k)
        }  //end_for1
    }

    /** 枚举法——BN精确推理
      * 根据证据变量取值，计算查询变量取值的后验概率
      */
    def Enumeration_Process1(bn: BayesianNetwork, Evidences: Array[InferenceVariable], EvidenceNum: Int, QueryVariable: Array[InferenceVariable], QueryVariableNum:Int, QueryVariable_probability: Array[Double], c: Int, MendedQueryVariable:Array[Array[Int]]) = {
        var Numerator = 0.0       //概率推理公式中的分子

        val Node = new Array[Int](bn.NodeNum)           //存储证据变量与查询变量的具体取值，初始值为-1
        for (i <- 0 until bn.NodeNum)
            Node(i) = -1
        for (i <- 0 until EvidenceNum)
            Node(Evidences(i).sequence - 1) = Evidences(i).value
        //    Node(QueryVariable.sequence - 1) = QueryVariable.value
        for (i <- 0 until QueryVariableNum)
            Node(QueryVariable(i).sequence - 1) = 1
        /** 找出既非证据变量也非查询变量的节点序号，存储于MendedNode数组中 */
        if (bn.NodeNum - EvidenceNum - QueryVariableNum > 0) {
            var MendedSampleNum = 1                           //MendedSampleNum记录与查询变量当前取值相关的补后样本数
            val MendedNode = new Array[Int](bn.NodeNum - EvidenceNum - QueryVariableNum)
            for (i <- 0 until MendedNode.length)
                MendedNode(i) = -1
            var v = 0
            for (i <- 0 until bn.NodeNum)
                if (-1 == Node(i)) {
                    MendedNode(v) = i
                    v += 1
                }

            /** 根据数组MendedNode以及其中对应节点的势，将证据变量取值与查询变量取值修补为完整样本
              * 利用补后样本可得出概率推理中求和算子中每个具体因式对应的联合概率分布
              */
            for (i <- 0 until MendedNode.length)
                MendedSampleNum *= bn.r(MendedNode(i))
//                              println("============MendedNode============")
//                              for (i <- 0 until MendedNode.length)
//                                println(MendedNode(i))
//                              println("============MendedSampleNum============")
//                              println(MendedSampleNum)
//                              System.exit(0)
            val MendedSample = new Array[Array[Int]](MendedSampleNum)    //存储补后样本对应的节点值——MendedSample[0]存储第一行补后样本对应的所有节点值——MendedSample[0][0]存储第一行中的第一个节点的取值
            for (i <- 0 until MendedSampleNum) {
                MendedSample(i) = new Array[Int](bn.NodeNum)
            }
            for (i <- 0 until (MendedNode.length - 1) ) {
                Node(MendedNode(i)) = 1
            }

            var i = MendedNode.length - 1
            Node(MendedNode(i)) = 0                              //待修补节点的初始值为0
            for (v <- 0 until MendedSampleNum) {
                Node(MendedNode(i)) += 1
                breakable {
                    while (i >= 0) {
                        if (Node(MendedNode(i)) <= bn.r(MendedNode(i)) ) {
                            Node.copyToArray(MendedSample(v))           //将当前补后样本存储于MendedSample[v]中
                            i = MendedNode.length - 1                   //始终保持从MendedNode数组中的最后一个节点开始进1修补
                            break()
                        }
                        else {       //当Node(MendedNode(i)) > bn.r(MendedNode(i)时，将当前节点值置为1；然后，向前一节点的值进1。
                            Node(MendedNode(i)) = 1
                            i -= 1
                            Node(MendedNode(i)) += 1
                        }
                    }
                }
            }
//            输出查询变量当前取值对应的补后样本
//                        println(MendedSampleNum)
//                          for (i <- 0 until MendedSampleNum) {
//                            for (j <- 0 until bn.NodeNum)
//                              print(MendedSample(i)(j) + ",")
//                            println()
//                          }
//                          System.exit(0)



            /** 计算每行补后样本MendedSample[i]对应的联合概率分布和概率推理公式中的分子 */
            for (i <- 0 until MendedSampleNum) {
                val theta_temp = new Array[Double](bn.NodeNum)   //存储补后样本对应的n个θijk的值
                ijk_computation(MendedSample(i), bn, theta_temp)  //计算每行补后样本所对应的参数θijk,其中theta_temp (θijk) = BN参数
                var theta_product = 1.0                          //存储补后样本对应的联合概率分布值，即n个θijk的乘积
                for (j <- 0 until bn.NodeNum)
                    theta_product *= theta_temp(j)
                Numerator += theta_product
            }
            QueryVariable_probability(0) = Numerator
//            println("QueryVariable_probability(0): " + QueryVariable_probability(0))

            /** 计算概率推理公式中的分母
              * 查询变量非当前取值相关的补后样本，称为剩余补后样本
              */
            for (t <- 1 until c){
                Numerator = 0.0
                for (i <- 0 until MendedSampleNum) {
                    for (j <- 0 until QueryVariableNum)
                        MendedSample(i)(QueryVariable(j).sequence - 1) = MendedQueryVariable(t)(j+1)
//                    MendedSample(i)(QueryVariable.sequence - 1) = t
//                        for (j <- 0 until bn.NodeNum)
//                          print(MendedSample(i)(j) + ",")
//                        println()
                    val theta_temp = new Array[Double](bn.NodeNum)   //存储补后样本对应的n个θijk的值
                    ijk_computation(MendedSample(i), bn, theta_temp)  //计算每行补后样本所对应的参数θijk,其中theta_temp (θijk) = BN参数
                    var theta_product = 1.0                          //存储补后样本对应的联合概率分布值，即n个θijk的乘积
                    for (j <- 0 until bn.NodeNum)
                        theta_product *= theta_temp(j)
                    Numerator += theta_product
                }
                QueryVariable_probability(t) = Numerator
//                println("QueryVariable_probability(0): " + QueryVariable_probability(t-1))
            }
        }
        else if (bn.NodeNum - EvidenceNum - 1 == 0) {      //此时分子中无修补样本
            val sample = new Array[Int](bn.NodeNum)          //存储样本对应的节点值，sample[0]存储第一个节点的取值
            Node.copyToArray(sample)
            /** 计算样本sample对应的联合概率分布和概率推理公式中的分子 */
            for (t <- 0 until c){
                for (i <- 0 until QueryVariableNum)
                    sample(QueryVariable(i).sequence - 1) = MendedQueryVariable(t)(i+1)
//                    sample(QueryVariable.sequence - 1) = t
                val theta_temp = new Array[Double](bn.NodeNum)   //存储补后样本对应的n个θijk的值
                ijk_computation(sample, bn, theta_temp)  //计算每行补后样本所对应的参数θijk,其中theta_temp (θijk) = BN参数
                var theta_product = 1.0                          //存储补后样本对应的联合概率分布值，即n个θijk的乘积
                for (j <- 0 until bn.NodeNum)
                    theta_product *= theta_temp(j)
                QueryVariable_probability(t-1) = theta_product
            }
        }
        else {  //bn.NodeNum - EvidenceNum - 1 < 0
            Console.err.println("方法\"Enumeration_Process\"中存在错误！Error: bn.NodeNum - EvidenceNum - 1 < 0")
        }
    }




    def preferenceInferenceProcess_strategy() {
        println("---------------11111111111111111----------------------")

        /********************更换【测试数据集】一定要记得修改EvidenceNum与Evidences*****************/
        if(reference.data_type == "ml-1m"){
            reference.user_Attribution_num = 294   //用户属性取值组合数
            reference.usercount = 6040
        }else if(reference.data_type == "Clothing-Fit-Data") {
            reference.user_Attribution_num = 117649 //用户属性取值组合数
            reference.usercount = 77347
        }
        // 对每种用户属性类型，构建三维数组，   每维偏好 的 每个取值 的 5种评分
        val P_R = new Array[Array[Array[Double]]](reference.user_Attribution_num+1)        //定义一个四维可变数组theta
        // P_R(user_Attribution_num+1)(reference.LatentNum_Total+1)(reference.Latent_r(i)+1)(rating+1)
        // P_R(294+1)(1+1)(18+1)(6+1)
        //        println(user_Attribution_num)
        //        println(reference.LatentNum_Total)
        //        println(reference.Latent_r(0))
        //        System.exit(0)
        for (i <- 1 until reference.user_Attribution_num+1) {
            P_R(i) = new Array[Array[Double]](reference.LatentNum_Total+1)
            //            println(i)
            for (j <- 1 until reference.LatentNum_Total+1){
                P_R(i)(j) = new Array[Double](reference.Latent_r(j-1)+1)
                //                println(i + " " + j)
            }
        }
        // 每个用户属性类型，构建三维数组，   每维偏好 的 每个取值 的 5种评分 概率
        val P_R_6040 = new Array[Array[Array[Double]]](reference.usercount+1)
        // 每个用户属性类型，构建三维数组，   每维偏好 的 每个取值 的 5种评分 评分次数
        val P_R_number_6040 = new Array[Array[Array[Array[Int]]]](reference.usercount+1)
        for (i <- 1 until reference.usercount+1) {
            P_R_6040(i) = new Array[Array[Double]](reference.LatentNum_Total+1)
            P_R_number_6040(i) = new Array[Array[Array[Int]]](reference.LatentNum_Total+1)
            //            println(i)
            for (j <- 1 until reference.LatentNum_Total+1){
                P_R_6040(i)(j) = new Array[Double](reference.Latent_r(j-1)+1)
                P_R_number_6040(i)(j) = new Array[Array[Int]](reference.Latent_r(j-1)+1)
                //                println(i + " " + j)
                for (k <- 1 until reference.Latent_r(j-1)+1){
                    P_R_number_6040(i)(j)(k) = new Array[Int](5+1)
//                                        println(i + " " + j + " " + k)
                }
            }
        }
        //        System.exit(0)
        // 取出每个OPBN
        var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
        if(reference.data_type == "ml-1m"){
            for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
                bn_Array(i) = new BayesianNetwork(reference.NodeNum)
                bn_Array(i).r(0) = 2; bn_Array(i).r(1) = 7; bn_Array(i).r(2) = 21;
                bn_Array(i).r(3) = 5;
                bn_Array(i).r(4) = reference.Latent_r(i);
                bn_Array(i).r(5) = reference.Latent_r(i);
                bn_Array(i).l(0) = "U"; bn_Array(i).l(1) = "U"; bn_Array(i).l(2) = "U";
                bn_Array(i).l(3) = "R";
                bn_Array(i).l(4) = "I";
                bn_Array(i).l(5) = "L";
                bn_Array(i).latent(5) = 1;
            }
        }else if(reference.data_type == "Clothing-Fit-Data") {
            for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
                bn_Array(i) = new BayesianNetwork(reference.NodeNum)
                bn_Array(i).r(0) = 7; bn_Array(i).r(1) = 7; bn_Array(i).r(2) = 7;bn_Array(i).r(3) = 7; bn_Array(i).r(4) = 7; bn_Array(i).r(5) = 7;
                bn_Array(i).r(6) = 5;
                bn_Array(i).r(7) = reference.Latent_r(i);
                bn_Array(i).r(8) = reference.Latent_r(i);
                bn_Array(i).l(0) = "U"; bn_Array(i).l(1) = "U"; bn_Array(i).l(2) = "U";bn_Array(i).l(3) = "U"; bn_Array(i).l(4) = "U"; bn_Array(i).l(5) = "U";
                bn_Array(i).l(6) = "R";
                bn_Array(i).l(7) = "I";
                bn_Array(i).l(8) = "L";
                bn_Array(i).latent(8) = 1;
            }
        }
        for(i <- 0 until reference.LatentNum_Total){
            //修改文件名
            var temp_string = (i+1).toString + ".txt"
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            //            outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
            //            outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
            outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
            //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
            val input = new BN_Input
            //从文件中读取表示BN初始结构的邻接矩阵到bn.structure中
            input.structureFromFile(outputFile.optimal_structure, separator.structure, bn_Array(i))
            bn_Array(i).qi_computation()
            bn_Array(i).create_CPT()
            //从文件中读取参数θ到动态三维数组bn.theta
            input.CPT_FromFile(outputFile.optimal_theta, separator.theta, bn_Array(i))
        }
        println("---------------22222222222222222----------------------")

        var EvidenceNum = 3 //证据变量个数
        if(reference.data_type == "ml-1m"){
            EvidenceNum = 3
        }else if(reference.data_type == "Clothing-Fit-Data") {
            EvidenceNum = 6
        }
        var Evidences = new Array[InferenceVariable](EvidenceNum) //创建一个证据变量数组存储每个证据变量的序号及其取值
        if(reference.data_type == "ml-1m"){
            for (i <- 0 until EvidenceNum) //创建数组中每个对象
                Evidences(i) = new InferenceVariable
            Evidences(0).sequence = 1; //U1
            Evidences(1).sequence = 2; //U2
            Evidences(2).sequence = 3; //U3
        }else if(reference.data_type == "Clothing-Fit-Data") {
            for (i <- 0 until EvidenceNum) //创建数组中每个对象
                Evidences(i) = new InferenceVariable
            val QueryVariable = new InferenceVariable                     //查询变量
            Evidences(0).sequence = 1; //U1
            Evidences(1).sequence = 2; //U2
            Evidences(2).sequence = 3; //U3
            Evidences(3).sequence = 4; //U4
            Evidences(4).sequence = 5; //U5
            Evidences(5).sequence = 6; //U6
        }
        var QueryVariable = new InferenceVariable                     //查询变量
        if(reference.data_type == "ml-1m"){
            QueryVariable.sequence = 6;  //T1
        }else if(reference.data_type == "Clothing-Fit-Data") {
            QueryVariable.sequence = 9;  //T1
        }


        val inFile = SparkConf.sc.textFile(inputFile.inference_test_Data, SparkConf.partitionNum)     //读取测试数据为RDD对象
        reference.TestSampleNum = inFile.count()                                 //测试数据文档中的样本数
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        //        val P_R_flag = new Array[Int](user_Attribution_num)        //定义一个三维可变数组theta
        reference.SampleNum = inFile.count()                         //原始数据文档中的样本数
        var outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("训练数据集 : " + reference.SampleNum + "\r\n\r\n")
        outLog.close()
        // 得到每个测试用户的用户属性
        val user_Data_6040 = SparkConf.sc.textFile(inputFile.inference_learning_User, SparkConf.partitionNum)  //读取原始数据为RDD
        val user_Data_6040_RDD = user_Data_6040.map {    //将每行原始样本映射处理为相应的推理结果
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                val lineStringArray = line.split(separator.data).map( _.toInt )
                val userID = lineStringArray(0)
                var user_attribution = 0
                if(reference.data_type == "ml-1m"){
                    user_attribution = (lineStringArray(1)-1)*7*21 + (lineStringArray(2)-1)*21 + lineStringArray(3)
                }else if(reference.data_type == "Clothing-Fit-Data") {
                    user_attribution = (lineStringArray(1)-1)*7*7*7*7*7 + (lineStringArray(2)-1)*7*7*7*7 + (lineStringArray(3)-1)*7*7*7 + (lineStringArray(4)-1)*7*7 + (lineStringArray(5)-1)*7 + lineStringArray(6)
                }
                (userID, user_attribution)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
        }
        val user_Data_6040_collection = user_Data_6040_RDD.collect()
        //        0 1 158 第一个用户 第158种用户属性组合
        //        1 2 143
//        for(i <- 0 until user_Data_6040_collection.length){
//            println(i + " " + user_Data_6040_collection(i)._1 + " " + user_Data_6040_collection(i)._2)
//        }
//        System.exit(0)
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        println("---------------33333333333333333----------------------")
        // 对每个OPBN，计算
//        for (j <- 0 until reference.LatentNum_Total) {
        for (j <- 0 until reference.LatentNum_Total) {
            breakable{
                if (j == 1 && reference.data_type == "Clothing-Fit-Data") {
                    println("跳过 隐变量： " + (j+1))
                    break
                }
                println("隐变量： " + (j+1))
                val bn = bn_Array(j)
                var learning_User = SparkConf.sc.textFile(inputFile.inference_learning_User_strategy, SparkConf.partitionNum)  //每种用户取值组合的用户属性
                learning_User = learning_User.map {    //将每行原始样本映射处理为相应的推理结果
                    line =>
                        var line1 = ""
                        for (k <- 0 until reference.Latent_r(j)-1){
                            line1 += line + separator.data + (k + 1).toString + separator.data + (k + 1).toString + separator.tab // 填充评分的位置
                        }
                        line1 += line + separator.data + reference.Latent_r(j).toString + separator.data + reference.Latent_r(j).toString
                        line1
                }
                learning_User = learning_User.flatMap(line => line.split(separator.tab))

                //            294,2,7,21,1,1
                //            294,2,7,21,2,2
                //            ...
                //            294,2,7,21,17,17
                //            294,2,7,21,18,18
                //            learning_User.foreach(println(_))
                //            System.exit(0)

                val sampleInferenceRDD = learning_User.map {    //将每行原始样本映射处理为相应的推理结果
                    line =>
                        //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
                        val lineStringArray = line.split(separator.data)
                        val lineIntArray = lineStringArray map ( _.toInt )
                        for (i <- 0 until EvidenceNum) {
                            Evidences(i).value = lineIntArray(Evidences(i).sequence)
                            //                        println(Evidences(i).value)
                        }
                        //                    println(line)
                        //                    System.exit(0)
                        //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)
                        val SampleQueryResult = new InferenceResult(bn.r(QueryVariable.sequence - 1))
                        SampleQueryResult.userID = lineStringArray(0) + separator.data + lineIntArray(Evidences(EvidenceNum-1).sequence) //以 ID,属性  为标记， 用于填表
                        //            1,1  第1种用户组合对M1=1的5种评分概率
                        //            1,2  第1种用户组合对M1=2的5种评分概率
                        //            。。。
                        //            294,18 第294种用户组合对M1=18的5种评分概率
                        //                    println(SampleQueryResult.userID)
                        //                    System.exit(0)
                        for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
                            SampleQueryResult.QueryResult(i) =  new Posterior
                        //                            println("============userID============")
                        //                            println(SampleQueryResult.userID)
                        //                            println("============Evidence============")
                        //                            for (i <- 0 until EvidenceNum)
                        //                              println(Evidences(i).value)
                        //                            System.exit(0)
                        val QueryVariable_probability = new Array[Double](bn.r(QueryVariable.sequence - 1)) //存放查询变量的联合概率  P(Q=q,E)
                        if(reference.inference_type == "MLBN") {
                            Enumeration_Process(bn, Evidences, EvidenceNum, QueryVariable, QueryVariable_probability, bn.r(QueryVariable.sequence - 1))
                        }else if (reference.inference_type == "PNN") {

                        }
                        //                    for (i <- 0 until bn.r(QueryVariable.sequence - 1)) {
                        //                        println(QueryVariable_probability(i))
                        //                    }
                        //                    System.exit(0)
                        val sum = QueryVariable_probability.sum
                        for (i <- 1 to bn.r(QueryVariable.sequence - 1) ) {                      //依次计算不同查询变量取值对应的后验概率
                            QueryVariable.value = i
                            SampleQueryResult.QueryResult(i - 1).value  = QueryVariable.value
                            if(sum == 0)
                                SampleQueryResult.QueryResult(i - 1).probability = 1.0 / 5
                            else
                                SampleQueryResult.QueryResult(i - 1).probability = QueryVariable_probability(i-1) / sum

                        }
                        SampleQueryResult
                }
                //SampleQueryResult存储每行样本对应的推理结果——各用户偏好属性值对应的后验概率
                var SampleQueryResult = sampleInferenceRDD.collect()    //执行action，将数据提取到Driver端。
                println("---------------4444444444444444----------------------")
                //            println("============SampleQueryResult============")
                //            println(SampleQueryResult.length)
                //            for (q <- SampleQueryResult){
                //                println("============userID============")
                //                println(q.userID)
                //                println("============QueryResult============")
                //                for (qq <- q.QueryResult){
                //                    println(qq.value + " " + qq.probability)
                //                }
                //            }
                ////            ============userID============
                ////            294,17
                ////            ============QueryResult============
                ////            1 0.056840182964498207
                ////            2 0.10347232156300407
                ////            3 0.28025781975718
                ////            4 0.37114278463268524
                ////            5 0.18828689108263255
                ////            ============userID============
                ////            294,18
                ////            ============QueryResult============
                ////            1 0.027876871649756222
                ////            2 0.05131554955433919
                ////            3 0.1790203428290585
                ////            4 0.30029796270031806
                ////            5 0.4414892732665281
                //            println(SampleQueryResult.length)
                //            System.exit(0)
                ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                // 每类用户对某一电影类型的每一取值的5种评分的  概率
                //            ============userID============
                //            userID,I
                //            294,17
                //            ============QueryResult============
                //            value probability
                //            1 0.056840182964498207
                //            2 0.10347232156300407
                //            3 0.28025781975718
                //            4 0.37114278463268524
                //            5 0.18828689108263255

                for (q <- SampleQueryResult){
                    val userID = q.userID.split(separator.data)(0).toInt
                    val I = q.userID.split(separator.data)(1).toInt
                    //                println(userID + " " + I)
                    // P_R(user_Attribution_num+1)(reference.LatentNum_Total+1)(reference.Latent_r(i)+1)(rating+1)
                    // P_R(294+1)(1+1)(18+1)(5+1)
                    for (qq <- q.QueryResult){
                        P_R(userID)(j+1)(qq.value) = qq.probability
                        //                    println(qq.value + " " + P_R(userID)(j+1)(I)(qq.value))
                    }
                }
                //            for (userID <- 1 until user_Attribution_num+1) {
                //                for (j <- 1 until reference.LatentNum_Total+1){
                //                    for (k <- 1 until reference.Latent_r(j-1)+1){
                //                        println(userID + " " + j + " " + k + ":::")
                //                        for (l <- 1 until 5+1){
                //                            print(P_R(userID)(j)(k)(l) + " ")
                //                        }
                //                        println()
                //                    }
                //                }
                //            }
                //            System.exit(0)

                // 每个用户对某一电影类型的每一取值的5种评分的  次数
                //        val introduction_Data = SparkConf.sc.textFile(inputFile.inference_introduction_Data, SparkConf.partitionNum)
                //        val introduction_Data = SparkConf.sc.textFile(inputFile.inference_test_Data, SparkConf.partitionNum)
                val introduction_Data = SparkConf.sc.textFile(inputFile.inference_learning_Data, SparkConf.partitionNum)
                val inference_introduction_Data_RDD = introduction_Data.map {    //将每行原始样本映射处理为相应的推理结果
                    line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                        val lineStringArray = line.split(separator.data).map( _.toInt )
                        val userID = lineStringArray(0)

                        //                    // learning_data
                        //                    // ID,U,U,U,R,M1,M2
                        //                    // 1,2,1,11,5,7,7
                        var I = lineStringArray(5+j)
                        if(reference.data_type == "ml-1m"){
                            I = lineStringArray(5+j)
                        }else if(reference.data_type == "Clothing-Fit-Data") {
                            I = lineStringArray(8+j)
                        }

                        // inference_data
                        // ID,U,U,U,R,M,L,R,M,L
                        // 0,1,2,3, 4,5,6,7,8,9
                        // 1,1,7,14,3,3,1,3,8,1
                        //                    val I = lineStringArray(5+3*j)

                        var real_RatingValue = lineStringArray(4)
                        if(reference.data_type == "ml-1m"){
                            real_RatingValue = lineStringArray(4)
                        }else if(reference.data_type == "Clothing-Fit-Data") {
                            real_RatingValue = lineStringArray(7)
                        }
                        (userID, I, real_RatingValue)                                    //SampleQueryResult存储每行样本对应的推理结果——各评分值对应的后验概率
                }



                val inference_introduction_Data_collection = inference_introduction_Data_RDD.collect()
                println("---------------55555555555555555----------------------")
                // 统计每个用户对每种电影属性每种评分的次数
                for(p <- inference_introduction_Data_collection){
                    //                println("userID" + " " + p._1)
                    //                println("I" + " " + p._2)
                    //                println("real_RatingValue" + " " + p._3)
                    P_R_number_6040(p._1)(j+1)(p._2)(p._3) += 1
                }
                //            System.exit(0)


                //            for (userID <- 1 until 2) {
                //                for (k <- 1 until reference.Latent_r(j)+1){ //电影属性的势
                //                    var line = ""
                //                    line += userID + "-" + (j+1) + "-" + k
                //                    for (l <- 1 until 5+1){
                //                        line += " " + P_R_number_6040(userID)(j+1)(k)(l)
                //                    }
                //                    println(line)
                //                }
                //            }
                //
                //            for (userID <- 1 until 2) {
                //                val user_attrition = user_Data_6040_collection(userID-1)._2  // 每个用户属于294种用户类型中的哪一个
                //                var line = ""
                //                line += userID + "-" + (j+1) + ":"
                //                var line2 = ""
                //                line2 += userID + "-" + (j+1) + ":"
                //                var line3 = ""
                //                line3 += userID + "-" + (j+1) + ":"
                //                for (k <- 1 until reference.Latent_r(j)+1) { //电影属性的势
                //                    line += " " + P_R(user_attrition)(j+1)(k)
                //                    line2 += " " + P_R_number_6040(userID)(j+1)(k).sum
                //                    var weight_rating = 0
                //                    for (l <- 1 until 5+1){
                //                        weight_rating += P_R_number_6040(userID)(j+1)(k)(l) * l
                //                    }
                //                    line3 += " " + weight_rating
                //                }
                //                println(line)
                //                println(line2)
                //                println(line3)
                //            }

                outputFile.PreferenceResult_Test = "Inference/L2/log/PreferenceResult_Test" +  (j+1).toString + ".txt"
                var outLog = new FileWriter(outputFile.PreferenceResult_Test)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
                outLog.close()
                outLog = new FileWriter(outputFile.PreferenceResult_Test, true)    //以追加写入的方式创建FileWriter对象
                // 每个用户对某一电影类型的偏好概率
                for (userID <- 1 until reference.usercount + 1) {
                    //        user_Data_6040_collection
                    //        0 1 158 第一个用户 第158种用户属性组合
                    //        1 2 143
                    var line = ""
                    line += userID
                    val user_attrition = user_Data_6040_collection(userID-1)._2  // 每个用户属于294种用户类型中的哪一个
                    for (k <- 1 until reference.Latent_r(j)+1){ //电影属性的势
                        var weight_rating = 0
                        for (l <- 1 until 5+1){
                            weight_rating += P_R_number_6040(userID)(j+1)(k)(l) * l
                        }
                        P_R_6040(userID)(j+1)(k) = P_R(user_attrition)(j+1)(k) * (weight_rating + 1)
//                      P_R_6040(userID)(j+1)(k) = P_R(user_attrition)(j+1)(k) * (1)
//                      println(P_R_6040(userID)(j+1)(k) + " = "  + P_R(user_attrition)(j+1)(k) +  " * "  + (weight_rating + 1))
                    }
                    var sum = P_R_6040(userID)(j+1).sum
                    //                print(sum)
                    for (k <- 1 until reference.Latent_r(j)+1){ //电影属性的势
                        P_R_6040(userID)(j+1)(k) = (P_R_6040(userID)(j+1)(k) / sum)
                        val temp:String = P_R_6040(userID)(j+1)(k).formatted("%.4f")
                        line += separator.data + temp
                    }
                    outLog.write(line + "\r\n")

                }
                outLog.close()
            }


        }
        println("---------------666666666666666666----------------------")

        outputFile.PreferenceResult_Test = "Inference/L2/log/usersPreference.txt"
        outLog = new FileWriter(outputFile.PreferenceResult_Test)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.close()
        outLog = new FileWriter(outputFile.PreferenceResult_Test, true)    //以追加写入的方式创建FileWriter对象
        // 每个用户对某一电影类型的偏好概率
        for (userID <- 1 until reference.usercount + 1) {
            var line = ""
            line += userID
            val user_attrition = user_Data_6040_collection(userID-1)._2  // 每个用户属于294种用户类型中的哪一个
            for (j <- 0 until reference.LatentNum_Total){
                breakable{
                    if (j == 1 && reference.data_type == "Clothing-Fit-Data") {
                        println("跳过 隐变量： " + (j+1))
                        break
                    }
                    println("隐变量： " + (j+1))
                    for (k <- 1 until reference.Latent_r(j)+1){ //电影属性的势
                        val temp:String = P_R_6040(userID)(j+1)(k).formatted("%.4f")
                        line += "::" + temp
                    }
                }
            }
            outLog.write(line + "\r\n")
        }
        outLog.close()
    }






}




