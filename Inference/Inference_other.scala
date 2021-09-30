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
import scala.io.Source
import scala.util.control.Breaks._
import scala.io.Source

import java.io.{FileInputStream, ObjectInputStream}

/**
  * Created by Gao on 2017/4/15.
  */

object Inference_other {

    val nf = NumberFormat.getNumberInstance()
    nf.setMaximumFractionDigits(4)                       //设置输出格式保留小数位数

    /** 根据样本自动进行BN推理的过程 */
    def PerferenceInferenceProcess() {

        val t_start = new Date()                                  //获取起始时间对象
        val input = new BN_Input
        val output = new BN_Output

        var outLog = new FileWriter(logFile.inference_log)    //以“非”追加写入的方式创建FileWriter对象——BN推理写对象
        outLog.write("模型推理日志\r\n")
        outLog.write("其他模型结果: " + inputFile.result + "\r\n")
        outLog.write("测试数据集: " + inputFile.inference_test_Data + "\r\n")
        outLog.close()


        println("=====================其他模型结果  处理=====================")
        val inFile = SparkConf.sc.textFile(inputFile.result, SparkConf.partitionNum)  //读取原始数据为RDD
        reference.SampleNum = inFile.count()                         //原始数据文档中的样本数
        outLog = new FileWriter(logFile.inference_log, true)    //以追加写入的方式创建FileWriter对象
        outLog.write("其他模型结果 : " + reference.SampleNum + "\r\n\r\n")
        outLog.close()

        val sampleInferenceRDD = inFile.map {    //将每行原始样本映射处理为相应的推理结果
            line => //line存储从文件中读取的每行数据(String类型)，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
                //lineStringArray(0) = user_id, lineStringArray(i) = Vi的取值——字符串类型
                var lineString = line.split("::")
//                println(lineString(0))

                val lineStringArray = lineString(1).split(separator.data)
                val lineIntArray = lineStringArray.map ( _.toDouble )
//                lineIntArray.foreach(println(_))

                //SampleQueryResult存储user_id以及所有查询变量取值对应的后验概率，lineStringArray(0) = user_id，QueryResult(i) = P(QueryVariable.value = i+1| Evidences)
                val SampleQueryResult = new InferenceResult(18)
                SampleQueryResult.userID = lineString(0)
//                println(SampleQueryResult.userID)

                for (i <- 0 until SampleQueryResult.QueryVariable_Cardinality)
                    SampleQueryResult.QueryResult(i) =  new Posterior
                //        println("============userID============")
                //        println(SampleQueryResult.userID)
                //        println("============Evidence============")
                //        for (i <- 0 until EvidenceNum)
                //          println(Evidences(i).value)
                //        System.exit(0)
                var QueryVariable_probability = new Array[Double](18) //存放查询变量的联合概率  P(Q=q,E)
                QueryVariable_probability = lineIntArray
//                Enumeration_Process(bn, Evidences, EvidenceNum, QueryVariable, QueryVariable_probability, bn.r(QueryVariable.sequence - 1))
                val sum = QueryVariable_probability.sum
                for (i <- 1 to 18 ) {                      //依次计算不同查询变量取值对应的后验概率
                    SampleQueryResult.QueryResult(i - 1).value  = i
                    SampleQueryResult.QueryResult(i - 1).probability = QueryVariable_probability(i-1) / sum
                }
                SampleQueryResult
        }
        //SampleQueryResult存储每行样本对应的推理结果——各用户偏好属性值对应的后验概率
        var SampleQueryResult = sampleInferenceRDD.collect()    //执行action，将数据提取到Driver端

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
//        System.exit(0)
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

        val weight_QueryResult = new Array[Posterior](18)   //存储所有查询变量各取值对应的加权后验概率，weight_QueryResult(i).probability = 评分频率 * P(QueryVariable.value = i+1| Evidences)
        for (i <- 0 until 18) {
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
                for (j <- 0 until 18 )
                    weight_QueryResult(j).probability += SampleQueryResult(i).QueryResult(j).probability
            }
            else {  //SampleQueryResult(i).userID != current_UserID——user_id发生变化
                for (j <- 0 until 18)                     //计算上一个用户的加权偏好
                    weight_QueryResult(j).probability /= userRatingTimes

                val UserPreference = new InferenceResult(18)  //存储用户的加权偏好程度(p1, p2, ...)
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
                for (j <- 0 until 18 ) {
                    weight_QueryResult(j).probability = 0.0      //重置查询变量各取值对应的加权后验概率值
                    weight_QueryResult(j).probability += SampleQueryResult(i).QueryResult(j).probability
                }
            }
            if (i == (reference.SampleNum.toInt - 1) ) {
                for (j <- 0 until 18)
                    weight_QueryResult(j).probability /= userRatingTimes

                val UserPreference = new InferenceResult(18)  //存储用户的加权偏好程度(p1, p2, ...)
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
            sorted_PreferenceResult(i) = new InferenceResult(18)
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
            for (j <- 0 until 18 ) {
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
        model_evaluation_1(UserNum, inputFile.inference_test_Data, sorted_PreferenceResult, 18)

        var outResult = new FileWriter(outputFile.PreferenceResult_Learning)    //以非追加写入的方式创建FileWriter对象
        for (i <- 0 until UserNum) {
            outResult.write(sorted_PreferenceResult(i).userID)
            for (j <- 0 until 18 )
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
        model_evaluation_R(bn, inputFile.inference_test_Data, "max")
        println("评分推理完成！")
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
//                Enumeration_Process1(bn, Evidences, EvidenceNum, QueryVariable, QueryVariableNum, QueryVariable_probability, c, MendedQueryVariable)
//                                println()
//                                for (i <- 0 until c) {
//                                    println(QueryVariable_probability(i))
//                                }
//                                System.exit(0)
                val sum = QueryVariable_probability.sum
                for (i <- 0 until c) { //依次计算不同查询变量取值对应的后验概率
                    SampleQueryResult.QueryResult(i).values = new Array[Int](QueryVariableNum+1)
                    MendedQueryVariable(i).copyToArray(SampleQueryResult.QueryResult(i).values)
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
                        predicted_RatingValue += line._1.QueryResult(i).values(1) * line._1.QueryResult(i).probability
                    }
                }
                else {  //"max" == PredictedRatingType——预测评分值为推理结果中最大后验概率相应的评分变量取值
                    //                    var MaxPosterior_index = 0
                    //                    for (i <- 0 until c - 1 ) {
                    //                        if (line._1.QueryResult(MaxPosterior_index).probability < line._1.QueryResult(i + 1).probability)
                    //                            MaxPosterior_index = i + 1
                    //                    }
                    //                    predicted_RatingValue = line._1.QueryResult(MaxPosterior_index).value

                    val sorted_QueryResult =  line._1.QueryResult.sortWith(_.probability > _.probability)  //sorted_QueryResult存储所有查询变量取值对应的后验概率,按其概率值进行排序
                    val outLog = new FileWriter(logFile.inference_temporary, true)    //以追加写入的方式创建FileWriter对象
                    outLog.write(line._3)

                    for (j <- 0 until c ) {
                        outLog.write("::")
                        for (k <- 1 until  QueryVariableNum+1) //创建数组中每个对象
                            outLog.write("::" + sorted_QueryResult(j).values(k) + " ")
                        outLog.write(" " + sorted_QueryResult(j).probability)
                    }
                    outLog.write("\r\n")
                    outLog.close()
                    predicted_RatingValue = sorted_QueryResult(0).values(1)
//                    println( "predicted_RatingValue " + predicted_RatingValue)
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





}




