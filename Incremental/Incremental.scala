package BNLV_learning.Incremental

import java.io.FileWriter
import java.text.NumberFormat
import java.util.Date

import BNLV_learning.Global._
import BNLV_learning.Input.BN_Input
import BNLV_learning.Learning._
import BNLV_learning.Output.BN_Output
import BNLV_learning._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.math.{log, _}
import scala.util.control.Breaks._


//object Incremental extends Serializable {

	/**
	* 数据修补以及权重计算
	* MendedData——补后样本RDD
	* incremental_candidateStructure[][2]——统计所有样本incremental_candidateStructure的权重之和incremental_candidateStructure的二维数组
	* theta_num——期望统计量或参数的总数量
	* separator——横向分隔符的类型
	*/

//def filled_data(bn: BayesianNetwork):RDD[String] = {
//
//	// 填充数据
//	val Ln = reference.LatentNum_Total
//	val Lr = new Array[Int](Ln)
//	for(i <- 0 until Ln){
//		Lr(i) = reference.Latent_r(i)
//	}
//	var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
//	for(i <- 0 until Ln){
//		c *= Lr(i)
//	}
//	reference.c = c
//
//	// var PD_log = 0.0
//
//	val inFile = SparkConf.sc.textFile(inputFile.incremental_Data, SparkConf.partitionNum) //读取原始数据为RDD
//	reference.SampleNum = inFile.count() //原始数据文档中的样本数
//	//println("reference.SampleNum " + reference.SampleNum)
////	inFile.foreach(println(_))
////	System.exit(0)
//
//	// var PD_accumulator = SparkConf.sc.accumulator(0.0) //初始值为0
//	val complete_data = inFile.map { //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
//		line => //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
//			//数据修补
//			val t = new Array[String](c) //存储一条原始样本的c个补后样本
//			var tag_num = 0
//
//			val data_Array = line.split(separator.data)
//			var users = ""
//			for (j <- 0 to 2) {
//				users += data_Array(j) + separator.data
//			}
//			var Lx = Array.ofDim[Int](Ln + 1)
//			var paddingdata_c = ""
//			while (Lx(0) == 0) {
//				var paddingdata = ""
//				for (i <- 1 until Ln) { //1,2,3,....,Ln
//					paddingdata += data_Array(3) + separator.data + data_Array(3+i) + separator.data + (Lx(i) + 1).toString + separator.data
//				}
//				paddingdata += data_Array(3) + separator.data + data_Array(3+Ln) + separator.data + (Lx(Ln) + 1).toString
//				t(tag_num) = users + paddingdata
////				println(t(tag_num))
//				tag_num += 1
//				Lx(Ln) += 1
//				for (i <- 0 until Ln) { //0,1,2,....,Ln-1
//					if (Lx(Ln - i) == Lr(Ln - i - 1)) {
//						Lx(Ln - i) = 0
//						Lx(Ln - i - 1) += 1
//					}
//				}
//			}
//
//
////			Lx = Array.ofDim[Int](Ln + 1)
////
////			while (Lx(0) == 0) {
////				var paddingdata = ""
////				for (i <- 1 until Ln + 1) { //1,2,3,....,Ln
////					paddingdata += separator.data + (Lx(i) + 1).toString
////				}
////				t(tag_num) = line + paddingdata
////				//println(t(tag_num))
////				tag_num += 1
////				Lx(Ln) += 1
////				for (i <- 0 until Ln) { //0,1,2,....,Ln-1
////					if (Lx(Ln - i) == Lr(Ln - i - 1)) {
////						Lx(Ln - i) = 0
////						Lx(Ln - i - 1) += 1
////					}
////				}
////			}
//
//			//			println("====================数据修补====================")
//			//            println("num:" + tag_num)
//			//            ====================数据修补t = Array[String](c)====================
//			//            1,1,1,1
//			//            1,1,1,2
//			//            1,1,2,1
//			//            1,1,2,2
//			//            num:4
//			//System.exit(0)
//			val theta_product = new Array[Double](c) //每行补后数据对应的n个θijk的乘积
//			val m = new Array[Array[String]](c) //存储c行补后数据对应的期望统计量incremental_candidateStructure——字符串类型，大小为NUM的数组m[0]存储第一行对应的所有m_ijk标记——m[0][0]存储第一行的第一个属性值对应的m_ijk标号
//			for (i <- 0 until c) {
//				m(i) = new Array[String](bn.NodeNum)
//			}
//
//			/** 考虑每行补后数据t[i]，并计算相应的m_ijk */
//			for (i <- 0 until c) {
//				val tokens = t(i).split(separator.data)
//				val t_int = tokens.map(_.toInt) //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
//				val theta_temp = new Array[Double](bn.NodeNum) //存储每行补后数据对应的n个θijk的值
//				ijk_computation(t_int, bn, m(i), theta_temp) //计算每行补后样本所对应的期望统计量m_ijk,其中theta_temp (θijk) = EM初始参数
//				theta_product(i) = 1
//				for (j <- 0 until bn.NodeNum)
//					theta_product(i) *= theta_temp(j)
//
////				                println("========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============")
////				                println(t(i))
////				                for(a <- m(i)){print(a+" ")}
////				                println()
////				                for(a <- theta_temp){print(a+" ")}
////				                println()
////				                println(theta_product(i))
//
//			}
////			System.exit(0)
//
//			//            ========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============
//			//            1,1,1,1
//			//            1-1-1 2-1-1 3-1-1 4-1-1
//			//            0.43 0.25 0.6 0.6
//			//            0.0387
//			//            ========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============
//			//            1,1,1,2
//			//            1-1-1 2-1-1 3-1-1 4-1-2
//			//            0.43 0.25 0.6 0.4
//			//            0.025800000000000003
//			//            ========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============
//			//            1,1,2,1
//			//            1-1-1 2-2-1 3-1-2 4-1-1
//			//            0.43 0.6 0.4 0.6
//			//            0.06192
//			//            ========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============
//			//            1,1,2,2
//			//            1-1-1 2-2-1 3-1-2 4-1-2
//			//            0.43 0.6 0.4 0.4
//			//            0.04128000000000001
//
//			/** 计算每行补后数据的权重,共c行 */
//			val weight = new Array[Double](c) //存储每行补后数据的权重weight[i]
//			val flag = new Array[Double](c) //存储每行补后数据的权重weight[i]
//			var temp1 = 0.0;
////			var temp2 = 0.0
//			for (i <- 0 until c)
//				temp1 += theta_product(i)
//			///////////////////////////P(D)////////////////////////////////
//			// PD_accumulator += log(temp1) //P(D)
//			///////////////////////////P(D)////////////////////////////////
//			for (i <- 0 until c){
//				weight(i) = theta_product(i) / temp1
////				println(weight(i))
//			}
////			for (i <- 0 until c) //归一化每个原始样本的最后一个补后样本权重
////				temp2 += weight(i) //temp2——前c-1个补后样本的权重之和
////			println(temp2)
//
//			//var sum = 0.0
//			val cc = 1.0 / c
//			var fill_num = 0
//			//		for(i<- 0 until c)
//			//			print(weight(i)+" ")
//			//		println()
//			for (i <- 0 until c) {
//				//sum += weight(i)
//				if (weight(i) >= cc) {
//					flag(i) = 1
//					fill_num += 1
//				}
//				else
//					flag(i) = 0
//			}
//			//		for(i<- 0 until c)
//			//			print(weight(i)+" ")
//			//		println()
//			//println("cc : "+cc)
//			//println("sum : "+sum)
//			// System.exit(0)
//
//
////			println(line + "  fill_num : "+fill_num + "   " + ccc)
//			var lines = ""
//			for (i <- 0 until c) {
//				if (flag(i) == 1) {
//					fill_num -= 1
//					lines = lines + t(i) + separator.space + 1 //以空格符分隔补后数据与其权重值
////					lines = lines + t(i) + separator.space + weight(i) //以空格符分隔补后数据与其权重值
//					if (fill_num != 0)
//						lines += separator.tab //以制表符分隔每条补后数据
//				}
//			}
//			//		println("========================完整数据 及其 P(L|D)==============")
//			//		println(lines)
//			//		println("fill_num : "+fill_num)
//			//		System.exit(0)
//			//			========================完整数据 及其 P(L|D)==============
//			//			2,1,11,3,9,1,9,1 1
//			//			2,1,11,5,7,2,2,2 1	2,1,11,5,7,2,3,2 1	2,1,11,5,7,2,7,2 1
//			lines
//	} //END_mended_data = inFile.map
//	/*
//      * 返回值lines——complete_data中的每行,包含了每条样本对应的所有补后样本及相应的权重值；
//      * 每条补后样本以separator.tab为分隔符，补后样本数据与权重以separator.space为分隔符。如1,1,1 separator.space W1 separator.tab 1,1,2 separator.space W2
//    */
//	val mended_data = complete_data.flatMap(line => line.split(separator.tab))
//	mended_data
//}

//def compute_theta(bn: BayesianNetwork, mended_data:RDD[String]){
//    //求每个填充数据对应的jik
//    val m_ijk = mended_data.map {
//      line =>
//        val mended_sample = line.split(separator.space) //补后样本值mended_sample(0)，权重mended_sample(1)，空格为分隔符
//        val m_ijk = new Array[String](bn.NodeNum)
//        val variable_value = mended_sample(0).split(separator.data)
//        val t_int = variable_value.map(_.toInt) //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
//        SEM.ijk_ML(t_int, bn, m_ijk) //计算每行补后样本所对应的期望统计量m_ijk标号
//        var lines = ""
//        for (i <- 0 until bn.NodeNum - 1) {
//          lines += m_ijk(i) + separator.space + mended_sample(1)
//          lines += separator.tab
//        }
//        lines += m_ijk(bn.NodeNum - 1) + separator.space + mended_sample(1)
//        lines
//    }
////        println("========================ijk-key P(L|D)==============")
////        for(a <- m_ijk)
////          println(a)
////		  System.exit(0)
////	========================ijk-key P(L|D)==============
////	1-1-2 1	2-1-1 1	3-2-11 1	4-337-3 1	5-9-9 1	6-1-1 1	7-1-9 1	8-1-1 1
////	1-1-2 1	2-1-1 1	3-2-11 1	4-264-5 1	5-2-7 1	6-2-2 1	7-1-2 1	8-1-2 1
////	1-1-2 1	2-1-1 1	3-2-11 1	4-266-5 1	5-3-7 1	6-2-2 1	7-1-3 1	8-1-2 1
////	1-1-2 1	2-1-1 1	3-2-11 1	4-274-5 1	5-7-7 1	6-2-2 1	7-1-7 1	8-1-2 1
//
//    val m_ijk_data = m_ijk.flatMap(line => line.split(separator.tab))
////	for(a <- m_ijk_data)
////	  println(a)
////	  System.exit(0)
////	1-1-2 1
////	1-1-2 1
////	2-1-1 1
////	2-1-1 1
////	3-2-11 1
////	4-264-5 1
////	5-2-7 1
////	3-2-11 1
////	6-2-2 1
////	7-1-2 1
////	8-1-2 1
//
//    val m_ijk_pair = m_ijk_data.map {                           //将m_ijk标号及其权重值映射为(key,value)格式——(m_ijk标号,权重)
//      line =>
//        val temp = line.split(separator.space)
//        val tag = temp(0)
//        val value = temp(1).toDouble
//        (tag, value)
//    }
////	for(a <- m_ijk_pair)
////		println(a)
////	System.exit(0)
////	(1-1-2,1.0)
////	(1-1-2,1.0)
////	(2-1-1,1.0)
////	(2-1-1,1.0)
////	(3-2-11,1.0)
////	(3-2-11,1.0)
////	(4-337-3,1.0)
////	(4-264-5,1.0)
////	(5-2-7,1.0)
////	(5-9-9,1.0)
////	(6-2-2,1.0)
////	(6-1-1,1.0)
////	(7-1-2,1.0)
////	(7-1-9,1.0)
////	(8-1-2,1.0)
////	(8-1-1,1.0)
//
//
//    val M_ijk_pairRDD = m_ijk_pair.reduceByKey((x, y) => x + y) //统计m_ijk到对应的M_ijk中
////	for(a <- M_ijk_pairRDD)
////          println(a)
////	System.exit(0)
////	(5-9-9,1.0)
////	(5-2-7,1.0)
////	(7-1-9,1.0)
////	(4-264-5,1.0)
////	(4-274-5,1.0)
////	(5-7-7,1.0)
////	(4-266-5,1.0)
////	(2-1-1,4.0)
////	(7-1-3,1.0)
////	(1-1-2,4.0)
////	(8-1-2,3.0)
//
//
//      //println("=====PD_accumulator.value 2: "+ PD_accumulator.value)
////    M_ijk_pairRDD.persist(StorageLevel.MEMORY_ONLY)
////    val M_ijk_realNum = M_ijk_pairRDD.count().toInt //从样本中统计得到的实际M_ijk数目，注：M_ijk_realNum很可能小于ThetaNum
//    val M_ijk_pair = M_ijk_pairRDD.collect() //执行action，将数据提取到Driver端
////    M_ijk_pairRDD.unpersist()
//      //println("=====PD_accumulator.value 3: "+ PD_accumulator.value)
//
//
//    //求充分统计量m_ijk
////    bn.initialize_M_ijk(M_ijk, ThetaNum) //初始化M_ijk的标号及值，每个M_ijk的权重置0
////    for (i <- 0 until ThetaNum)
////      breakable {
////        for (j <- 0 until M_ijk_realNum)
////          if (M_ijk(i).tag == M_ijk_pair(j)._1) {
////            M_ijk(i).value = M_ijk_pair(j)._2
////
////            break()
////          }
////      }
//	bn.create_M_ijk() // 重置BN的统计量
//	for(p <- M_ijk_pair){
//		val ijk = p._1.split("-").map(_.toInt)
//		bn.M_ijk(ijk(0)-1)(ijk(1)-1)(ijk(2)-1) = p._2
//	}
////		println("==================充分统计量m_ijk 及 参数===================")
////		for(a <- M_ijk)
////			println(a.tag + " : " + a.value)
////		System.exit(0)
////        ==================充分统计量m_ijk===================
////		1-1-1 : 0.0
////		1-1-2 : 4.0
////		2-1-1 : 4.0
////		2-1-2 : 0.0
////		2-1-3 : 0.0
////		2-1-4 : 0.0
////		2-1-5 : 0.0
////		2-1-6 : 0.0
////		2-1-7 : 0.0
//
//
//	/** 根据M_ijk更新参数theta */
//	var temp = 0.0
//	var temp1 = 0.0
//	var flag = 0
//	var k_Mijk = new ArrayBuffer[Double]
//	var k_Theta = new ArrayBuffer[Double]
//	for (i <- 0 until bn.NodeNum)
//		for (j <- 0 until bn.q(i)) {
//			temp = 0.0
//			for (k <- 0 until bn.r(i)) {
//				temp += bn.M_ijk(i)(j)(k)
//			}
//			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////			if(temp != 0){
////				for (k <- 0 until bn.r(i)){
////					bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
////					if(bn.theta(i)(j)(k) == 0)
////						flag = 1
////				}
////				if(flag == 1){
////					k_Mijk.clear()
////					for (k <- 0 until bn.r(i) ) {
////						k_Mijk += bn.M_ijk(i)(j)(k)
////					}
////					val max = k_Mijk.max
////					//								println("max : "+max)
////					for (t <- 0 until bn.r(i) ) {
////						if(k_Mijk(t) == max){
////							k_Mijk(t) = 2.0
////						}else{
////							k_Mijk(t) = 1.0
////						}
////					}
////					var k_Mijk_sum = k_Mijk.sum
////					for (k <- 0 until bn.r(i))
////						bn.theta(i)(j)(k) = k_Mijk(k) / k_Mijk_sum
////				}
////			}else{
////				for (k <- 0 until bn.r(i) ) {
////					bn.theta(i)(j)(k) = 1.0 / bn.r(i)
////					//Theta_ijk(v) = 1.0 / bn.r(i)
////				}
////			}
//			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//			//                for (k <- 0 until bn.r(i) ) {
//			//                  v = bn.hash_ijk(i, j, k)
//			//                  bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
//			//                }
//			///////////////////////////////////////////////////////////////////////////////////////////////////////////////
//			//            if(temp != 0){
//			//              for (k <- 0 until bn.r(i) ) {
//			//                v = bn.hash_ijk(i, j, k)
//			//                bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
//			//              }
//			//            }else{
//			//              for (k <- 0 until bn.r(i) ) {
//			//                v = bn.hash_ijk(i, j, k)
//			//                bn.theta(i)(j)(k) = 1.0 / bn.r(i)
//			//              }
//			//            }
//			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//			k_Theta.clear()
//			if(temp != 0){
//				for (k <- 0 until bn.r(i) ) {
//					bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
//					k_Theta += bn.theta(i)(j)(k)
//				}
//				val max = k_Theta.max
//				val min = k_Theta.min
//
//				if(min == 0){
//					if(max == 1){
//						for (t <- 0 until k_Theta.length ) {
//							if(k_Theta(t) == 1){
//								k_Theta(t) = 2.0/ (bn.r(i)+1)
//							}else{
//								k_Theta(t) = 1.0/ (bn.r(i)+1)
//							}
//						}
//					}else{
//						temp1 = 0.0
//						var min_no0 = 1.0
//						// 找到非0的最小值 min_no0
//						for (t <- 0 until k_Theta.length ) {
//							if(k_Theta(t) != 0 && k_Theta(t) < min_no0){
//								min_no0 = k_Theta(t)
//							}
//						}
//						// 令参数0等于非0的最小值 min_no0
//						for (t <- 0 until k_Theta.length ) {
//							if(k_Theta(t) == 0){
//								k_Theta(t) = min_no0
//							}
//							temp1 += k_Theta(t)
//						}
//						// 重新计算参数
//						for (t <- 0 until k_Theta.length ) {
//							k_Theta(t) = k_Theta(t) / temp1
//						}
//					}
//					//					print(i + "-" + j + " : ")
//					//					k_Theta.map(x => print(x + " "))
//					//					println()
//					for (k <- 0 until bn.r(i) ) {
//						bn.theta(i)(j)(k) = k_Theta(k)
//						//Theta_ijk(v) = k_Theta(k)
//					}
//				}
//			}else{
//				for (k <- 0 until bn.r(i) ) {
//					bn.theta(i)(j)(k) = 1.0 / bn.r(i)
//					//Theta_ijk(v) = 1.0 / bn.r(i)
//				}
//			}
//			/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//		}
////	println("==================充分统计量m_ijk 及 参数===================")
////	println("ThetaNum : "+ ThetaNum)
////	for(i <- 0 until ThetaNum)
////		println(M_ijk(i).tag + " : " + M_ijk(i).value + "   " + Theta_ijk(i))
////	System.exit(0)
//
//	// PD_log
//}
//
//  /** 计算参数的log似然度 */
//  def Likelihood(bn: BayesianNetwork, M_ijk: Array[M_table], Theta_ijk: Array[Double]): Double = {
//
//    var likelihood = 0.0
//    var v = 0
//    for (i <- 0 until bn.NodeNum)
//      for (j <- 0 until bn.q(i) )
//        for (k <- 0 until bn.r(i) ) {
//          v = bn.hash_ijk(i, j, k)
//          likelihood += M_ijk(v).value * log(Theta_ijk(v) )  //log似然度
//        }
//    likelihood
//  }
//
//  /** 计算每行补后样本所对应的期望统计量m_ijk标号 及 参与计算P(D,L)的θijk值 */
//  def ijk_computation(t_int: Array[Int], bn: BayesianNetwork, m: Array[String], theta_temp: Array[Double]) {
//    /*
//    t_int——存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
//    m_ijk[]——存储每行补后数据对应的期望统计量m_ijk标号i,j,k——整数类型
//    theta_temp[NUM]——存储每行补后数据对应的n个θijk
//    本函数中用t[c][i]表示第c+1行补后样本中的第i+1个变量
//	  */
//    var int_i = 0                      //每行样本每个属性值对应的ijk,int_i——θijk中的i值——整数类型, 从0开始计算
//    var int_j = 0
//    var int_k = 0
//    var m_i = ""                      //每行样本每个属性值对应的ijk,m_i——θijk中的i值——字符串类型, 从1开始计算
//    var m_j = ""
//    var m_k = ""
//    val NodeNum = bn.NodeNum
//
//    for (i <- 0 until NodeNum) {  // for1
//      int_i = i
//      m_i = (int_i + 1).toString                     //m_i——θijk中的i值——字符串类型
//      val pa_t = new Array[Int](NodeNum - 1)     //存放t[c][i]的父节点集,每个节点至多有NUM-1个父节点
//      var pa = 0                                    //扫描structure中t[c][i]的父节点列,pa为当前扫描到的父节点
//      var k = 0
//      while (k < NodeNum - 1 && pa < NodeNum) {    //看bn.structure的第i列，有没有节点指向它
//        if (1 == bn.structure(pa)(i)) {
//          pa_t(k) = pa
//          k += 1
//        }
//        pa += 1
//      }
//      //存放t[c][i]的父节点pa
//      //t[c][i]的父节点共有k个——pa_t[0]...pa_t[k-1]
//      /** 计算m_j */
//      if (k == 0) {     //k==0说明t[c][i]无父节点
//        int_j = 0
//        m_j = "1"       //m_j——θijk中的j值——字符串类型
//      }
//      else {
//        var temp1 = 0; var temp2 = 0       //用于存储计算中间值的临时变量
//        for (t <- 0 until k - 1) {
//          temp1 = 1
//          for (j <- t + 1 until k)          //后k-t-1个父节点的势的乘积
//            temp1 *= bn.r(pa_t(j))
//          temp2 += ( t_int(pa_t(t)) - 1 ) * temp1
//        }
//        temp2 += t_int(pa_t(k - 1))               //temp2即为t[c][i]的父节点的组合情况取值——θijk中的j
//        int_j = temp2 - 1
//        m_j = temp2.toString			                //m_j——θijk中的j值——字符串类型
//      }
//		/* 计算m_k */
//      int_k = t_int(i) - 1
//      m_k = t_int(i).toString                     //m_k——θijk中的k值——字符串类型
//      m(i) = m_i + "-" + m_j + "-" + m_k          //i-j-k
//      theta_temp(i) = bn.theta(int_i)(int_j)(int_k)   //找节点对应的参数
//    }  //end_for1
//  }
//
//}
