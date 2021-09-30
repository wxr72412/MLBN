package BNLV_learning.Learning

import java.io.FileWriter

import BNLV_learning.Global.{logFile, _}
import BNLV_learning.Learning.ParameterLearning.{ijk_ML, ijk_computation}
import BNLV_learning.{BayesianNetwork, M_table}
import BNLV_learning.Output.BN_Output
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.math.log
import scala.util.control.Breaks._

/**
  * Created by Gao on 2016/8/31.
  */

object SEM {

	def PD(bn: BayesianNetwork):Double = {

		val inFile = reference.Data
		var PD_accumulator = SparkConf.sc.accumulator(0.0)   //初始值为0
		val c = reference.c
		val complete_data = inFile.map {  //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
			line =>                         //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
				val t = line.split(separator.tab)
				val theta_product = new Array[Double](c) //每行补后数据对应的n个θijk的乘积
				val m = new Array[Array[String]](c) //存储c行补后数据对应的期望统计量m_ijk——字符串类型，大小为NUM的数组m[0]存储第一行对应的所有m_ijk标记——m[0][0]存储第一行的第一个属性值对应的m_ijk标号
				for (i <- 0 until c) {
					m(i) = new Array[String](bn.NodeNum)
				}
				/** 考虑每行补后数据t[i]，并计算相应的m_ijk */
				for (i <- 0 until c) {
					val tokens = t(i).split(separator.data)
					val t_int = tokens.map(_.toInt) //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
					val theta_temp = new Array[Double](bn.NodeNum) //存储每行补后数据对应的n个θijk的值
					ijk_computation(t_int, bn, m(i), theta_temp) //计算每行补后样本所对应的期望统计量m_ijk,其中theta_temp (θijk) = EM初始参数
					theta_product(i) = 1
					for (j <- 0 until bn.NodeNum)
						theta_product(i) *= theta_temp(j)
				}
				/** 计算每行补后数据的权重,共c行 */
				//val weight = new Array[Double](c) //存储每行补后数据的权重weight[i]
				var temp1 = 0.0;
				//var temp2 = 0.0;
				for (i <- 0 until c)
					temp1 += theta_product(i)
				///////////////////////////P(D)////////////////////////////////
				PD_accumulator += (log(temp1)*reference.BIC_data_number)    //P(D)
				///////////////////////////P(D)////////////////////////////////
//				for (i <- 0 until c - 1)
//					weight(i) = theta_product(i) / temp1
//				for (i <- 0 until c - 1) //归一化每个原始样本的最后一个补后样本权重
//					temp2 += weight(i) //temp2——前c-1个补后样本的权重之和
//				weight(c - 1) = 1 - temp2
//
//				var lines = ""
//				for (i <- 0 until c - 1) {
//					lines = lines + t(i) + separator.space + weight(i) //以空格符分隔补后数据与其权重值
//					lines += separator.tab //以制表符分隔每条补后数据
//				}
//				lines += t(c - 1) + separator.space + weight(c - 1)
//				lines
		} //END_mended_data = inFile.map
		complete_data.count()
		PD_accumulator.value
	}




def Maxlikelihood(bn: BayesianNetwork, M_ijk: Array[M_table], Theta_ijk: Array[Double], ThetaNum: Int) {

//		println("========================结构学习 完整数据 及其 P(L|D)==============")
//		mendedData.foreach(println(_))
//        ========================完整数据 及其 P(L|D)==============
//        1,1,1,1 0.23076923076923073
//        1,1,1,2 0.15384615384615385
//        1,1,2,1 0.3692307692307692
//        1,1,2,2 0.24615384615384617

	//mendedData——读取碎权补后数据RDD
	val m_ijk = reference.MendedData.map {
		line =>
			val mended_sample = line.split(separator.space)              //补后样本值mended_sample(0)，权重mended_sample(1)，空格为分隔符
			val m_ijk = new Array[String](bn.NodeNum)
			val variable_value = mended_sample(0).split(separator.data)
			val t_int = variable_value map ( _.toInt )                   //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
			ijk_ML(t_int, bn, m_ijk)                                     //计算每行补后样本所对应的期望统计量m_ijk标号

			var lines = ""
			for (i <- 0 until bn.NodeNum - 1){
				lines += m_ijk(i) + separator.space + mended_sample(1)
				lines += separator.tab
			}
			lines += m_ijk(bn.NodeNum - 1) + separator.space + mended_sample(1)
			lines
	}
	val m_ijk_data = m_ijk.flatMap(line => line.split(separator.tab))
	//m_ijk_data.take(6).foreach(println)
	val m_ijk_pair = m_ijk_data map {          //将m_ijk标号及其权重值映射为(key,value)格式——(m_ijk标号,权重)
		line =>
		val temp = line.split(separator.space)
		val tag = temp(0)
		val value = temp(1).toDouble
		(tag, value)
	}
	val M_ijk_pairRDD = m_ijk_pair.reduceByKey((x, y) => x + y)    //统计m_ijk到对应的M_ijk中
	M_ijk_pairRDD.persist(StorageLevel.MEMORY_ONLY)
	val M_ijk_realNum = M_ijk_pairRDD.count().toInt
	val M_ijk_pair = M_ijk_pairRDD.collect()                       //执行action，将数据提取到Driver端
	M_ijk_pairRDD.unpersist()
	//M_ijk_pair.foreach(pair => println(pair._1 + "," + pair._2))   //在控制台输出M_ijk的值
//	println("-------------------------------------------------")
//	for(p <- M_ijk_pair){
//		val ijk = p._1.split("-").map(_.toInt)
//		for (i <- 0 until bn.change_node_num) {
//			if (ijk(0) - 1 == bn.change_node(i)){
//				println(p._1 + "," + p._2)
//			}
//		}
//	}
//	println("-------------------------------------------------")

	bn.initialize_M_ijk(M_ijk, ThetaNum)                           //初始化M_ijk的标号及值，每个M_ijk的权重置0
	for (i <- 0 until ThetaNum)
		breakable {
			for (j <- 0 until M_ijk_realNum)
				if (M_ijk(i).tag == M_ijk_pair(j)._1){
					M_ijk(i).value = M_ijk_pair(j)._2
			break()
			}
		}

	/** 根据M_ijk更新参数theta */
	var temp = 0.0; var v = 0 ;var temp1 = 0.0
	var k_Theta = new ArrayBuffer[Double]
	for (i <- 0 until bn.NodeNum)
		for (j <- 0 until bn.q(i) ) {
			temp = 0.0
			for (k <- 0 until bn.r(i) ) {
				v = bn.hash_ijk(i, j, k)
				temp += M_ijk(v).value
			}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			//        for (k <- 0 until bn.r(i) ) {
			//          v = bn.hash_ijk(i, j, k)
			//          Theta_ijk(v) = M_ijk(v).value / temp          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
			//        }
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//			if(temp != 0){
//				for (k <- 0 until bn.r(i) ) {
//					v = bn.hash_ijk(i, j, k)
//					//bn.theta(i)(j)(k) = M_ijk(v).value / temp
//					Theta_ijk(v) = M_ijk(v).value / temp          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
//				}
//			}else{
//				for (k <- 0 until bn.r(i) ) {
//					v = bn.hash_ijk(i, j, k)
//					//bn.theta(i)(j)(k) = 1.0 / bn.r(i)
//					Theta_ijk(v) = 1.0 / bn.r(i)          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
//				}
//			}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			k_Theta.clear()
			if(temp != 0){
				for (k <- 0 until bn.r(i) ) {
					v = bn.hash_ijk(i, j, k)
					Theta_ijk(v) = M_ijk(v).value / temp          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
					k_Theta += Theta_ijk(v)
				}

				//				print(i + "-" + j + " : ")
				//				k_Theta.map(x => print(x + " "))
				//				println()

				val max = k_Theta.max
				val min = k_Theta.min

				//				println("max : "+max+" min "+min)

				if(min == 0){
					if(max == 1){
						for (t <- 0 until k_Theta.length ) {
							if(k_Theta(t) == 1){
								k_Theta(t) = 2.0/ (bn.r(i)+1)          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
							}else{
								k_Theta(t) = 1.0/ (bn.r(i)+1)          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
							}
						}
					}else{
						temp1 = 0.0
						var min_no0 = 1.0
						// 找到非0的最小值 min_no0
						for (t <- 0 until k_Theta.length ) {
							if(k_Theta(t) != 0 && k_Theta(t) < min_no0){
								min_no0 = k_Theta(t)
							}
						}
						// 令参数0等于非0的最小值 min_no0
						for (t <- 0 until k_Theta.length ) {
							if(k_Theta(t) == 0){
								k_Theta(t) = min_no0          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
							}
							temp1 += k_Theta(t)
						}
						// 重新计算参数
						for (t <- 0 until k_Theta.length ) {
							k_Theta(t) = k_Theta(t) / temp1          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
						}
					}
					//					print(i + "-" + j + " : ")
					//					k_Theta.map(x => print(x + " "))
					//					println()
					for (k <- 0 until bn.r(i) ) {
						v = bn.hash_ijk(i, j, k)
						//bn.theta(i)(j)(k) = 1.0 / bn.r(i)
						Theta_ijk(v) = k_Theta(k)          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
					}
				}
			}else{
				for (k <- 0 until bn.r(i) ) {
					v = bn.hash_ijk(i, j, k)
					//bn.theta(i)(j)(k) = 1.0 / bn.r(i)
					Theta_ijk(v) = 1.0 / bn.r(i)          //与EM中不同，SEM.MaxLikelihood直接输出Theta_ijk以便计算期望BIC评分值
				}
			}
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		}
}


	def Maxlikelihood(bn: BayesianNetwork) = {

				//求每个填充数据对应的jik
				val m_ijk = reference.MendedData.map {
					line =>
						val mended_sample = line.split(separator.space) //补后样本值mended_sample(0)，权重mended_sample(1)，空格为分隔符
						val m_ijk = new Array[String](bn.NodeNum)
						val variable_value = mended_sample(0).split(separator.data)
						val t_int = variable_value.map(_.toInt) //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
						ijk_ML(t_int, bn, m_ijk) //计算每行补后样本所对应的期望统计量m_ijk标号
						var lines = ""
						for (i <- 0 until bn.NodeNum - 1) {
							lines += m_ijk(i) + separator.space + mended_sample(1)
							lines += separator.tab
						}
						lines += m_ijk(bn.NodeNum - 1) + separator.space + mended_sample(1)
						//						println(line+"->"+lines)
						lines

				}

				val m_ijk_data = m_ijk.flatMap{
					line =>
						line.split(separator.tab)
				}

				val m_ijk_pair = m_ijk_data.map {                           //将m_ijk标号及其权重值映射为(key,value)格式——(m_ijk标号,权重)
					line =>
						val temp = line.split(separator.space)
						val tag = temp(0)
						val value = temp(1).toDouble
						(tag, value)
				}

				val M_ijk_pairRDD = m_ijk_pair.reduceByKey((x, y) => x + y) //统计m_ijk到对应的M_ijk中


				val M_ijk_pair = M_ijk_pairRDD.collect() //执行action，将数据提取到Driver端

				bn.create_M_ijk() // 重置BN的统计量
				for(p <- M_ijk_pair){
					val ijk = p._1.split("-").map(_.toInt)
					bn.M_ijk(ijk(0)-1)(ijk(1)-1)(ijk(2)-1) = p._2
				}

				for (i <- 0 until bn.NodeNum){
					for (j <- 0 until bn.q(i)) {
						for (k <- 0 until bn.r(i)) {
							if(bn.M_ijk(i)(j)(k) == 0)
								bn.M_ijk(i)(j)(k) += 0.1
							bn.M_ijk(i)(j)(k) *= reference.BIC_data_number
						}
					}
				}

				/** 根据M_ijk更新参数theta */
				var temp = 0.0
				var temp1 = 0.0
				var flag = 0
				var k_Mijk = new ArrayBuffer[Double]
				for (i <- 0 until bn.NodeNum){
					for (j <- 0 until bn.q(i)) {
						temp = 0.0
						flag = 0
						for (k <- 0 until bn.r(i)) {
							temp += bn.M_ijk(i)(j)(k)
						}
						if(temp != 0){
							for (k <- 0 until bn.r(i) ) {
								bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
							}
						}else{
							for (k <- 0 until bn.r(i) ) {
								bn.theta(i)(j)(k) = 1.0 / bn.r(i)
							}
						}
					}
				}

	} //EM迭代结束

	def Maxlikelihood_no_latent(bn: BayesianNetwork) = {

		//求每个填充数据对应的jik
		val m_ijk = reference.MendedData.map {
			line =>
				val mended_sample = line.split(separator.space) //补后样本值mended_sample(0)，权重mended_sample(1)，空格为分隔符
			val m_ijk = new Array[String](bn.NodeNum)
				val variable_value = mended_sample(0).split(separator.data)
				val t_int = variable_value.map(_.toInt) //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
				ijk_ML(t_int, bn, m_ijk) //计算每行补后样本所对应的期望统计量m_ijk标号
			var lines = ""
				for (i <- 0 until bn.NodeNum - 1) {
					lines += m_ijk(i) + separator.space + mended_sample(1)
					lines += separator.tab
				}
				lines += m_ijk(bn.NodeNum - 1) + separator.space + mended_sample(1)
				//						println(line+"->"+lines)
				lines

		}

		val m_ijk_data = m_ijk.flatMap{
			line =>
				line.split(separator.tab)
		}

		val m_ijk_pair = m_ijk_data.map {                           //将m_ijk标号及其权重值映射为(key,value)格式——(m_ijk标号,权重)
			line =>
				val temp = line.split(separator.space)
				val tag = temp(0)
				val value = temp(1).toDouble
				(tag, value)
		}

		val M_ijk_pairRDD = m_ijk_pair.reduceByKey((x, y) => x + y) //统计m_ijk到对应的M_ijk中


		val M_ijk_pair = M_ijk_pairRDD.collect() //执行action，将数据提取到Driver端

		bn.create_M_ijk() // 重置BN的统计量
		for(p <- M_ijk_pair){
			val ijk = p._1.split("-").map(_.toInt)
			bn.M_ijk(ijk(0)-1)(ijk(1)-1)(ijk(2)-1) = p._2
		}

		/** 根据M_ijk更新参数theta */
		var temp = 0.0
		var temp1 = 0.0
		var flag = 0
		var k_Mijk = new ArrayBuffer[Double]
		for (i <- 0 until bn.NodeNum){
			for (j <- 0 until bn.q(i)) {
				temp = 0.0
				flag = 0
				for (k <- 0 until bn.r(i)) {
					temp += bn.M_ijk(i)(j)(k)
				}
				if(temp != 0){
					for (k <- 0 until bn.r(i) ) {
						bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
					}
				}else{
					for (k <- 0 until bn.r(i) ) {
						bn.theta(i)(j)(k) = 1.0 / bn.r(i)
					}
				}
			}
		}

	}



  /** 计算每行补后样本所对应的期望统计量m_ijk标号 */
  def ijk_ML(t_int: Array[Int], bn: BayesianNetwork, m: Array[String]) {
    /*
    t_int——存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
    m_ijk[]——存储每行补后数据对应的期望统计量m_ijk标号i,j,k——整数类型
    theta_temp[NUM]——存储每行补后数据对应的n个θijk
    本函数中用t[c][i]表示第c+1行补后样本中的第i+1个变量
	  */
    var int_i = 0                    //每行样本每个属性值对应的ijk,int_i——θijk中的i值——整数类型, 从0开始计算
    var int_j = 0
    var int_k = 0
    var m_i = ""                      //每行样本每个属性值对应的ijk,m_i——θijk中的i值——字符串类型, 从1开始计算
    var m_j = ""
    var m_k = ""
    val NodeNum = bn.NodeNum

    for (i <- 0 until NodeNum) {  // for1
      int_i = i
      m_i = (int_i + 1).toString                     //m_i——θijk中的i值——字符串类型
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
      if (k == 0) {     //k==0说明t[c][i]无父节点
        int_j = 0
        m_j = "1"       //m_j——θijk中的j值——字符串类型
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
        int_j = temp2 - 1
        m_j = temp2.toString			                //m_j——θijk中的j值——字符串类型
      }
      int_k = t_int(i) - 1
      m_k = t_int(i).toString                     //m_k——θijk中的k值——字符串类型
      m(i) = m_i + "-" + m_j + "-" + m_k          //i-j-k
    }  //end_for1
  }

}
