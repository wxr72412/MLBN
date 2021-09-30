package BNLV_learning.Learning

import BNLV_learning.Global._
import BNLV_learning.Learning.ParameterLearning.ijk_computation
import BNLV_learning.{BayesianNetwork, M_table}
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.math.log
import scala.util.control.Breaks._

/**
  * Created by Gao on 2016/8/31.
  */

object SEM_F {

	def Maxlikelihood_F(bn: BayesianNetwork) {

//			println("========================结构学习 完整数据 及其 P(L|D)==============")
//			reference.MendedData.foreach(println(_))
//			System.exit(0)
	//        ========================完整数据 及其 P(L|D)==============
	//        1,1,1,1 0.23076923076923073
	//        1,1,1,2 0.15384615384615385
	//        1,1,2,1 0.3692307692307692
	//        1,1,2,2 0.24615384615384617

		//mendedData——读取碎权补后数据RDD
		val m_ijk = reference.MendedData.map {
			line =>
				val mended_sample = line.split(separator.space)              //补后样本值mended_sample(0)，权重mended_sample(1)，空格为分隔符
				val m_ijk = new Array[String](bn.change_node_num)
				val variable_value = mended_sample(0).split(separator.data)
				val t_int = variable_value map ( _.toInt )                   //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
				ijk_ML_F(t_int, bn, m_ijk)                                     //计算每行补后样本所对应的期望统计量m_ijk标号

				var lines = ""
				for (i <- 0 until bn.change_node_num - 1){
					lines += m_ijk(i) + separator.space + mended_sample(1)
					lines += separator.tab
				}
				lines += m_ijk(bn.change_node_num - 1) + separator.space + mended_sample(1)
				lines
		}
//		m_ijk.foreach(println(_))
//		System.exit(0)
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
		val M_ijk_pair = M_ijk_pairRDD.collect()                       //执行action，将数据提取到Driver端
//		println("-------------------------------------------------")
//		M_ijk_pair.foreach(println(_))
//		System.exit(0)
		for(p <- M_ijk_pair){
			val ijk = p._1.split("-").map(_.toInt)
			for (i <- 0 until bn.change_node_num) {
				if ((ijk(0)-1) == bn.change_node(i)){
					bn.M_ijk(i)(ijk(1) - 1)(ijk(2) - 1) = p._2
//					println(i + " " + bn.change_node(i) + " " + (ijk(1) - 1) + " " + (ijk(2) - 1))
//					println(p._1 + "," + p._2)
				}
			}
		}
//		System.exit(0)
	//	println("-------------------------------------------------")
		/** 根据M_ijk更新参数theta */

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//		//后处理参数
//		/** 根据M_ijk更新参数theta */
//		var k_Theta = new ArrayBuffer[Double]
//		var temp = 0.0
//		for (i <- 0 until bn.change_node_num){
//			for (j <- 0 until bn.q(bn.change_node(i))) {
//				temp = 0.0
//				for (k <- 0 until bn.r(bn.change_node(i))) {
//					temp += bn.M_ijk(i)(j)(k)
//				}
//				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//				if(temp != 0){
//					for (k <- 0 until bn.r(bn.change_node(i)) )
//						bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
//					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//					k_Theta.clear()
//					for (k <- 0 until bn.r(bn.change_node(i)) )
//						k_Theta += bn.theta(i)(j)(k)
//					val max = k_Theta.max
//					val min = k_Theta.min
//					if(min == 0){
//						println("后处理参数 : " + (i+1) + " " + (j+1))
//						if(max == 1){
//							for (t <- 0 until k_Theta.length ) {
//								if(k_Theta(t) == 1){
//									k_Theta(t) = 2.0/ (bn.r(i)+1)
//								}else{
//									k_Theta(t) = 1.0/ (bn.r(i)+1)
//								}
//							}
//						}else{
//							var min_no0 = 1.0
//							var temp0 = 0.0
//							// 找到非0的最小值 min_no0
//							for (t <- 0 until k_Theta.length ) {
//								if(k_Theta(t) != 0 && k_Theta(t) < min_no0){
//									min_no0 = k_Theta(t)
//								}
//							}
//							// 令参数0等于非0的最小值 min_no0
//							for (t <- 0 until k_Theta.length ) {
//								if(k_Theta(t) == 0){
//									k_Theta(t) = min_no0
//								}
//								temp0 += k_Theta(t)
//							}
//							// 重新计算参数
//							for (t <- 0 until k_Theta.length ) {
//								k_Theta(t) = k_Theta(t) / temp0
//							}
//						}
//						for (k <- 0 until bn.r(bn.change_node(i)) ) {
//							bn.theta(i)(j)(k) = k_Theta(k)
//						}
//					}
//					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//				}else{
//					for (k <- 0 until bn.r(bn.change_node(i)) )
//						bn.theta(i)(j)(k) = 1.0 / bn.r(bn.change_node(i))
//				}
//				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//			}
//		}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////
		for (i <- 0 until bn.change_node_num){
			for (j <- 0 until bn.q(bn.change_node(i))) {
				for (k <- 0 until bn.r(bn.change_node(i))) {
					if(bn.M_ijk(i)(j)(k) == 0)
						bn.M_ijk(i)(j)(k) += 0.1
					bn.M_ijk(i)(j)(k) *= reference.BIC_data_number
				}
			}
		}

		var temp = 0.0
//		var k_Theta = new ArrayBuffer[Double]
		for (i <- 0 until bn.change_node_num){
			for (j <- 0 until bn.q(bn.change_node(i))) {
				temp = 0.0
				for (k <- 0 until bn.r(bn.change_node(i))) {
					temp += bn.M_ijk(i)(j)(k)
				}

				if(temp != 0){
					for (k <- 0 until bn.r(bn.change_node(i)) )
						bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
				}else{
					for (k <- 0 until bn.r(bn.change_node(i)) )
						bn.theta(i)(j)(k) = 1.0 / bn.r(bn.change_node(i))
				}
			}
		}


	}


	def Maxlikelihood_F_no_latent(bn: BayesianNetwork) {
		println("Maxlikelihood_F_no_latent")
		//reference.Data——完整数据RDD
		val m_ijk = reference.Data.map {
			line =>
				val m_ijk = new Array[String](bn.change_node_num)
				val variable_value = line.split(separator.data)
				val t_int = variable_value map ( _.toInt )                   //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
				ijk_ML_F(t_int, bn, m_ijk)                                     //计算每行样本所对应的统计量m_ijk标号

				var lines = ""
				for (i <- 0 until bn.change_node_num - 1){
					lines += m_ijk(i) + separator.space + 1
					lines += separator.tab
				}
				lines += m_ijk(bn.change_node_num - 1) + separator.space + 1
				lines
		}
//				m_ijk.foreach(println(_))
//				System.exit(0)
		val m_ijk_data = m_ijk.flatMap(line => line.split(separator.tab))

		val m_ijk_pair = m_ijk_data map {          //将m_ijk标号及其权重值映射为(key,value)格式——(m_ijk标号,权重)
			line =>
				val temp = line.split(separator.space)
				val tag = temp(0)
				val value = temp(1).toDouble
				(tag, value)
		}
		val M_ijk_pairRDD = m_ijk_pair.reduceByKey((x, y) => x + y)    //统计m_ijk到对应的M_ijk中
		val M_ijk_pair = M_ijk_pairRDD.collect()                       //执行action，将数据提取到Driver端
//		println("-------------------------------------------------")
//		M_ijk_pair.foreach(println(_))
//				System.exit(0)
		for(p <- M_ijk_pair){
			val ijk = p._1.split("-").map(_.toInt)
			for (i <- 0 until bn.change_node_num) {
				if ((ijk(0)-1) == bn.change_node(i)){
					bn.M_ijk(i)(ijk(1) - 1)(ijk(2) - 1) = p._2
//					println(i + " " + bn.change_node(i) + " " + (ijk(1) - 1) + " " + (ijk(2) - 1))
//					println(p._1 + "," + p._2)
				}
			}
		}
//		System.exit(0)

		for (i <- 0 until bn.change_node_num){
			for (j <- 0 until bn.q(bn.change_node(i))) {
				for (k <- 0 until bn.r(bn.change_node(i))) {
					if(bn.M_ijk(i)(j)(k) == 0)
						bn.M_ijk(i)(j)(k) += 0.1
				}
			}
		}

		var temp = 0.0
		for (i <- 0 until bn.change_node_num){
			for (j <- 0 until bn.q(bn.change_node(i))) {
				temp = 0.0
				for (k <- 0 until bn.r(bn.change_node(i))) {
					temp += bn.M_ijk(i)(j)(k)
				}

				if(temp != 0){
					for (k <- 0 until bn.r(bn.change_node(i)) )
						bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
				}else{
					for (k <- 0 until bn.r(bn.change_node(i)) )
						bn.theta(i)(j)(k) = 1.0 / bn.r(bn.change_node(i))
				}
			}
		}


	}


	/** 计算每行补后样本所对应的期望统计量m_ijk标号 */
	def ijk_ML_F(t_int: Array[Int], bn: BayesianNetwork, m: Array[String]) {
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
		var i = 0
		for (ii <- 0 until bn.change_node_num) {  // for1\
			i = bn.change_node(ii)
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
			m(ii) = m_i + "-" + m_j + "-" + m_k          //i-j-k
		}  //end_for1
	}

}
