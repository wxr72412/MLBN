package BNLV_learning.Learning

import java.io.FileWriter
import java.text.NumberFormat
import java.util.Date

import BNLV_learning.Global._
import BNLV_learning.Input.BN_Input
import BNLV_learning.Learning.StructureLearning.generate_Initial_Structure
import BNLV_learning.Output.BN_Output
import BNLV_learning._
import BNLV_learning.Learning.Search._

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.math.{log, _}
import scala.util.control.Breaks._

object ParameterLearning extends Serializable {

	val nf = NumberFormat.getNumberInstance()
	nf.setMaximumFractionDigits(4)                       //设置输出格式保留小数位数

	def learningProcess(bn: BayesianNetwork) {

		val t_start = new Date()                                  //获取起始时间对象
		val input = new BN_Input
		val output = new BN_Output


		val iteration_EM = reference.EM_iterationNum               //EM迭代次数

		// 填充数据
		val inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
		val Ln = reference.LatentNum
		val Lr = new Array[Int](Ln)
		for(i <- 0 until Ln){
			Lr(Ln-1-i) = bn.r(reference.NodeNum-1-i)
		}
		var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
		for(i <- 0 until Ln){
			c *= Lr(i)
		}
		reference.c = c
		val complete_data = inFile.map { //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
			line => //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
				//数据修补
				//            for (i <- 0 until c) {
				//              latent_value = (i + 1).toString //latent_value = i+1
				//              t(i) = line + separator.data + latent_value //隐变量的值补在每行样本的末端
				//
				//              println(t(i))
				//            }
				//val t = new Array[String](c) //存储一条原始样本的c个补后样本
				var Lx = Array.ofDim[Int](Ln + 1)
				//var tag_num = 0
				var paddingdata_c = ""
				while (Lx(0) == 0) {
					var paddingdata = ""
					for (i <- 1 until Ln + 1) { //1,2,3,....,Ln
						paddingdata += separator.data + (Lx(i) + 1).toString
					}
					//t(tag_num) = line + paddingdata
					//println(t(tag_num))
					paddingdata_c += line + paddingdata + separator.tab
					//tag_num += 1
					Lx(Ln) += 1
					for (i <- 0 until Ln) { //0,1,2,....,Ln-1
						if (Lx(Ln - i) == Lr(Ln - i - 1)) {
							Lx(Ln - i) = 0
							Lx(Ln - i - 1) += 1
						}
					}
				}
				paddingdata_c

			//			println("====================数据修补====================")
			//            println("num:" + tag_num)
			//            ====================数据修补t = Array[String](c)====================
			//            1,1,1,1
			//            1,1,1,2
			//            1,1,2,1
			//            1,1,2,2
			//            num:4

		}
		reference.Data = complete_data
		reference.Data.persist(StorageLevel.MEMORY_AND_DISK_SER)
		reference.SampleNum = reference.Data.count()                         //原始数据文档中的样本数
		//	reference.Data.foreach(println(_))
		//	System.exit(0)
		var outLog = new FileWriter(logFile.parameterLearning)    //以“非”追加写入的方式创建FileWriter对象——参数学习日志写对象
		outLog.write("BNLV参数学习日志\r\n" + "数据集: " + inputFile.primitiveData + " " + reference.SampleNum + "\r\n")
		outLog.write("BNLV结构：\r\n")
		outLog.close()

		//从文件中读取表示BN结构的邻接矩阵到bn.structure中
		//    input.structureFromFile(inputFile.bn_structure, separator.structure, bn)
		//生成初始结构
		generate_Initial_Structure(bn)
		//输出初始结构
		println("==================初始结构==================")
		for (s <- bn.structure){
			for(ss <- s){
				print(ss + " ")
			}
			println()
		}
		//o_structure_console(bn);
		// 按文件追加的方式，输出BN的结构到文件
		output.structureAppendFile(bn, logFile.parameterLearning)

		//计算BN中每个变量其父节点的组合情况数qi
		bn.qi_computation()
		//输出父节点的组合情况数qi
		println("==================父节点的组合情况数qi==================")
		for (s <- bn.q)
			print(s + " ")
		println()
		println("=================节点的取值数ri==================")
		for (s <- bn.r)
			print(s + " ")
		println()

		//创建CPT
		bn.create_CPT()
		//输出CPT
		//    println("========================CPT========================")
		//    for(i <- 0 until bn.theta.length){
		//      for(j <- 0 until bn.theta(i).length){
		//        for(k <- 0 until bn.theta(i)(j).length){
		//          print(bn.theta(i)(j)(k)+" ")
		//        }
		//        println()
		//      }
		//      println("#")
		//    }

		// 创建统计所有样本m_ijk权重之和的M_ijk表
		bn.create_M_ijk()
		var ThetaNum = 0                                        //期望统计量或参数的总数量
		for (i <- 0 until bn.NodeNum)
			ThetaNum += bn.q(i) * bn.r(i)                         //节点的  势 * 父节点取值组合数
		//    val M_ijk = new Array[M_table](ThetaNum)               //创建一个M_ijk表存储每个M_ijk的标号及其权重——所有样本的m_ijk权重统计之和
		//    for (v <- 0 until ThetaNum)                            //创建数组中每个对象
		//      M_ijk(v) =  new M_table
		//    println("========================M_ijk表========================")
		//    println("ThetaNum: " + ThetaNum)       //10
		//    println("M_ijk.length: " + M_ijk.length)   //10
		//    println("M_ijk(0).tag: " + M_ijk(0).tag)   //null
		//    println("M_ijk(0).value: " + M_ijk(0).value) //0.0

		//生成初始参数
		//bn.RandomTheta()                                           //生成随机初始参数
		//bn.RandomTheta_WeakConstraint(bn)                           //生成约束随机初始参数
		input.CPT_FromFile(inputFile.initial_theta_parameterLearning, separator.theta, bn)      //从文件中读取参数θ到动态三维数组bn.theta
		//output.CPT_FormatToConsole(bn)                             //输出参数内容到控制台

		//以追加写入的方式创建FileWriter对象
		outLog = new FileWriter(logFile.parameterLearning, true)
		outLog.write("CPT参数个数 ThetaNum: " + ThetaNum + "\r\n\r\n")
		outLog.write("EM迭代次数: " + iteration_EM + "\r\n\r\n")
		outLog.write("初始CPT:\r\n")
		outLog.close()
		output.CPT_FormatAppendFile(bn, logFile.parameterLearning)

		//EM_Process(bn, M_ijk, ThetaNum, iteration_EM)               //参数估计
		EM_ProcessThreshold(bn, iteration_EM,reference.c)               //参数估计

		reference.Data.unpersist()

		//向文件输出参数学习时间
		val t_end = new Date()                                      //getTime获取得到的数值单位：毫秒数
		val runtime = (t_end.getTime - t_start.getTime)/1000.0
		outLog = new FileWriter(logFile.parameterLearning, true)    //以追加写入的方式创建FileWriter对象
		outLog.write("BNLV参数学习时间: " + runtime + " 秒\r\n\r\n")
		outLog.close()
		val outTime = new FileWriter(logFile.parameterLearningTime)
		outTime.write(runtime + "\r\n")
		outTime.close()

		//向控制台输出参数学习结果
		//output.CPT_FormatToConsole(bn)

		//向文件输出参数学习结果
		output.CPT_FormatToFile(bn, outputFile.optimal_theta)

		println("参数学习完成!")



	}

	/**
	  * EM算法——迭代次数版
	  * 数据修补以及权重计算
	  * inputFile.primitiveData——原始数据文档
	  * MendedData——补后样本RDD
	  * M_ijk[][2]——统计所有样本m_ijk的权重之和M_ijk的二维数组
	  * theta_num——期望统计量或参数的总数量
	  * separator——横向分隔符的类型
	  */
	def EM_Process(bn: BayesianNetwork, M_ijk: Array[M_table], ThetaNum: Int, iteration_EM: Int) {

		val c = bn.r(bn.NodeNum - 1)             //隐变量的势
		var latent_value = " "                   //隐变量的临时修补值

		if (iteration_EM < 1) {
			println("迭代次数<1——不合理!")
			System.exit(0)
		}

		val inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
		reference.SampleNum = inFile.count()                         //原始数据文档中的样本数


		for (iteration_num <- 1 to iteration_EM){
			val complete_data = inFile.map {                 //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
				line =>                                          //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
					val t = new Array[String](c)                  //存储一条原始样本的c个补后样本
					for (i <- 0 until c) {                        //数据修补
						latent_value = (i + 1).toString             //latent_value = i+1
						t(i) = line + separator.data + latent_value //隐变量的值补在每行样本的末端
					}

					val theta_product = new Array[Double](c)      //P(D,L)：每行补后数据对应的n个θijk的乘积
				val m = new Array[Array[String]](c)           //存储c行补后数据对应的期望统计量m_ijk——字符串类型，大小为NUM的数组m[0]存储第一行对应的所有m_ijk标记——m[0][0]存储第一行的第一个属性值对应的m_ijk标号
					for (i <- 0 until c) {
						m(i) = new Array[String](bn.NodeNum)
					}

					/** 考虑每行补后数据t[i]，并计算相应的m_ijk */
					for (i <- 0 until c) {
						val tokens = t(i).split(separator.data)
						val t_int = tokens.map( _.toInt )              //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
						val theta_temp = new Array[Double](bn.NodeNum)  //存储每行补后数据对应的n个θijk的值
						ijk_computation(t_int, bn, m(i), theta_temp)    //计算每行补后样本所对应的期望统计量m_ijk,其中theta_temp (θijk) = EM初始参数
						theta_product(i) = 1
						for (j <- 0 until bn.NodeNum)
							theta_product(i) *= theta_temp(j)
						//          println("========================数据、ijk-key、P(D,L)的乘数参数、P(D,L)==============")
						//          for(a <- t_int){print(a+" ")}
						//          println()
						//          for(a <- m(i)){print(a+" ")}
						//          println()
						//          for(a <- theta_temp){print(a+" ")}
						//          println()
						//          println(theta_product(i))
						//          println("======================= 数据、ijk-key、P(D,L)的乘数参数、P(D,L)==============")
					}

					// 计算每行补后数据的权重P(L|D),共c行
					val weight = new Array[Double](c)                    //存储每行补后数据的权重weight[i]=P(L|D)
				var temp1 = 0.0;   //P(D)
				var temp2 = 0.0
					for (i <- 0 until c)  //P(D)
						temp1 += theta_product(i)
					for (i <- 0 until c - 1)
						weight(i) = theta_product(i) / temp1    //P(L|D) = P(D,L)/P(D)
					//最后一个补后样本权重 = 1 - 前面c-1个权重
					for (i <- 0 until c - 1)                              //归一化每个原始样本的最后一个补后样本权重
						temp2 += weight(i)                                   //temp2——前c-1个补后样本的权重之和
					weight(c - 1) = 1 - temp2


					var lines = ""
					for (i <- 0 until c - 1) {
						lines = lines + t(i) + separator.space + weight(i)   //以空格符分隔补后数据与其权重值
						lines += separator.tab                               //以制表符分隔每条补后数据
					}
					lines += t(c - 1) + separator.space + weight(c - 1)
					/*
					 * 返回值lines——complete_data中的每行,包含了每条样本对应的所有补后样本及相应的权重值；
					 * 每条补后样本以separator.tab为分隔符，补后样本数据与权重以separator.space为分隔符。如1,1,1 separator.space W1 separator.tab 1,1,2 separator.space W2
					 */
					lines
			}//END_mended_data = inFile.map

			//		println("=======================complete_data======================")
			//		complete_data.foreach(println(_))
			/*
			=======================complete_data======================
			1,1,1 0.45827633378932975	1,1,2 0.5417236662106703
			*/
			val mended_data = complete_data.flatMap(line => line.split(separator.tab))
			//		println("=======================mended_data======================")
			//		mended_data.foreach(println(_))
			//		1,1,1 0.45827633378932975
			//		1,2,1 0.7919621749408983


			if (iteration_EM == iteration_num) {                             //最后一次迭代，补后样本以RDD的形式保存于MendedData中
				reference.MendedData = mended_data
				//注意，若这里使用了持久化，则生成多组初始参数的方法中需要加上unpersist()
				//reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK)     //当RDD数据量较大时使用MEMORY_AND_DISK持久化
			}

			val m_ijk = mended_data.map{
				line =>
					val mended_sample = line.split(separator.space)              //补后样本值mended_sample(0)，权重mended_sample(1)，空格为分隔符
				val m_ijk = new Array[String](bn.NodeNum)
					val variable_value = mended_sample(0).split(separator.data)
					val t_int = variable_value.map( _.toInt )                   //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
					SEM.ijk_ML(t_int, bn, m_ijk)                                 //计算每行补后样本所对应的期望统计量m_ijk标号
				var lines = ""
					for (i <- 0 until bn.NodeNum - 1){
						lines += m_ijk(i) + separator.space + mended_sample(1)
						lines += separator.tab
					}
					lines += m_ijk(bn.NodeNum - 1) + separator.space + mended_sample(1)
					lines
			}

			//		println("========================m_ijk========================")
			//		for(a <- m_ijk){println(a+" ")}
			//		println()
			//		1-1-1 0.45827633378932975	2-1-1 0.45827633378932975	3-1-1 0.45827633378932975
			//		1-1-1 0.5417236662106703	2-2-1 0.5417236662106703	3-1-2 0.5417236662106703



			val m_ijk_data = m_ijk.flatMap(line => line.split(separator.tab))
			//		1-1-1 0.45827633378932975
			//		2-1-1 0.45827633378932975
			//		3-1-1 0.45827633378932975
			val m_ijk_pair = m_ijk_data.map{          //将m_ijk标号及其权重值映射为(key,value)格式——(m_ijk标号,权重)
				line =>
					val temp = line.split(separator.space)
					val tag = temp(0)
					val value = temp(1).toDouble
					(tag, value)
			}
			//		println("========================m_ijk_pair========================")
			//		for(a <- m_ijk_pair){println(a+" ")}
			//		println()
			//		========================m_ijk_pair========================
			//		(1-1-1,0.45827633378932975)
			//		(2-1-1,0.45827633378932975)
			//		(3-1-1,0.45827633378932975)
			//		(1-1-1,0.5417236662106703)
			//		(2-2-1,0.5417236662106703)
			//		(3-1-2,0.5417236662106703)



			val M_ijk_pairRDD = m_ijk_pair.reduceByKey((x, y) => x + y)    //统计m_ijk到对应的M_ijk中
			//		println("========================M_ijk_pairRDD========================")
			//		for(a <- M_ijk_pairRDD){println(a+" ")}
			//		println()
			//		========================M_ijk_pairRDD========================
			//		(2-1-1,0.45827633378932975)
			//		(2-2-1,0.5417236662106703)
			//		(3-1-2,0.5417236662106703)
			//		(3-1-1,0.45827633378932975)
			//		(1-1-1,1.0)


			M_ijk_pairRDD.persist(StorageLevel.MEMORY_ONLY)
			val M_ijk_realNum = M_ijk_pairRDD.count().toInt                //从样本中统计得到的实际M_ijk数目，注：M_ijk_realNum很可能小于ThetaNum
			val M_ijk_pair = M_ijk_pairRDD.collect()                       //执行action，将数据提取到Driver端
			M_ijk_pairRDD.unpersist()
			//		println("========================M_ijk_pair========================")
			//		M_ijk_pair.foreach(pair => println(pair._1 + "," + pair._2))
			//		3-1-2,0.5417236662106703
			//		1-1-1,1.0
			//		2-1-1,0.45827633378932975
			//		2-2-1,0.5417236662106703
			//		3-1-1,0.45827633378932975


			bn.initialize_M_ijk(M_ijk, ThetaNum)        //初始化M_ijk的标号及值，每个M_ijk的权重置0
			//设置M_ijk中每个M_ijk的权重
			for (i <- 0 until ThetaNum)
				breakable {
					for (j <- 0 until M_ijk_realNum)
						if (M_ijk(i).tag == M_ijk_pair(j)._1){
							M_ijk(i).value = M_ijk_pair(j)._2
							break()
						}
				}
			//        println("========================M_ijk表========================")
			//        var l = 0
			//        for(i <- 0 until bn.theta.length){
			//          for(j <- 0 until bn.theta(i).length){
			//            for(k <- 0 until bn.theta(i)(j).length){
			//              print(M_ijk(l).tag + ":" +  nf.format(M_ijk(l).value) +" ")
			//              l += 1
			//            }
			//            println()
			//          }
			//          println("#")
			//        }
			//        1-1-1:1 1-1-2:0
			//        #
			//        2-1-1:0.4583 2-1-2:0
			//        2-2-1:0.5417 2-2-2:0
			//        #
			//        3-1-1:0.4583 3-1-2:0.5417
			//        3-2-1:0 3-2-2:0

			/** 根据M_ijk更新参数theta */
			/* 若某一种组合情况的样本在数据集中一次也未出现，则此种样本所对应的M_ijk求和为0，即temp = 0；
			* 下述计算并未考虑此种情形；此时，某个节点CPT中的某几行会出现全为问号?的行，即发生了除0操作；
			*/
			var temp = 0.0; var v = 0
			for (i <- 0 until bn.NodeNum)
				for (j <- 0 until bn.q(i) ) {
					temp = 0.0
					for (k <- 0 until bn.r(i) ) {
						v = bn.hash_ijk(i, j, k)
						temp += M_ijk(v).value
					}

					//                for (k <- 0 until bn.r(i) ) {
					//                  v = bn.hash_ijk(i, j, k)
					//                  bn.theta(i)(j)(k) = M_ijk(v).value / temp
					//                }

					if(temp != 0){
						for (k <- 0 until bn.r(i) ) {
							v = bn.hash_ijk(i, j, k)
							bn.theta(i)(j)(k) = M_ijk(v).value / temp
						}
					}else{
						for (k <- 0 until bn.r(i) ) {
							v = bn.hash_ijk(i, j, k)
							bn.theta(i)(j)(k) = 1.0 / bn.r(i)
						}
					}

				}

			/** 参数估计记录日志 */   //BN结构学习时需要注释掉
			/*
			val Theta_ijk = new Array[Double](ThetaNum)
			bn.CPT_to_Theta_ijk(Theta_ijk)                                     //参数CPT形式转换为顺序表形式以便计算log似然度
			val outLog = new FileWriter(logFile.parameterLearning, true)      //参数学习输出流对象
			outLog.write("【EM迭代 " + iteration_num + " 次后的参数log似然度及CPT】\r\n")

			/*
			outLog.write("M_ijk表\r\n")
			outLog.write("M_ijk:\r\n")
			for (i <- 0 until ThetaNum)
			  outLog.write(M_ijk(i).tag + "\t")
			outLog.write("\r\n")
			for (i <- 0 until ThetaNum)
			  outLog.write(nf.format(M_ijk(i).value) + "\t")
			outLog.write("\r\n")
			*/

			outLog.write("参数log似然度: " + Likelihood(bn, M_ijk, Theta_ijk) + "\r\n")
			outLog.close()
			val output = new BN_Output                                        //BN输出对象
			output.CPT_FormatAppendFile(bn,logFile.parameterLearning)
			*/
		} //EM迭代结束

	}

	/**
	  * EM算法——参数相似度阈值版
	  * 数据修补以及权重计算
	  * inputFile.primitiveData——原始数据文档
	  * MendedData——补后样本RDD
	  * M_ijk[][2]——统计所有样本m_ijk的权重之和M_ijk的二维数组
	  * theta_num——期望统计量或参数的总数量
	  * separator——横向分隔符的类型
	  * iteration_EM——迭代次数上限
	  */
	def EM_ProcessThreshold(bn: BayesianNetwork, iteration_EM: Int, c:Int):Double = {


//		println("c : " + c)

		//    var old_likelihood = 0.0                 //本次EM迭代前的参数似然度
		//    var new_likelihood = 0.0                 //本次EM迭代后的参数似然度
		var old_likelihood1 = Double.NegativeInfinity                 //本次EM迭代前的参数似然度
		var new_likelihood1 = Double.NegativeInfinity                 //本次EM迭代后的参数似然度

		//    var similarity = 0.0                     //参数相似度
		var similarity1 = 0.0                     //参数相似度
		var iteration_count = -1                 //实际迭代次数记录

		var PD_log = 0.0

		if (iteration_EM < 0) {
			println("迭代次数上限<0——不合理!")
			System.exit(0)
		}


		val inFile = reference.Data
		reference.SampleNum = inFile.count()                         //原始数据文档中的样本数
//		println(reference.SampleNum)
//		inFile.foreach(println(_))
//		System.exit(0)

		breakable {
			for (iteration_num <- 0 to iteration_EM) {

//				println("EM算法第 " + iteration_num + " 次，计算填充数据权值")
//				var EM_start = new Date()                                      //getTime获取得到的数值单位：数

//				val outLog = new FileWriter(outputFile.temporary)                      //以追加写入的方式创建FileWriter对象
//				outLog.write("\r\n")
//				outLog.close()

				iteration_count = iteration_num
				var PD_accumulator = SparkConf.sc.accumulator(0.0)   //初始值为0
				val complete_data = inFile.map {  //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
					line =>                         //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
						val t = line.split(separator.tab)
//						println(line)
//						println(t.length)
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
							for (j <- 0 until bn.NodeNum) {
								theta_product(i) *= theta_temp(j)
							}
//							println("========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============")
//							println(t(i))
//							for(a <- m(i)){print(a+" ")}
//							println()
//							for(a <- theta_temp){print(a+" ")}
//							println()
//							println(theta_product(i))

						}
						//            ========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============
						//            1,1,1,1
						//            1-1-1 2-1-1 3-1-1 4-1-1
						//            0.43 0.25 0.6 0.6
						//            0.0387
						//            ========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============
						//            1,1,1,2
						//            1-1-1 2-1-1 3-1-1 4-1-2
						//            0.43 0.25 0.6 0.4
						//            0.025800000000000003
						/** 计算每行补后数据的权重,共c行 */
						val weight = new Array[Double](c) //存储每行补后数据的权重weight[i]
						var temp1 = 0.0
						for (i <- 0 until c)
							temp1 += theta_product(i)
						///////////////////////////P(D)////////////////////////////////
						PD_accumulator += (log(temp1)*reference.BIC_data_number)    //P(D)
//						println(temp1)
//						println(log(temp1))
//						if(temp1 < 0 ){
//							val outLog = new FileWriter(outputFile.temporary, true)                      //以追加写入的方式创建FileWriter对象
//							outLog.write("complete_data :P(D) " + line + " " + temp1+" < 0 \r\n")
//							outLog.close()
//						}
						///////////////////////////P(D)////////////////////////////////
//						var weight_max_index = 0
//						var weight_max:Double = 0
//						var weight_temp:Double = 0
						for (i <- 0 until c){
							weight(i) = theta_product(i) / temp1  //P(L|D)
//							println(t(i))
//							println(weight(i))
//							if(weight(i) > weight_max){
//								weight_max = weight(i)
//								weight_max_index = i
//							}
//							weight_temp += weight(i)

//							if(weight(i) < 0 ){
//								val outLog = new FileWriter(outputFile.temporary, true)                      //以追加写入的方式创建FileWriter对象
//								outLog.write("complete_data :P(L|D) " + line + " " + weight(i)+" < 0 \r\n")
//								outLog.close()
//							}
						}
//						weight(weight_max_index) =  1 - (weight_temp - weight_max)
//						println("weight(weight_max_index):"  + weight(weight_max_index))
//						println("temp1: " + temp1)
//						println("weight_temp: " + weight_temp)
//						println("weight_max_index: " + weight_max_index)
//						println("weight_max: " + weight_max)
						var lines = ""
						for (i <- 0 until c - 1) {
							lines = lines + t(i) + separator.space + weight(i) //以空格符分隔补后数据与其权重值
							lines += separator.tab //以制表符分隔每条补后数据
//							if (weight(i) < 0){
//								println(t(i) + separator.space + weight(i))
//							}
						}
//						println(c)
//						println(t(c - 1))
//						println(weight(c - 1))
						lines += t(c - 1) + separator.space + weight(c - 1)
						//			println("========================完整数据 及其 P(L|D)==============")
						//            println(lines)
						//            ========================完整数据 及其 P(L|D)==============
						//            1,1,1,1 0.23076923076923073	1,1,1,2 0.15384615384615385	1,1,2,1 0.3692307692307692	1,1,2,2 0.24615384615384617
						lines
				} //END_mended_data = inFile.map
				/*
				  * 返回值lines——complete_data中的每行,包含了每条样本对应的所有补后样本及相应的权重值；
				  * 每条补后样本以separator.tab为分隔符，补后样本数据与权重以separator.space为分隔符。如1,1,1 separator.space W1 separator.tab 1,1,2 separator.space W2
				*/

				val mended_data = complete_data.flatMap(line => line.split(separator.tab))


//				var EM_end = new Date()                                      //getTime获取得到的数值单位：数
//				var runtime = (EM_end.getTime - EM_start.getTime)/1000.0
//				println("complete_data.flatMap : " + runtime + "秒")
//				EM_start = new Date()                                      //getTime获取得到的数值单位：数

				reference.MendedData = mended_data //补后样本以RDD的形式保存于MendedData中,用于候选模型参数及期望BIC分数的计算

				//		  //作为测试用，查看完整数据集，实际学习时必须注释掉，不然和后面的代码2次action RDD，累加器会变为原来的2倍，使得BIC分数增加，导致候选模型分数永远小于lod分数
				//		  println("=====PD_accumulator.value 1: "+ PD_accumulator.value)
//				        println("========================完整数据 及其 P(L|D)==============")
//						  mended_data.foreach(println(_))
//						  System.exit(0)
				//		  println("=====PD_accumulator.value 2: "+ PD_accumulator.value)

				//        ========================完整数据 及其 P(L|D)==============
//				2,7,21,5,17,2,1,1 0.0
//				2,7,21,5,17,2,1,2 0.07912257989101688
//				2,7,21,5,17,2,2,1 0.0
//				2,7,21,5,17,2,2,2 0.06740064679918288
//				。。。。。。。。。
//				2,7,21,5,17,2,17,1 0.0
//				2,7,21,5,17,2,17,2 0.06609453486228467

				//求每个填充数据对应的jik
				val m_ijk = mended_data.map {
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


//				println("========================ijk-key P(L|D)==============")
//				for(a <- m_ijk)
//					println(a)
//				println(m_ijk.count())
//				System.exit(0)
				//        ========================ijk-key P(L|D)==============
//				1-7-2 0.0	2-1-7 0.0	3-2-21 0.0	4-1123-5 0.0	5-1-17 0.0	6-1-2 0.0	7-2-1 0.0	8-14-1 0.0
//				1-7-2 0.07912257989101688	2-1-7 0.07912257989101688	3-2-21 0.07912257989101688	4-1124-5 0.07912257989101688	5-2-17 0.07912257989101688	6-2-2 0.07912257989101688	7-2-1 0.07912257989101688	8-14-2 0.07912257989101688
//				1-7-2 0.0	2-1-7 0.0	3-2-21 0.0	4-1125-5 0.0	5-3-17 0.0	6-1-2 0.0	7-2-2 0.0	8-14-1 0.0
//				1-7-2 0.06740064679918288	2-1-7 0.06740064679918288	3-2-21 0.06740064679918288	4-1126-5 0.06740064679918288	5-4-17 0.06740064679918288	6-2-2 0.06740064679918288	7-2-2 0.06740064679918288	8-14-2 0.06740064679918288
//
//				1-7-2 0.0	2-1-7 0.0	3-2-21 0.0	4-1155-5 0.0	5-33-17 0.0	6-1-2 0.0	7-2-17 0.0	8-14-1 0.0
//				1-7-2 0.06609453486228467	2-1-7 0.06609453486228467	3-2-21 0.06609453486228467	4-1156-5 0.06609453486228467	5-34-17 0.06609453486228467	6-2-2 0.06609453486228467	7-2-17 0.06609453486228467	8-14-2 0.06609453486228467
//				34

				val m_ijk_data = m_ijk.flatMap{
					line =>
						line.split(separator.tab)
				}

//				EM_end = new Date()                                      //getTime获取得到的数值单位：数
//				runtime = (EM_end.getTime - EM_start.getTime)/1000.0
//				println("m_ijk.flatMap : " + runtime + "秒")
//				EM_start = new Date()                                      //getTime获取得到的数值单位：数

//				        for(a <- m_ijk_data)
//				          println(a)
				//        1-1-1 0.23076923076923073
				//        2-1-1 0.23076923076923073
				//        3-1-1 0.23076923076923073
				//        4-1-1 0.23076923076923073
				//        1-1-1 0.15384615384615385
				//        2-1-1 0.15384615384615385
				//        3-1-1 0.15384615384615385
				//        4-1-2 0.15384615384615385
				//        1-1-1 0.3692307692307692
				//        2-2-1 0.3692307692307692
				//        3-1-2 0.3692307692307692
				//        4-1-1 0.3692307692307692
				//        1-1-1 0.24615384615384617
				//        2-2-1 0.24615384615384617
				//        3-1-2 0.24615384615384617
				//        4-1-2 0.24615384615384617

				val m_ijk_pair = m_ijk_data.map {                           //将m_ijk标号及其权重值映射为(key,value)格式——(m_ijk标号,权重)
					line =>
						val temp = line.split(separator.space)
						val tag = temp(0)
						val value = temp(1).toDouble
						(tag, value)
				}

				//        for(a <- m_ijk_pair)
				//          println(a)
				//        (1-1-1,0.23076923076923073)
				//        (2-1-1,0.23076923076923073)
				//        (3-1-1,0.23076923076923073)
				//        (4-1-1,0.23076923076923073)
				//        (1-1-1,0.15384615384615385)
				//        (2-1-1,0.15384615384615385)
				//        (3-1-1,0.15384615384615385)
				//        (4-1-2,0.15384615384615385)
				//        (1-1-1,0.3692307692307692)
				//        (2-2-1,0.3692307692307692)
				//        (3-1-2,0.3692307692307692)
				//        (4-1-1,0.3692307692307692)
				//        (1-1-1,0.24615384615384617)
				//        (2-2-1,0.24615384615384617)
				//        (3-1-2,0.24615384615384617)
				//        (4-1-2,0.24615384615384617)

				val M_ijk_pairRDD = m_ijk_pair.reduceByKey((x, y) => x + y) //统计m_ijk到对应的M_ijk中

//				EM_end = new Date()                                      //getTime获取得到的数值单位：数
//				runtime = (EM_end.getTime - EM_start.getTime)/1000.0
//				println("m_ijk_pair.reduceByKey : " + runtime + "秒")
//				EM_start = new Date()                                      //getTime获取得到的数值单位：数

//				for(a <- M_ijk_pairRDD)
//				  println(a)
//				System.exit(0)
				//        (1-1-1,1.0)
				//        (2-2-1,0.6153846153846154)
				//        (3-1-2,0.6153846153846154)
				//        (4-1-2,0.4)
				//        (4-1-1,0.5999999999999999)
				//        (2-1-1,0.3846153846153846)
				//        (3-1-1,0.3846153846153846)

				//println("=====PD_accumulator.value 2: "+ PD_accumulator.value)
				//			M_ijk_pairRDD.persist(StorageLevel.MEMORY_ONLY)
				//			val M_ijk_realNum = M_ijk_pairRDD.count().toInt //从样本中统计得到的实际M_ijk数目，注：M_ijk_realNum很可能小于ThetaNum
				val M_ijk_pair = M_ijk_pairRDD.collect() //执行action，将数据提取到Driver端

//				EM_end = new Date()                                      //getTime获取得到的数值单位：数
//				runtime = (EM_end.getTime - EM_start.getTime)/1000.0
//				println("M_ijk_pairRDD.collect : " + runtime + "秒")
//				EM_start = new Date()                                      //getTime获取得到的数值单位：数

				//			M_ijk_pairRDD.unpersist()
				//println("=====PD_accumulator.value 3: "+ PD_accumulator.value)

				//比较似然函数值，判断是否结束迭代
				new_likelihood1 =  PD_accumulator.value      //本次EM迭代后的参数似然度
				PD_log = new_likelihood1
				similarity1 = new_likelihood1 - old_likelihood1        //两次迭代间的参数相似度
				println("=====log: "+ new_likelihood1 + "=====迭代次数: " + iteration_count +"=====similarity："+ similarity1)
				if (similarity1 < reference.EM_threshold){   //两次迭代间的参数似然度之差的绝对值小于EM收敛阈值threshold
					println(similarity1 + " < " +  reference.EM_threshold + " ,两次迭代间的参数似然度之差的绝对值小于EM收敛阈值threshold,结束EM迭代")
					break()                                  //这里若使用return，则会出现错误——Return statements aren't allowed in Spark closures(还未查明为什么)
				}
				old_likelihood1 = new_likelihood1            //下一次EM迭代前的参数似然度

				//比较似然函数值，判断是否结束迭代
				if (iteration_count == iteration_EM){
					println("迭代次数达到上限 "+ iteration_EM + " 次，结束EM迭代")
					break()
				}

				//求期望统计量m_ijk
				//			bn.initialize_M_ijk(M_ijk, ThetaNum) //初始化M_ijk的标号及值，每个M_ijk的权重置0
				//			for (i <- 0 until ThetaNum)
				//				breakable {
				//					for (j <- 0 until M_ijk_realNum)
				//					if (M_ijk(i).tag == M_ijk_pair(j)._1) {
				//					M_ijk(i).value = M_ijk_pair(j)._2
				//					break()
				//				}
				//			}
				bn.create_M_ijk() // 重置BN的统计量
				for(p <- M_ijk_pair){
					val ijk = p._1.split("-").map(_.toInt)
					bn.M_ijk(ijk(0)-1)(ijk(1)-1)(ijk(2)-1) = p._2
				}
//				println(M_ijk_pair.length)
//				System.exit(0)

//				EM_end = new Date()                                      //getTime获取得到的数值单位：数
//				runtime = (EM_end.getTime - EM_start.getTime)/1000.0
//				println("开始计算参数 : " + runtime + "秒")
//				EM_start = new Date()
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
//							for (k <- 0 until bn.r(i)){
//								bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
//								if(bn.theta(i)(j)(k) == 0)
//									flag = 1
//							}
							/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
							for (k <- 0 until bn.r(i) ) {
						  		bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
							}
							///////////////////////////////////////////////////////////////////////////////////////////////////////////////
							//					if(temp != 0){
							//						for (k <- 0 until bn.r(i) ) {
							//							//v = bn.hash_ijk(i, j, k)
							//							bn.theta(i)(j)(k) = bn.M_ijk(i)(j)(k) / temp
							//						}
							//					}else{
							//						for (k <- 0 until bn.r(i) ) {
							//							//v = bn.hash_ijk(i, j, k)
							//							bn.theta(i)(j)(k) = 1.0 / bn.r(i)
							//						}
							//					}
							/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//							if(flag == 1){
//								k_Mijk.clear()
//								for (k <- 0 until bn.r(i) ) {
//									k_Mijk += bn.M_ijk(i)(j)(k)
//								}
//								val max = k_Mijk.max
////								println("max : "+max)
//								for (t <- 0 until bn.r(i) ) {
//									if(k_Mijk(t) == max){
//										k_Mijk(t) = 2.0
//									}else{
//										k_Mijk(t) = 1.0
//									}
//								}
//								var k_Mijk_sum = k_Mijk.sum
//								for (k <- 0 until bn.r(i))
//									bn.theta(i)(j)(k) = k_Mijk(k) / k_Mijk_sum
//							}
							/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//							if(flag == 1) {
//								var flag0 = 0
//								var k_Theta = new ArrayBuffer[Double]
//								flag0 = 0
//								k_Theta.clear()
//								for (k <- 0 until bn.r(i))
//									k_Theta += bn.theta(i)(j)(k)
//								val max = k_Theta.max
//								val min = k_Theta.min
//
//								if (min == 0) {
//									println("后处理参数 : " + (i + 1) + " " + (j + 1))
//									if (max == 1) {
//										for (t <- 0 until k_Theta.length) {
//											if (k_Theta(t) == 1) {
//												k_Theta(t) = 2.0 / (bn.r(i) + 1)
//											} else {
//												k_Theta(t) = 1.0 / (bn.r(i) + 1)
//											}
//										}
//									} else {
//										var min_no0 = 1.0
//										var temp0 = 0.0
//										// 找到非0的最小值 min_no0
//										for (t <- 0 until k_Theta.length) {
//											if (k_Theta(t) != 0 && k_Theta(t) < min_no0) {
//												min_no0 = k_Theta(t)
//											}
//										}
//										// 令参数0等于非0的最小值 min_no0
//										for (t <- 0 until k_Theta.length) {
//											if (k_Theta(t) == 0) {
//												k_Theta(t) = min_no0
//											}
//											temp0 += k_Theta(t)
//										}
//										// 重新计算参数
//										for (t <- 0 until k_Theta.length) {
//											k_Theta(t) = k_Theta(t) / temp0
//										}
//									}
//									for (k <- 0 until bn.r(i)) {
//										bn.theta(i)(j)(k) = k_Theta(k)
//									}
//								}
//							}
							/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
						}else{
							for (k <- 0 until bn.r(i) ) {
								bn.theta(i)(j)(k) = 1.0 / bn.r(i)
								//Theta_ijk(v) = 1.0 / bn.r(i)
							}
						}
						/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					}
				}
				/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

//				EM_end = new Date()                                      //getTime获取得到的数值单位：数
//				runtime = (EM_end.getTime - EM_start.getTime)/1000.0
//				println("计算参数结束，参数R初始化 : " + runtime + "秒")

//				if(reference.R_type == "1")
//					bn.RandomTheta_WeakConstraint22(bn)   // 我的参数约束
//				else if(reference.R_type == "n")
//					bn.RandomTheta_WeakConstraintRR(bn)   // 我的参数约束
				/////////////////////////////////////////////////////////////////////////////////////////////////////////////////

				//比较似然函数值，判断是否结束迭代
				//		val Theta_ijk = new Array[Double](ThetaNum)
				//		bn.CPT_to_Theta_ijk(Theta_ijk)                           //参数CPT形式转换为顺序表形式以便计算log似然度
				//		new_likelihood = Likelihood(bn, M_ijk, Theta_ijk)        //本次EM迭代后的参数似然度
				//		similarity = abs(new_likelihood - old_likelihood)        //两次迭代间的参数相似度
				//		println("=====likelihood: "+ new_likelihood + "=====迭代次数: " + iteration_count +"=====similarity："+ similarity)
				//		if (similarity < reference.EM_threshold)   //两次迭代间的参数似然度之差的绝对值小于EM收敛阈值threshold
				//		break()                                  //这里若使用return，则会出现错误——Return statements aren't allowed in Spark closures(还未查明为什么)
				//		old_likelihood = new_likelihood            //下一次EM迭代前的参数似然度




				/** 参数估计记录日志 */  //BN结构学习时需要注释掉
				/*
				val outLog = new FileWriter(logFile.parameterLearning, true) //参数学习日志文件输出对象
				outLog.write("【EM迭代" + iteration_num + "次后的参数log似然度及CPT】\r\n")
				outLog.write("参数log似然度: " + new_likelihood + "\r\n")
				outLog.write("本次迭代与上一次迭代的参数相似度: " + similarity + "\r\n")
				outLog.close()
				val output = new BN_Output
				output.CPT_FormatAppendFile(bn, logFile.parameterLearning)
				*/

//				//向控制台输出参数学习结果
//				 val output = new BN_Output
//				 output.CPT_FormatToConsole(bn)
			} //EM迭代结束
		}
		val outLog = new FileWriter(logFile.parameterLearning, true)
		outLog.write("EM实际迭代次数: " + iteration_count + "\r\n")
		outLog.close()
		println("=====EM实际迭代次数"+ iteration_count +"=====")
		PD_log
	}


	def learningParameter_no_latent(bn: BayesianNetwork):Unit = {

//		reference.Data.foreach(println(_))
//		System.exit(0)
		val m_ijk = reference.Data.map {
			line =>
				val m_ijk = new Array[String](bn.NodeNum)
				val variable_value = line.split(separator.data)
				val t_int = variable_value map ( _.toInt )                   //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
				ijk_ML(t_int, bn, m_ijk)                                     //计算每行样本所对应的统计量m_ijk标号

				var lines = ""
				for (i <- 0 until bn.NodeNum - 1){
					lines += m_ijk(i) + separator.space + 1
					lines += separator.tab
				}
				lines += m_ijk(bn.NodeNum - 1) + separator.space + 1
				lines
		}
//		m_ijk.foreach(println(_))
//		System.exit(0)

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


		for(p <- M_ijk_pair){
			val ijk = p._1.split("-").map(_.toInt)
			bn.M_ijk(ijk(0)-1)(ijk(1)-1)(ijk(2)-1) = p._2
		}

		/** 根据M_ijk更新参数theta */
		var temp = 0.0
		for (i <- 0 until bn.NodeNum){
			for (j <- 0 until bn.q(i)) {
				for (k <- 0 until bn.r(i)) {
					if(bn.M_ijk(i)(j)(k) == 0)
						bn.M_ijk(i)(j)(k) += 0.1
				}
			}
		}
		for (i <- 0 until bn.NodeNum){
			for (j <- 0 until bn.q(i)) {
				temp = 0.0
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



	/**
	  * EM算法——参数相似度阈值版
	  * 数据修补以及权重计算
	  * inputFile.primitiveData——原始数据文档
	  * MendedData——补后样本RDD
	  * M_ijk[][2]——统计所有样本m_ijk的权重之和M_ijk的二维数组
	  * theta_num——期望统计量或参数的总数量
	  * separator——横向分隔符的类型
	  * iteration_EM——迭代次数上限
	  */
	def EM_ProcessThreshold1105(bn: BayesianNetwork,bn_temp1: BayesianNetwork, bn_temp2: BayesianNetwork, iteration_EM: Int, c:Int):Double = {


		//println(c)

		//    var old_likelihood = 0.0                 //本次EM迭代前的参数似然度
		//    var new_likelihood = 0.0                 //本次EM迭代后的参数似然度


		var MendedData: RDD[String] = null
		var old_likelihood1 = reference.PD_log                 //本次EM迭代前的参数似然度
		var new_likelihood1 = Double.NegativeInfinity                 //本次EM迭代后的参数似然度

		//    var similarity = 0.0                     //参数相似度
		var similarity1 = 0.0                     //参数相似度
		var iteration_count = -1                 //实际迭代次数记录

		var PD_log = 0.0

		if (iteration_EM < 0) {
			println("迭代次数上限<0——不合理!")
			System.exit(0)
		}


		val inFile = reference.Data
		//reference.SampleNum = inFile.count()                         //原始数据文档中的样本数
		//	println(reference.SampleNum)
		//	System.exit(0)

		breakable {
			for (iteration_num <- 0 to iteration_EM) {


				iteration_count = iteration_num
				var PD_accumulator = SparkConf.sc.accumulator(0.0)   //初始值为0
				val complete_data = inFile.map {  //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
					line =>                         //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
						val t = line.split(separator.tab)
						//						println(line)
						//						println(t.length)
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
							for (j <- 0 until bn.NodeNum) {
								theta_product(i) *= theta_temp(j)
							}
							//							println("========================数据、m(i) = ijk-key、theta_temp = P(D,L)乘数、theta_product(i) = P(D,L)==============")
							//							println(t(i))
							////							for(a <- m(i)){print(a+" ")}
							////							println()
							////							for(a <- theta_temp){print(a+" ")}
							////							println()
							//							println(theta_product(i))

						}
						/** 计算每行补后数据的权重,共c行 */
						val weight = new Array[Double](c) //存储每行补后数据的权重weight[i]
					var temp1 = 0.0
						for (i <- 0 until c)
							temp1 += theta_product(i)
						///////////////////////////P(D)////////////////////////////////
						PD_accumulator += log(temp1)    //P(D)
						for (i <- 0 until c){
							weight(i) = theta_product(i) / temp1  //P(L|D)
						}
						var lines = ""
						for (i <- 0 until c - 1) {
							lines = lines + t(i) + separator.space + weight(i) //以空格符分隔补后数据与其权重值
							lines += separator.tab //以制表符分隔每条补后数据
							//							if (weight(i) < 0){
							//								println(t(i) + separator.space + weight(i))
							//							}
						}
						lines += t(c - 1) + separator.space + weight(c - 1)
						//			println("========================完整数据 及其 P(L|D)==============")
						//            println(lines)
						//            ========================完整数据 及其 P(L|D)==============
						//            1,1,1,1 0.23076923076923073	1,1,1,2 0.15384615384615385	1,1,2,1 0.3692307692307692	1,1,2,2 0.24615384615384617
						lines
				} //END_mended_data = inFile.map

				MendedData = complete_data.flatMap(line => line.split(separator.tab))

			  //作为测试用，查看完整数据集，实际学习时必须注释掉，不然和后面的代码2次action RDD，累加器会变为原来的2倍，使得BIC分数增加，导致候选模型分数永远小于lod分数
//				println("=====PD_accumulator.value 1: "+ PD_accumulator.value)
//				println("========================完整数据MendedData_last 及其 P(L|D)==============")
//				MendedData_last.foreach(println(_))
//				println("========================完整数据MendedData 及其 P(L|D)==============")
//				MendedData.foreach(println(_))
//				System.exit(0)

				//求每个填充数据对应的jik
				val m_ijk = MendedData.map {
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


				//println("=====PD_accumulator.value 2: "+ PD_accumulator.value)
				//			M_ijk_pairRDD.persist(StorageLevel.MEMORY_ONLY)
				//			val M_ijk_realNum = M_ijk_pairRDD.count().toInt //从样本中统计得到的实际M_ijk数目，注：M_ijk_realNum很可能小于ThetaNum
				val M_ijk_pair = M_ijk_pairRDD.collect() //执行action，将数据提取到Driver端

				//				EM_end = new Date()                                      //getTime获取得到的数值单位：数
				//				runtime = (EM_end.getTime - EM_start.getTime)/1000.0
				//				println("M_ijk_pairRDD.collect : " + runtime + "秒")
				//				EM_start = new Date()                                      //getTime获取得到的数值单位：数

				//			M_ijk_pairRDD.unpersist()
				//println("=====PD_accumulator.value 3: "+ PD_accumulator.value)

				//比较似然函数值，判断是否结束迭代
				new_likelihood1 =  PD_accumulator.value      //本次EM迭代后的参数似然度
				PD_log = old_likelihood1
				similarity1 = new_likelihood1 - old_likelihood1        //两次迭代间的参数相似度
				println("=====log: "+ new_likelihood1 + "=====迭代次数: " + iteration_count +"=====similarity："+ similarity1)

				if ((iteration_count != 0 && similarity1 < reference.EM_threshold) || iteration_count == iteration_EM){   //两次迭代间的参数似然度之差的绝对值小于EM收敛阈值threshold
					Search.copy_bn(bn, bn_temp2)

					if(bn_temp1.mendedData_flag == 1){
						val complete_data = inFile.map {  //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
							line =>                         //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
								val t = line.split(separator.tab)
								val theta_product = new Array[Double](c) //每行补后数据对应的n个θijk的乘积
								val m = new Array[Array[String]](c) //存储c行补后数据对应的期望统计量m_ijk——字符串类型，大小为NUM的数组m[0]存储第一行对应的所有m_ijk标记——m[0][0]存储第一行的第一个属性值对应的m_ijk标号
								for (i <- 0 until c) {
									m(i) = new Array[String](bn_temp1.NodeNum)
								}
								/** 考虑每行补后数据t[i]，并计算相应的m_ijk */
								for (i <- 0 until c) {
									val tokens = t(i).split(separator.data)
									val t_int = tokens.map(_.toInt) //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
									val theta_temp = new Array[Double](bn_temp1.NodeNum) //存储每行补后数据对应的n个θijk的值
									ijk_computation(t_int, bn_temp1, m(i), theta_temp) //计算每行补后样本所对应的期望统计量m_ijk,其中theta_temp (θijk) = EM初始参数
									theta_product(i) = 1
									for (j <- 0 until bn_temp1.NodeNum) {
										theta_product(i) *= theta_temp(j)
									}
								}
								/** 计算每行补后数据的权重,共c行 */
								val weight = new Array[Double](c) //存储每行补后数据的权重weight[i]
								var temp1 = 0.0
								for (i <- 0 until c)
									temp1 += theta_product(i)
								///////////////////////////P(D)////////////////////////////////
								for (i <- 0 until c){
									weight(i) = theta_product(i) / temp1  //P(L|D)
								}
								var lines = ""
								for (i <- 0 until c - 1) {
									lines = lines + t(i) + separator.space + weight(i) //以空格符分隔补后数据与其权重值
									lines += separator.tab //以制表符分隔每条补后数据
								}
								lines += t(c - 1) + separator.space + weight(c - 1)
								lines
						}
						val mended_data = complete_data.flatMap(line => line.split(separator.tab))
						reference.MendedData = mended_data //补后样本以RDD的形式保存于MendedData中,用于候选模型参数及期望BIC分数的计算
						mended_data.count()
						bn_temp1.mendedData_flag = 0
					}
					break()                                  //这里若使用return，则会出现错误——Return statements aren't allowed in Spark closures(还未查明为什么)
				}

				if (iteration_count != 0){
					Search.copy_bn(bn_temp1, bn_temp2)
					bn_temp1.mendedData_flag = 1
				}
				Search.copy_bn(bn_temp2, bn)
				old_likelihood1 = new_likelihood1            //下一次EM迭代前的参数似然度
				reference.PD_log = old_likelihood1

//				//比较似然函数值，判断是否结束迭代
//				if (iteration_count == iteration_EM){
//					println("迭代次数达到上限 "+ iteration_EM + " 次，结果EM迭代")
//					break()
//				}

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
								//Theta_ijk(v) = 1.0 / bn.r(i)
							}
						}
					}
				}
				//向控制台输出参数学习结果
//			 val output = new BN_Output
//				output.CPT_FormatToConsole(bn)
			} //EM迭代结束
		}
		val outLog = new FileWriter(logFile.parameterLearning, true)
		outLog.write("EM实际迭代次数: " + iteration_count + "\r\n")
		outLog.close()
		println("=====EM实际迭代次数"+ iteration_count +"=====")
		PD_log
	}




	/** 计算参数的log似然度 */
	def Likelihood(bn: BayesianNetwork, M_ijk: Array[M_table], Theta_ijk: Array[Double]): Double = {

		var likelihood = 0.0
		var v = 0
		for (i <- 0 until bn.NodeNum)
			for (j <- 0 until bn.q(i) )
				for (k <- 0 until bn.r(i) ) {
					v = bn.hash_ijk(i, j, k)
					likelihood += M_ijk(v).value * log(Theta_ijk(v) )  //log似然度
				}
		likelihood
	}

	/** 计算每行补后样本所对应的期望统计量m_ijk标号 及 参与计算P(D,L)的θijk值 */
	def ijk_computation(t_int: Array[Int], bn: BayesianNetwork, m: Array[String], theta_temp: Array[Double]) {
		/*
		t_int——存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
		m_ijk[]——存储每行补后数据对应的期望统计量m_ijk标号i,j,k——整数类型
		theta_temp[NUM]——存储每行补后数据对应的n个θijk
		本函数中用t[c][i]表示第c+1行补后样本中的第i+1个变量
		*/
		var int_i = 0                      //每行样本每个属性值对应的ijk,int_i——θijk中的i值——整数类型, 从0开始计算
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
			while (k < NodeNum - 1 && pa < NodeNum) {    //看bn.structure的第i列，有没有节点指向它
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
					temp2 += ( t_int(pa_t(t)) - 1 ) * temp1
				}
				temp2 += t_int(pa_t(k - 1))               //temp2即为t[c][i]的父节点的组合情况取值——θijk中的j
				int_j = temp2 - 1
				m_j = temp2.toString			                //m_j——θijk中的j值——字符串类型
			}
			/* 计算m_k */
			int_k = t_int(i) - 1
			m_k = t_int(i).toString                     //m_k——θijk中的k值——字符串类型
			m(i) = m_i + "-" + m_j + "-" + m_k          //i-j-k
			theta_temp(i) = bn.theta(int_i)(int_j)(int_k)   //找节点对应的参数
		}  //end_for1
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
