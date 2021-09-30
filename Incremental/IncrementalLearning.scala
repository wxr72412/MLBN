package BNLV_learning.Incremental

import java.io.FileWriter
import java.text.NumberFormat
import java.util.Date

import BNLV_learning.Global._
import BNLV_learning.Input.BN_Input
//import BNLV_learning.Learning.ParameterLearning.ijk_computation
//import BNLV_learning.Learning.StructureLearning.{BIC_PD, Q_BIC_F, generate_Initial_Structure, searchProcess_1}
import BNLV_learning.Learning.{SEM, _}
import BNLV_learning.Output.BN_Output
import BNLV_learning._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.math._
import org.apache.spark.storage.StorageLevel

import scala.util.Random
import scala.util.control.Breaks._

object IncrementalLearning {

	val nf = NumberFormat.getNumberInstance()
	nf.setMaximumFractionDigits(4)                       //设置输出格式保留小数位数

	/******************************************/
	def learningProcess(bn: BayesianNetwork):Array[Int] =  {

		var P_time = 0.0
		var S_time = 0.0

		var outTime_P = new FileWriter(logFile.parameterLearningTime)
		outTime_P.write("EM算法迭代时间 ： " + "\r\n")
		outTime_P.close()

		var outTime_S = new FileWriter(logFile.structureLearningTime)
		outTime_S.write("SEM算法迭代时间 ： " + "\r\n")
		outTime_S.close()

		val input = new BN_Input
		val output = new BN_Output

		//生成初始结构
		//generate_Initial_Structure(bn)
		//从文件中读取表示BN初始结构的邻接矩阵到bn.structure中
		input.structureFromFile(inputFile.incremental_structure, separator.structure, bn)

		//根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
		bn.qi_computation()
		//创建CPT
		bn.create_CPT()

		/////////////////////////CPT////////////////////////////
		/////////////////////////CPT/////////////////////////////
		/////////////////////////CPT/////////////////////////////
		input.CPT_FromFile(inputFile.incremental_theta, separator.theta, bn) //从文件中读取参数θ到动态三维数组bn.theta
//		output.CPT_FormatToConsole(bn)
//		output.structureToConsole(bn)
//		System.exit(0)

		var sum = 0.0
		/////////////////////////CPT/////////////////////////////
//		output.CPT_FormatToFile(bn, outputFile.temporary_I_theta00)        //输出BN参数到theta_file中
		/////////////////////////CPT/////////////////////////////

		var outCandidate = new FileWriter(logFile.candidateStructure) //以非追加写入的方式创建FileWriter对象
		outCandidate.write("BN各节点的候选模型\r\n")
		outCandidate.close()

//

		val old_bn = new BayesianNetwork(bn.NodeNum)
		Search.copy_bn(old_bn, bn)
//		output.CPT_FormatToConsole(old_bn)
//		System.exit(0)

		/** 创建统计所有样本m_ijk权重之和的M_ijk表 */
//		var ThetaNum = 0 //期望统计量或参数的总数量
//		for (i <- 0 until bn.NodeNum)
//			ThetaNum += bn.q(i) * bn.r(i)

//		val old_Theta_ijk = new Array[Double](ThetaNum) //模型参数顺序表——类似M_ijk表
//		bn.CPT_to_Theta_ijk(old_Theta_ijk)
		//			for(i <- 0 until ThetaNum)
		//				println(M_ijk(i).tag + " : " + M_ijk(i).value + "   " + old_Theta_ijk(i))
		//			System.exit(0)

		// 填充数据
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
		var complete_data = reference.inFile_Data.map { //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
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
				//				println("填充数据：" + paddingdata_c)
				paddingdata_c
			//
			//            ====================数据修补t = Array[String](c)====================
			//            1,1,1,1
			//            1,1,1,2
			//            1,1,2,1
			//            1,1,2,2
			//            num:4

		}
		// 2,7,21,5,17,2,17
		// 2,7,21,5,17,2,1,1	2,7,21,5,17,2,1,2	2,7,21,5,17,2,2,1	2,7,21,5,17,2,2,2 。。。2,7,21,5,17,2,17,1	2,7,21,5,17,2,17,2
//		println("====================complete_data====================")
//		complete_data.foreach(println(_))
//		System.exit(0)
		reference.Data = complete_data
		reference.Data.persist(StorageLevel.MEMORY_AND_DISK_SER)
		reference.SampleNum = reference.Data.count()                         //原始数据文档中的样本数
		if(reference.R_type == "chest_clinic_1" || reference.R_type == "chest_clinic_2"||
			reference.R_type == "chest_clinic_1L"|| reference.R_type == "chest_clinic_2L"||
			reference.R_type == "chest_clinic_2L_1"|| reference.R_type == "chest_clinic_2L_2"||
			reference.R_type == "chest_clinic_2L_3+5_1" || reference.R_type == "chest_clinic_2L_3+5_2")
			reference.EM_threshold = reference.EM_threshold_chestclinic
		else {
			reference.EM_threshold = reference.EM_threshold_origin * reference.SampleNum
			reference.SEM_threshold = reference.SEM_threshold_origin * reference.SampleNum
		}
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////
		var outLog = new FileWriter(logFile.incremental_Learning) //以“非”追加写入的方式创建FileWriter对象——结构学习日志写对象
		outLog.write("BNLV结构学习日志\r\n" + "数据集: " + inputFile.primitiveData + " " + reference.SampleNum + "\r\n")
		outLog.write("ID阈值: " + reference.SubGraph_threshold + "\r\n")
		outLog.write("属性的势: ")
		for (i <- 0 until bn.NodeNum)
			outLog.write(bn.r(i) + ",")
		outLog.write("\r\n")
		outLog.write("隐变量标志: ")
		for (i <- 0 until bn.NodeNum)
			outLog.write(bn.latent(i) + ",")
		outLog.write("\r\n")
		outLog.write("隐变量组合数: " + reference.c + "\r\n")
		outLog.write("BIC罚项系数: " + reference.BIC + "\r\n")
		outLog.write("EM迭代次数上限: " + reference.EM_iterationNum + "\r\n")
		outLog.write("EM收敛阈值: " + reference.EM_threshold + "\r\n\r\n")
		outLog.write("SEM迭代次数上限: " + reference.SEM_iteration + "\r\n")
		outLog.write("SEM收敛阈值: " + reference.SEM_threshold + "\r\n\r\n")
		outLog.write("BNLV初始结构：\r\n")
		outLog.close()
		output.structureAppendFile(bn, logFile.incremental_Learning)



//		output.CPT_FormatToFile(bn, outputFile.temporary_I_theta0)        //输出BN参数到theta_file中
//		System.exit(0)
		// 计算参数
//		bn.RandomTheta_WeakConstraint2(bn)   // 我的参数约束
//		postTheta(old_bn)
		postTheta(bn)
		println("postTheta(bn)")
//		output.CPT_FormatToFile(bn, outputFile.temporary_I_theta0)        //输出BN参数到theta_file中
//		System.exit(0)

		val t_start_P = new Date()
		ParameterLearning.EM_ProcessThreshold(bn, 1, reference.c)    //参数估计并计算M_ijk的权重值
		val t_end_P = new Date()                                      //getTime获取得到的数值单位：毫秒数
		val runtime_P = (t_end_P.getTime - t_start_P.getTime)/1000.0
		P_time += runtime_P
		outTime_P = new FileWriter(logFile.parameterLearningTime, true)
		outTime_P.write("第一次EM求参数变化量 ： " + runtime_P + "\r\n")
		outTime_P.close()




//		Incremental.compute_theta(bn, reference.MendedData) //参数估计并计算M_ijk的权重值
//		output.CPT_FormatToFile(bn, outputFile.temporary_I_theta0)        //输出BN参数到theta_file中
//		output.Mijk_FormatToFile(bn, outputFile.temporary_I_mijk0)
		//		println("==================充分统计量m_ijk 及 参数===================")
		//		println("ThetaNum : "+ ThetaNum)
		//		for(i <- 0 until ThetaNum)
		//			println(M_ijk(i).tag + " : " + M_ijk(i).value + "   " + new_Theta_ijk(i))
//				System.exit(0)


		// 求节点的影响度
		var ID_Array = new Array[Double](bn.NodeNum)
		var ID = 0.0
		var t = 0
		sum = 0.0
		var v = 0
		var temp = 0.0
		for (i <- 0 until bn.NodeNum) {
			ID = 0.0
			t = bn.q(i) * bn.r(i)
			sum = 0.0
			/////////////////////////////////////////////////////////////////////////////////////
//			for (j <- 0 until bn.q(i)) {
//				for (k <- 0 until bn.r(i)) {
//					temp = abs(old_bn.theta(i)(j)(k) - bn.theta(i)(j)(k))
////					temp = (bn.M_ijk(i)(j)(k) * abs(old_bn.theta(i)(j)(k) - bn.theta(i)(j)(k))) / old_bn.theta(i)(j)(k)
////					sum += temp
//					sum += sqrt(temp)
//				}
//			}
//			println("t: " + t + "  bn.q(i): " + bn.q(i) + "  bn.r(i): " + bn.r(i))
//			println("sum: " + sum)
//			sum = sum*sum
//			println("sum*sum: " + sum)
//			ID = sum / t
//			println("ID: " + ID)
//			ID_Array(i) = ID
//////			ID_Array(i) = ID / reference.MendedData_Num
			/////////////////////////////////////////////////////////////////////////////////
			for (j <- 0 until bn.q(i)) {
				for (k <- 0 until bn.r(i)) {
					temp = abs(old_bn.theta(i)(j)(k) - bn.theta(i)(j)(k))
//					temp = (bn.M_ijk(i)(j)(k) * abs(old_bn.theta(i)(j)(k) - bn.theta(i)(j)(k)))
					sum += temp
//					sum += sqrt(temp)
				}
			}
//			println("t: " + t + "  bn.q(i): " + bn.q(i) + "  bn.r(i): " + bn.r(i))
//			println("sum: " + sum)
			ID = sum / t
//			println("ID: " + ID)
			ID_Array(i) = ID
			//////////////////////////////////////////////////////////////////////
		}

//		ID_Array = Array(0.001,0.001,0.001,0.03, 0.04,0.001)
		// var ID_max = ID_Array.max
		var ID_max = 0.0
		var ID_max_node = 0
		for (i <- 0 until bn.NodeNum) {
//			println("ID_" + i + " : " + ID_Array(i))
			if (ID_Array(i) > ID_max) {
				ID_max_node = i
				ID_max = ID_Array(i)
			}
		}
		ID_Array.foreach(println(_))
		println("ID_max_node = " + ID_max_node + "  ID_max = " + ID_max)
//		System.exit(0)

		outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
		outLog.write("各节点影响度\r\n")
		for (i <- 0 until bn.NodeNum)
			outLog.write(i + " : " + ID_Array(i) + "\r\n")
		outLog.write("影响度max : " + ID_max + "\r\n")
		outLog.close()

		var S_node_Array = new Array[Int](bn.NodeNum) // 子图中的节点
		var S_node_parents_Array = new Array[Int](bn.NodeNum) // 子图中节点的父节点

		if(ID_max >= reference.SubGraph_threshold) {


			//		println("ID_max : " + ID_max)
			//		println("ID_max_node : " + ID_max_node)
			//		System.exit(0)

			findSubgraph(bn, ID_Array, S_node_Array, S_node_parents_Array)

			//		S_node_Array = Array(0 ,0,0,0,0,1)
			//		S_node_parents_Array = Array(1 ,1,0,0,0,0)

			val subgraph_NodeNum = S_node_Array.sum + S_node_parents_Array.sum
			var subgraph_LatentNum = 0 //设置BN隐变量数
//			 println("subgraph_NodeNum : " + subgraph_NodeNum)
			val subgraph_bn = new BayesianNetwork(subgraph_NodeNum)
//			System.exit(0)

			Search.copy_bn(bn, old_bn)

			var temp1 = 0
			var temp2 = 0
			for (i <- 0 until bn.NodeNum) {
				if (S_node_Array(i) == 1 || S_node_parents_Array(i) == 1) {
					subgraph_bn.r(temp1) = bn.r(i)
					subgraph_bn.l(temp1) = bn.l(i)
					subgraph_bn.latent(temp1) = bn.latent(i)
					if (bn.l(i) == "L")
						subgraph_LatentNum += 1
					// 取出子图的结构
					temp2 = 0
					for (j <- 0 until bn.NodeNum) {
						if (S_node_Array(j) == 1 || S_node_parents_Array(j) == 1) {
							subgraph_bn.structure(temp1)(temp2) = bn.structure(i)(j)
							temp2 += 1
						}
					}
					temp1 += 1
				}
			}
//			output.structureToConsole(subgraph_bn)
//			System.exit(0)
			subgraph_bn.qi_computation()
			subgraph_bn.create_CPT()
			//		println(bn.NodeNum)

			// 在子图的节点中，标记与子图无关的父节点为1（子图中无需更新的父节点）
			val S_node_parents_Array_No_Related = new Array[Int](subgraph_bn.NodeNum)
			temp1 = 0
			for (i <- 0 until bn.NodeNum) {
				if (S_node_Array(i) == 1 || S_node_parents_Array(i) == 1) {
					if (S_node_parents_Array(i) == 1)
						S_node_parents_Array_No_Related(temp1) = 1
					temp1 += 1
				}
			}
//			println("S_node_parents_Array_No_Related")
//			S_node_parents_Array_No_Related.foreach(println(_))
//			System.exit(0)

			temp1 = 0
			for (i <- 0 until bn.NodeNum) {
				if (S_node_Array(i) == 1 || S_node_parents_Array(i) == 1) {
					if (S_node_Array(i) == 1) {   // 子图的点
						for (j <- 0 until bn.q(i)) {
							for (k <- 0 until bn.r(i)) {
								subgraph_bn.theta(temp1)(j)(k) = bn.theta(i)(j)(k)
							}
						}
					}
					else if (S_node_parents_Array(i) == 1) { // 子图的父节点
						for (j <- 0 until 1) {
							for (k <- 0 until bn.r(i)) {
								subgraph_bn.theta(temp1)(j)(k) = 1.0 / bn.r(i)
							}
						}
					}
					temp1 += 1
				}
			}
			//		output.CPT_FormatToFile(subgraph_bn, outputFile.temporary_I_theta1)        //输出BN参数到theta_file中
			//		System.exit(0)
			postTheta(subgraph_bn)
			println("postTheta(subgraph_bn)")

//			bn.RandomTheta_WeakConstraint2(subgraph_bn)   // 我的参数约束

			//		subgraph_bn.q.foreach(println(_))
			//		subgraph_bn.r.foreach(println(_))
			//		subgraph_bn.l.foreach(println(_))



			outLog = new FileWriter(logFile.incremental_Learning, true)
			outLog.write("S_node_Array : \r\n")
			S_node_Array.map(x => outLog.write(x + " "))
			outLog.write("\r\n")
			outLog.write("S_node_parents_Array : \r\n")
			S_node_parents_Array.map(x => outLog.write(x + " "))
			outLog.write("\r\n")
			outLog.write("S_node_parents_Array_No_Related : \r\n")
			S_node_parents_Array_No_Related.map(x => outLog.write(x + " "))
			outLog.write("\r\n")
			outLog.write("子图的节点 : \r\n")
			for (i <- 0 until subgraph_bn.NodeNum) {
				outLog.write(subgraph_bn.r(i) + " " + subgraph_bn.l(i) + "\r\n")
			}
			outLog.write("子图的隐变量标记 : \r\n")
			for (i <- 0 until subgraph_bn.NodeNum) {
				outLog.write(subgraph_bn.latent(i) + " " + subgraph_bn.latent(i) + "\r\n")
			}
			outLog.write("子图结构：\r\n")
			outLog.close()
			// 按文件追加的方式，输出BN的结构到文件
			output.structureAppendFile(subgraph_bn, logFile.incremental_Learning)

			//		println("subgraph_structure : ")
			//		for(i <- 0 until subgraph_bn.NodeNum){
			//			for(j <- 0 until subgraph_bn.NodeNum){
			//				print(subgraph_bn.structure(i)(j)+" ")
			//			}
			//			println()
			//		}
			//		println("subgraph_NodeNum : ")
			//		for(i <- 0 until subgraph_bn.NodeNum){
			//			println(subgraph_bn.r(i) + " " + subgraph_bn.l(i))
			//		}
			//		println("bn_structure : ")
			//		for(i <- 0 until bn.NodeNum){
			//			for(j <- 0 until bn.NodeNum){
			//				print(bn.structure(i)(j)+" ")
			//			}
			//			println()
			//		}
			//				System.exit(0)


			//截取与子图相关的数据
			//		temp1 = 0
			//		reference.Data.foreach(println(_))
			//		println("========================子图 完整数据 及其 P(L|D)==============")
			//		reference.MendedData.foreach(println(_))
			///////////////////////////////////////////////////////////////////////////////////
			//		reference.MendedData = reference.MendedData.map {
			//			line =>
			//				val mended_sample = line.split(separator.space) //补后样本值mended_sample(0)，权重mended_sample(1)，空格为分隔符
			//				val variable_value = mended_sample(0).split(separator.data)
			//				var lines = ""
			//				var temp = 0
			//				for (i <- 0 until bn.NodeNum) {
			//					if (S_node_Array(i) == 1 || S_node_parents_Array(i) == 1) {
			//						temp += 1
			//						if(temp != subgraph_bn.NodeNum)
			//							lines += variable_value(i) + separator.data
			//						else
			//							lines += variable_value(i)
			//					}
			//				}
			//				lines += separator.space + mended_sample(1)
			//				lines
			//		}
			///////////////////////////////////////////////////////////////////
			reference.Data = reference.Data.map {
				line =>
					var line1 = ""
					val t = line.split(separator.tab)
					for (i <- 0 until c) {
						var line2 = ""
						val variable_value = t(i).split(separator.data)
						var temp = 0
						for (i <- 0 until bn.NodeNum) {
							if (S_node_Array(i) == 1 || S_node_parents_Array(i) == 1) {
								temp += 1
								if (temp != subgraph_bn.NodeNum)
									line2 += variable_value(i) + separator.data
								else
									line2 += variable_value(i)
							}
						}
						line1 += line2 + separator.tab
					}
					line1
			}
			///////////////////////////////////////////////////////////////////
			//		reference.Data.foreach(println(_))
			reference.SampleNum = reference.Data.count() //原始数据文档中的样本数

			//		reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK)
			//		reference.MendedData_Num = reference.MendedData.count() // 将rdd持久化到内存
			//		println("========================子图 完整数据 及其 P(L|D)==============")
			//		reference.MendedData.foreach(println(_))
			//		println("mended_data_num : "+reference.MendedData_Num)
			//		System.exit(0)


			val EdgeConstraint = Array.ofDim[Int](subgraph_NodeNum, subgraph_NodeNum) //边方向约束（必须保留的边）
			val EdgeConstraint_violate = Array.ofDim[Int](subgraph_NodeNum, subgraph_NodeNum) //边方向约束（不能生成的边）


			//生成边方向约束（不能生成的边）
			if(reference.R_type == "chest_clinic_2L_3+5_1" || reference.R_type == "chest_clinic_2L_3+5_2"){
				for( i <- 0 to subgraph_bn.NodeNum-1) {
					for( j <- 0 to subgraph_bn.NodeNum-1) {
						EdgeConstraint_violate(i)(j) = 1
						if(subgraph_bn.l(i) == "S" && subgraph_bn.l(j) != "S")
							EdgeConstraint_violate(i)(j) = 0
						else if(subgraph_bn.l(i) != "C" && subgraph_bn.l(j) == "C")
							EdgeConstraint_violate(i)(j) = 0
						else if(subgraph_bn.l(i) == "C" && subgraph_bn.l(j) == "S")
							EdgeConstraint_violate(i)(j) = 0
						else if(S_node_parents_Array_No_Related (j) == 1 && S_node_parents_Array_No_Related (i) != 1)
							EdgeConstraint_violate(i)(j) = 0
						else if(S_node_parents_Array_No_Related (j) == 1 && S_node_parents_Array_No_Related (i) == 1)
							EdgeConstraint(i)(j) = subgraph_bn.structure(i)(j)

					}
				}
			}
			else{
				generate_EdgeConstraint_Violate(subgraph_bn, EdgeConstraint_violate, S_node_parents_Array_No_Related)
			}

			//生成边方向约束（必须保留的边）
			if(reference.R_type == "chest_clinic_2L_3+5_1"|| reference.R_type == "chest_clinic_2L_3+5_2") { // 2个隐变量
//				for( i <- 0 to subgraph_bn.NodeNum-1) {
//					if(S_node_parents_Array_No_Related(i) == 1){  // 当前子图与 其父节点 之间的边不变
//						for( j <- 0 to subgraph_bn.NodeNum-1) {
//							EdgeConstraint(i)(j) = subgraph_bn.structure(i)(j)
//							EdgeConstraint_violate(i)(j) = 0
//						}
//					}
//				}
			}
			else{
				generate_Initial_Structure(subgraph_bn, EdgeConstraint, S_node_parents_Array_No_Related)
			}


			println("EdgeConstraint_violate:")
			output.structureToConsole_matrix(EdgeConstraint_violate)

			outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
			outLog.write("边方向约束结构（1代表必须保留的边）：\r\n")
			outLog.close()
			// 按文件追加的方式，输出结构约束到文件
			output.structureAppendFile(EdgeConstraint, logFile.incremental_Learning, subgraph_bn.NodeNum)
			outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
			outLog.write("边方向约束结构（0代表不能生成的边）：\r\n")
			outLog.close()
			// 按文件追加的方式，输出结构约束到文件
			output.structureAppendFile(EdgeConstraint_violate, logFile.incremental_Learning, subgraph_bn.NodeNum)


			//		subgraph_bn.Q_BIC = new Array[Double](subgraph_bn.NodeNum)
			//		S_BIC(subgraph_bn, S_node_parents_Array_No_Related)
			////		subgraph_bn.Q_BIC.foreach(println(_))
			//		IncrementalLearning.old_score_BIC = subgraph_bn.Q_BIC.sum
			//		println("old_score_BIC : "+ old_score_BIC)

			var outBIC_score = new FileWriter(logFile.incremental_BIC_score) //以非追加写入的方式创建FileWriter对象
			outBIC_score.write("每次增量学习后的 BIC分数:\r\n")
			outBIC_score.close()
			//		outBIC_score = new FileWriter(logFile.incremental_BIC_score, true) //以非追加写入的方式创建FileWriter对象
			//		outBIC_score.write("增量学习前 BIC分数:" + IncrementalLearning.old_score_BIC + "\r\n")
			//		outBIC_score.close()
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			/** ************输出中间模型及其参数 *****************/
			//		bn.Theta_ijk_to_CPT(old_Theta_ijk)

//			System.exit(0)


			println("******************子图增量学习开始 *********************")

			var flag = 1 //迭代结束标志
			var incremental_Learning_iteration_count = 1
			val iteration_incremental = reference.incremental_iteration

			val t_start_P = new Date()
			var PD_log: Double = ParameterLearning.EM_ProcessThreshold(subgraph_bn, reference.EM_iterationNum, reference.c) //参数估计并计算M_ijk的权重值
			val t_end_P = new Date()                                      //getTime获取得到的数值单位：毫秒数
			val runtime_P = (t_end_P.getTime - t_start_P.getTime)/1000.0
			P_time += runtime_P
			outTime_P = new FileWriter(logFile.parameterLearningTime, true)
			outTime_P.write("第二次EM 生成破权样本 ： " + runtime_P + "\r\n")
			outTime_P.close()

			reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER)
			reference.MendedData_Num = reference.MendedData.count() //将RDD保存
			reference.Data.unpersist()
			outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
			outLog.write("reference.MendedData 个数 : " + reference.MendedData_Num + "\r\n")
			outLog.close()

			//		println("reference.PD_log: " + reference.PD_log)
			println("PD_log: " + PD_log)
			StructureLearning.old_score_BIC = StructureLearning.BIC_PD(subgraph_bn, PD_log)

//			output.CPT_FormatToFile(subgraph_bn, outputFile.temporary_I_theta1) //输出BN参数到theta_file中
//			output.Mijk_FormatToFile(subgraph_bn, outputFile.temporary_I_mijk1)
//
//			outCandidate = new FileWriter(outputFile.temporary_I_theta2) //以非追加写入的方式创建FileWriter对象
//			outCandidate.write("\r\n")
//			outCandidate.close()
//			outCandidate = new FileWriter(outputFile.temporary_I_mijk2) //以非追加写入的方式创建FileWriter对象
//			outCandidate.write("\r\n")
//			outCandidate.close()

			//		System.exit(0)

			while (flag == 1) {

				outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
				outLog.write("******************增量学习 第" + incremental_Learning_iteration_count + "次迭代*********************\r\n")
				println("******************增量学习 第" + incremental_Learning_iteration_count + "次迭代*********************")

				//存放BN学习过程中，每个节点的候选模型
				var outCandidate = new FileWriter(logFile.candidateStructure, true)
				outCandidate.write("******************增量学习 第" + incremental_Learning_iteration_count + "次迭代*********************\r\n")
				outCandidate.close()

				//			subgraph_bn.Q_BIC.foreach(println(_))
				//			flag = searchProcess_2(subgraph_bn, EdgeConstraint, EdgeConstraint_violate, S_node_parents_Array_No_Related)

				val t_start_S = new Date()
				flag = StructureLearning.searchProcess_1(subgraph_bn, EdgeConstraint, EdgeConstraint_violate)
				val t_end_S = new Date()                                      //getTime获取得到的数值单位：毫秒数
				val runtime_S = (t_end_S.getTime - t_start_S.getTime)/1000.0
				S_time += runtime_S
				outLog = new FileWriter(logFile.structureLearning, true)    //以追加写入的方式创建FileWriter对象
				outLog.write("BNLV结构学习时间: " + runtime_S + " 秒\r\n\r\n")
				outLog.close()

				/** ************输出中间模型及其参数 *****************/
//				outLog = new FileWriter(outputFile.temporary_I_theta2, true) //以追加写入的方式创建FileWriter对象
//				outLog.write("******************结构学习 第" + incremental_Learning_iteration_count + "次迭代 子图*********************\r\n")
//				outLog.close()
//				output.CPT_FormatAppendFile(subgraph_bn, outputFile.temporary_I_theta2) //输出BN参数到theta_file中
//				outLog = new FileWriter(outputFile.temporary_I_mijk2, true) //以追加写入的方式创建FileWriter对象
//				outLog.write("******************结构学习 第" + incremental_Learning_iteration_count + "次迭代 子图*********************\r\n")
//				outLog.close()
//				output.Mijk_FormatAppendFile(subgraph_bn, outputFile.temporary_I_mijk2)

				var outBIC_score = new FileWriter(logFile.incremental_BIC_score, true) //以非追加写入的方式创建FileWriter对象
				outBIC_score.write("增量学习 第" + incremental_Learning_iteration_count + "次后的 BIC分数:" + StructureLearning.old_score_BIC + "\r\n")
				outBIC_score.close()

				outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
				outLog.write("第" + incremental_Learning_iteration_count + "次迭代计算后的最优模型结构:\r\n")
				outLog.close()
				output.structureAppendFile(subgraph_bn, logFile.incremental_Learning)
				// output.CPT_FormatAppendFile(subgraph_bn, logFile.incremental_Learning)

				if (incremental_Learning_iteration_count == iteration_incremental) {
					println("增量学习迭代次数达到上限 " + iteration_incremental + " 次，结果EM迭代")
					flag = 0
				}
				incremental_Learning_iteration_count += 1
			}
			reference.MendedData.unpersist()


			// 将子图 subgraph_bn 覆盖原有的 bn
			//		bn.Theta_ijk_to_CPT(old_Theta_ijk) // 初始化节点参数为初始参数
			// 1、更新与子图相关的结构
			temp1 = 0
			temp2 = 0
			//		output.structureToConsole(bn)
			for (i <- 0 until bn.NodeNum) {
				if (S_node_Array(i) == 1 || S_node_parents_Array(i) == 1) {
					temp1 += 1
					// 取出子图的结构
					temp2 = 0
					for (j <- 0 until bn.NodeNum) {
//						if (S_node_parents_Array(i) == 1 && S_node_parents_Array(j) == 1) {
//							temp2 += 1
//						}
//						else
						if (S_node_Array(j) == 1 || S_node_parents_Array(j) == 1) {
							temp2 += 1
							bn.structure(i)(j) = subgraph_bn.structure(temp1-1)(temp2-1)
//							println((i+1)+" "+ (j+1) + "-"+bn.structure(i)(j) +"   " + temp1 + " " + temp2+ "-"+subgraph_bn.structure(temp1-1)(temp2-1))

						}
					}
//					println()
				}
			}
//			output.structureToConsole(bn)
//			output.structureToConsole(subgraph_bn)
//			System.exit(0)
			// 2、更新父节点取值qi
			bn.qi_computation()
			// 3、选择性的仅更新与子图节点有关（不包含子图的父节点）的CPT
			/** 创建CPT */
			temp1 = 0
			for (i <- 0 until bn.NodeNum) { //开辟存放第i个变量CPT的二维数组空间
				if (S_node_Array(i) == 1 || S_node_parents_Array(i) == 1) {
					//				println(i + " " + bn.l(i) + " " + bn.r(i) + " " + bn.q(i))
					//				println(temp1 + " " + subgraph_bn.l(temp1) + " " + subgraph_bn.r(temp1) + " " + subgraph_bn.q(temp1))
					if (S_node_Array(i) == 1) {
						println("更新的节点 ： " + i)
						//					println(i + " " + bn.l(i) + " " + bn.r(i) + " " + bn.q(i))
						//					println(temp1 + " " + subgraph_bn.l(temp1) + " " + subgraph_bn.r(temp1) + " " + subgraph_bn.q(temp1))
						bn.theta(i) = new Array[Array[Double]](bn.q(i))
						for (j <- 0 until bn.q(i))
							bn.theta(i)(j) = new Array[Double](bn.r(i)) //theta[i][j][k]==θijk
						// 复制参数
						for (j <- 0 until bn.q(i))
							for (k <- 0 until bn.r(i)) {
								//						println("bn.theta(i)(j)(k) : " + i + " " + j + " " + k + " ")
								//						println("subgraph_bn.theta(temp1)(j)(k) : " + temp1 + " " + j + " " + k + " ")
								//						println("bn.theta(i)(j)(k) : " + bn.theta(i)(j)(k))
								//						println("subgraph_bn.theta(temp1)(j)(k) : " + subgraph_bn.theta(temp1)(j)(k))
								bn.theta(i)(j)(k) = subgraph_bn.theta(temp1)(j)(k)
							}
					}
					temp1 += 1
				}
			}

//			output.structureToConsole(bn)
//			output.structureToConsole(subgraph_bn)
//			System.exit(0)
			/** ************最优模型对象存储于文件中 *****************/
			//output.ObjectToFile(bn, outputFile.learned_BN_object)        //以文件的形式将BN对象存储于outputFile.learned_BN_object中

			/** ************向控制台输出最优模型及其参数 *****************/
			//	println("==============最优模型==============")
			//    output.structureToConsole(bn)
			//	println("==============参数==============")
			//    output.CPT_FormatToConsole(bn)

			/** ************输出最优模型及其参数 *****************/
			output.structureToFile(bn, outputFile.incremental_optimal_structure) //输出BN结构到outputFile.optimal_structure中
			output.CPT_FormatToFile(bn, outputFile.incremental_optimal_theta) //输出BN参数到theta_file中
			output.structureToFile(subgraph_bn, outputFile.incremental_optimal_structure_S) //输出子图结构到outputFile.optimal_structure中
			output.CPT_FormatToFile(subgraph_bn, outputFile.incremental_optimal_theta_S) //输出子图参数到theta_file中



			println("增量学习完成!")
			outTime_P = new FileWriter(logFile.parameterLearningTime, true)
			outTime_P.write(P_time + "\r\n")
			outTime_P.close()
			outTime_S = new FileWriter(logFile.structureLearningTime, true)
			outTime_S.write(S_time + "\r\n")
			outTime_S.close()

			return S_node_Array
		}else{
			output.structureToFile(old_bn, outputFile.incremental_optimal_structure) //输出BN结构到outputFile.optimal_structure中
			output.CPT_FormatToFile(old_bn, outputFile.incremental_optimal_theta) //输出BN参数到theta_file中
			outTime_P = new FileWriter(logFile.parameterLearningTime, true)
			outTime_P.write(P_time + "\r\n")
			outTime_P.close()
			outTime_S = new FileWriter(logFile.structureLearningTime, true)
			outTime_S.write(S_time + "\r\n")
			outTime_S.close()

			return S_node_Array
		}



	}

//  /** 对当前节点的一次搜索过程(书中算法版——Algorithm1)
//    * SEM中局部最优模型先根据BIC评分与oldScore比较，若大于oldScore，则将其参数使用EM迭代数次，然后作为新的oldScore.
//    */
////def searchProcess_1(mendedData: String, sample_num: Long, bn: BayesianNetwork, EdgeConstraint: Array[Array[Int]], iteration_EM: Int):Int = {
//def searchProcess_2(bn: BayesianNetwork, EdgeConstraint: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]], S_node_parents_Array_No_Related:Array[Int]):Int = {
//	val sample_num = reference.SampleNum
//	val output = new BN_Output
//	val n = bn.NodeNum
//
//	var candidate_model = new Array[BayesianNetwork](3 * (n - 1)) //每种算子操作最多产生n-1个候选模型,因此每次爬山过程，最多有3*(n-1)个候选模型
//
//	var optimal_model_F = new BayesianNetwork(n) //局部最优模型
////	var optimal_model = new BayesianNetwork(n) //局部最优模型
//
//	val current_model = Array.ofDim[Int](n, n) //当前模型——int current_model[n][n]
//	Search.copy_structure(current_model, bn.structure, n) //令bn为当前模型
//
//	val old_Q_score = IncrementalLearning.old_score_BIC
//	var temp_Q_score = 0.0
//	var new_Q_score = Double.NegativeInfinity //初始分值为负无穷
//
//	for (current_node <- 0 until bn.NodeNum) { //爬山搜索,模型优化当前执行节点current_node
//
//		//current_node由learningProcess控制
//
//		for (i <- 0 until 3 * (n - 1)) //创建数组中每个对象
//			candidate_model(i) = new BayesianNetwork(n)
//
//		val candidate = new Candidate //记录候选模型数量
//		var existed_candidate_num = 0 //当前候选模型的数量
//
//		//进行三种算子操作,候选模型存储于candidate_model中
//		//加边
//		Incremental_Search.add_edge(current_model, EdgeConstraint_violate, n, candidate_model, candidate, current_node, S_node_parents_Array_No_Related)
//		var outCandidate = new FileWriter(logFile.candidateStructure, true)
//		outCandidate.write("【第" + (current_node + 1) + "个节点的候选模型】\r\n")
//		outCandidate.write("加边操作得到的候选模型:\r\n")
//		outCandidate.close()
//		output.candidateStructureAppendFile(candidate_model, existed_candidate_num, candidate.num, logFile.candidateStructure, current_node, n)
//		existed_candidate_num = candidate.num
//
//		//减边
//		Incremental_Search.delete_edge(current_model, EdgeConstraint, n, candidate_model, candidate, current_node, S_node_parents_Array_No_Related)
//		outCandidate = new FileWriter(logFile.candidateStructure, true)
//		outCandidate.write("减边操作得到的候选模型:\r\n")
//		outCandidate.close()
//		output.candidateStructureAppendFile(candidate_model, existed_candidate_num, candidate.num, logFile.candidateStructure, current_node, n)
//		existed_candidate_num = candidate.num
//
//		//反转边
//		Incremental_Search.reverse_edge(current_model, EdgeConstraint_violate, n, candidate_model, candidate, current_node, S_node_parents_Array_No_Related)
//		outCandidate = new FileWriter(logFile.candidateStructure, true)
//		outCandidate.write("转边操作得到的候选模型:\r\n")
//		outCandidate.close()
//		output.candidateStructureAppendFile(candidate_model, existed_candidate_num, candidate.num, logFile.candidateStructure, current_node, n)
//
//		outCandidate = new FileWriter(logFile.candidateStructure, true)
//		outCandidate.write("得到的候选模型: " + candidate.num + " 个\r\n")
//		outCandidate.close()
//
//
//		/** ******************计算每个候选模型变量的势r与其父节点的组合情况数q ************************/
//		for (v <- 0 until candidate.num) {
//			for (i <- 0 until n)
//				candidate_model(v).r(i) = bn.r(i) //设置BN中每个变量的势ri
//			candidate_model(v).qi_computation() //根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
//		}
//
//
//		//println("Test*************************:     " + current_node)
//		var outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
//		outLog.write("【第" + (current_node + 1) + "个节点的候选模型评分计算过程】\r\n")
//		outLog.close()
//		//对每个候选模型进行期望BIC评分，并选择出最大者
//		//var ThetaNum = 0 //期望统计量或参数的总数量
//		for (i <- 0 until candidate.num) {
//			//FOR 每个对G做一次加边、减边或转边而得到的模型结构G' DO
////			ThetaNum = 0
////			for (j <- 0 until n)
////				ThetaNum += candidate_model(i).q(j) * candidate_model(i).r(j)
////			val M_ijk = new Array[M_table](ThetaNum) //创建一个M_ijk表存储每个M_ijk的标号及其权重——所有样本的m_ijk权重统计之和
////			for (v <- 0 until ThetaNum) //创建数组中每个对象
////				M_ijk(v) = new M_table
////			val Theta_ijk = new Array[Double](ThetaNum) //模型参数顺序表——类似M_ijk表,由SEM_Maxlikelihood计算得出
//
//
////			println("========================结构学习 完整数据 及其 P(L|D)==============")
////			reference.MendedData.foreach(println(_))
//			//计算当前候选模型的期望BIC评分
//			// SEM.Maxlikelihood(reference.MendedData, candidate_model(i), M_ijk, Theta_ijk, ThetaNum)
//			// 计算参数
//			//Incremental.compute_theta(candidate_model(i), M_ijk, Theta_ijk, ThetaNum, mended_data_S) //参数估计并计算M_ijk的权重值
//
//			//temp_score = BIC(candidate_model(i), M_ijk, Theta_ijk, sample_num, S_node_parents_Array_No_Related)
//
//			candidate_model(i).create_CPT_F()
//			candidate_model(i).create_M_ijk_F()
//			SEM_F.Maxlikelihood_F(candidate_model(i))
//			//			println("--------------------Q_BIC_F----------------")
//			Q_BIC_F(candidate_model(i))
//			//			println("--------------------bn.Q_BIC----------------")
//			//			bn.Q_BIC.foreach(println(_))
//			//			println("--------------------candidate_model(i).Q_BIC----------------")
//			//			candidate_model(i).Q_BIC.foreach(println(_))
//			temp_Q_score = old_Q_score
//			for (j <- 0 until candidate_model(i).change_node_num){
//				var num_id = candidate_model(i).change_node(j)
//				println("num_id" + num_id + "  temp_Q_score = " + temp_Q_score + " - " + bn.Q_BIC(num_id) + " + " + candidate_model(i).Q_BIC(num_id))
//				temp_Q_score = temp_Q_score - bn.Q_BIC(num_id) + candidate_model(i).Q_BIC(num_id)
//			}
//			println("第" + (i + 1) + "个候选模型的期望BIC评分: " + temp_Q_score)
//			println()
//			outLog = new FileWriter(logFile.incremental_Learning, true)
//			outLog.write("第" + (i + 1) + "个候选模型的期望BIC评分: " + temp_Q_score + "\r\n\r\n")
//			outLog.close()
//			if (temp_Q_score > new_Q_score) {
//				//				Search.copy_model(optimal_model, candidate_model(i)) //optimal_model←candidate_model[i]
//				//				optimal_Theta.clear()
//				//				optimal_Theta ++= Theta_ijk //optimal_Theta←Theta_ijk
//				optimal_model_F = candidate_model(i)
//				new_Q_score = temp_Q_score
//			}
//		}
//	}
//
//	//计算最优候选模型的BIC评分,若new_score > incremental_Learning.old_score，则执行EM算法；否则，
////	Search.copy_model(optimal_model, optimal_model_F)
////	for (i <- 0 until n)
////		optimal_model.r(i) = bn.r(i) //设置BN中每个变量的势ri
////	optimal_model.qi_computation() //根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
////	optimal_model.create_CPT()                                           //optimal_model的CPT结构与原bn的CPT结构不同，因此需要先释放旧的CPT，再创建新的CPT
////	//	optimal_model.optimal_Theta_to_CPT(optimal_Theta)
////	for (i <- 0 until n) {
////		if(optimal_model_F.change_node_num == 1 && optimal_model_F.change_node(0) == i){
////			for (j <- 0 until optimal_model.q(i)) {
////				for (k <- 0 until optimal_model.r(i)) {
////					optimal_model.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
////				}
////			}
////		}
////		else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(0) == i){
////			for (j <- 0 until optimal_model.q(i)) {
////				for (k <- 0 until optimal_model.r(i)) {
////					optimal_model.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
////				}
////			}
////		}
////		else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(1) == i){
////			for (j <- 0 until optimal_model.q(i)) {
////				for (k <- 0 until optimal_model.r(i)) {
////					optimal_model.theta(i)(j)(k) = optimal_model_F.theta(1)(j)(k)
////				}
////			}
////		}else{
////			for (j <- 0 until optimal_model.q(i)) {
////				for (k <- 0 until optimal_model.r(i)) {
////					optimal_model.theta(i)(j)(k) = bn.theta(i)(j)(k)
////				}
////			}
////		}
////	}
//	//val new_score_BIC = BIC_PD(optimal_model, PD_log)
//
//	val similarity1 = new_Q_score - IncrementalLearning.old_score_BIC                   //参数相似度
//	println("候选模型与当前模型的BIC评分之差: " + similarity1)
//
//	var outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
//	outLog.write("当前候选模型中的最优结构, BIC评分: " + new_Q_score + "\r\n")
//	outLog.write("候选模型与当前模型的BIC评分之差: " + similarity1 + "\r\n")
//	outLog.write("当前候选模型中的最优结构: " + "\r\n")
//	outLog.close()
//    output.structureAppendFile(optimal_model_F, logFile.incremental_Learning)
//
////	println("当前候选模型中的最优结构, 期望BIC评分: " + new_score)
////	println("当前候选模型中的最优结构, BIC评分: " + new_score_BIC)
//
//
//	//如果新的最优候选模型的BIC评分高，则将bn设置为新的最优候选模型
//
//	if (similarity1 > reference.incremental_threshold) {
//		//if (new_score_BIC > StructureLearning.old_score_BIC) {   //参数未迭代优化的局部最优模型BIC评分 > 先前最优模型BIC评分
//		IncrementalLearning.old_score_BIC = new_Q_score
//		Search.copy_model(bn,optimal_model_F)                     //G←optimal_model
//		bn.qi_computation()
//
//		for (j <- 0 until optimal_model_F.change_node_num){
//			var num_id = optimal_model_F.change_node(j)
//			bn.Q_BIC(num_id) = optimal_model_F.Q_BIC(num_id)
//		}
//		//		bn.create_CPT()                                           //optimal_model的CPT结构与原bn的CPT结构不同，因此需要先释放旧的CPT，再创建新的CPT
//		//		bn.optimal_Theta_to_CPT(optimal_Theta)                    //θ←optimal_Theta
//		// 更新BN的局部参数
//		for (i <- 0 until n) {
//			if(optimal_model_F.change_node_num == 1 && optimal_model_F.change_node(0) == i){
//				bn.theta(i) = new Array[Array[Double]](bn.q(i))
//				bn.M_ijk(i) = new Array[Array[Double]](bn.q(i))
//				for (j <- 0 until bn.q(i)){
//					bn.theta(i)(j) = new Array[Double](bn.r(i))     //theta[i][j][k]==θijk
//					bn.M_ijk(i)(j) = new Array[Double](bn.r(i))
//				}
//
//				for (j <- 0 until bn.q(i)) {
//					for (k <- 0 until bn.r(i)) {
//						bn.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
//						bn.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(0)(j)(k)
//					}
//				}
//			}
//			else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(0) == i){
//				bn.theta(i) = new Array[Array[Double]](bn.q(i))
//				bn.M_ijk(i) = new Array[Array[Double]](bn.q(i))
//				for (j <- 0 until bn.q(i)){
//					bn.theta(i)(j) = new Array[Double](bn.r(i))     //theta[i][j][k]==θijk
//					bn.M_ijk(i)(j) = new Array[Double](bn.r(i))
//				}
//				for (j <- 0 until bn.q(i)) {
//					for (k <- 0 until bn.r(i)) {
//						bn.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
//						bn.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(0)(j)(k)
//					}
//				}
//			}
//			else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(1) == i){
//				bn.theta(i) = new Array[Array[Double]](bn.q(i))
//				bn.M_ijk(i) = new Array[Array[Double]](bn.q(i))
//				for (j <- 0 until bn.q(i)){
//					bn.theta(i)(j) = new Array[Double](bn.r(i))     //theta[i][j][k]==θijk
//					bn.M_ijk(i)(j) = new Array[Double](bn.r(i))
//				}
//				for (j <- 0 until bn.q(i)) {
//					for (k <- 0 until bn.r(i)) {
//						bn.theta(i)(j)(k) = optimal_model_F.theta(1)(j)(k)
//						bn.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(0)(j)(k)
//					}
//				}
//			}
//		}
//
//		println("当前候选模型中的最优结构, BIC评分: " + new_Q_score)
//
////		println("==================最优候选模型==================")
////		output.CPT_FormatToConsole(bn)
////		output.structureToConsole(bn)
//
////		println("==================父节点的组合情况数qi==================")
////		for (s <- bn.q)
////			print(s + " ")
////		println()
////		println("=================节点的取值数ri==================")
////		for (s <- bn.r)
////			print(s + " ")
////		println()
//
//		return 1
//      //书中的算法如上，未知——局部最优模型的BIC在与old_score比较前先进行EM迭代优化后的结果（未知策略没有书中策略好）
//    }else{
//		println("====================================== 增量学习 收敛 ======================================")
//		var outLog = new FileWriter(logFile.incremental_Learning, true) //以追加写入的方式创建FileWriter对象
//		outLog.write("====================================== 增量学习 收敛 ==========================================\r\n")
//		outLog.close()
//		return 0
//	}
//
//
////    if ((bn.NodeNum - 1) == current_node && 3 == iteration_ClimbHill) {  //最后一次模型选择后，将最优模型的BIC评分输出至logFile.BIC_score
////      val outBIC = new FileWriter(logFile.BIC_score)
////      outBIC.write(old_score + "\r\n")
////      outBIC.close()
////    }
////
////    outLog = new FileWriter(logFile.incremental_Learning, true)
////    outLog.write("当前最优模型, BIC评分: " + incremental_Learning.old_score_BIC + "\r\n")
////    outLog.write("当前最优模型的结构与CPT: \r\n")
////    outLog.close()
////    output.structureAppendFile(bn, logFile.incremental_Learning)
////    output.CPT_FormatAppendFile(bn, logFile.incremental_Learning)
////    outLog = new FileWriter(logFile.incremental_Learning, true)
////    outLog.write("\r\n")
////    outLog.close()
//}
//
//	/*********************BIC评分函数****************************/
//	def BIC_PD(bn: BayesianNetwork, PD_log: Double): Double = {
//
//		val n = bn.NodeNum
//		var model_dimension = 0
//		var score = 0.0
//
//		for (i <- 0 until n)
//			model_dimension += bn.q(i) * (bn.r(i) - 1)  //独立参数个数
//		val score1 = (model_dimension / 2.0).toDouble * log(reference.MendedData_Num)
//		score = PD_log - score1        //BIC分值
////		println("独立参数个数 : " + model_dimension)
////		println(score + " = " + PD_log + " - " + score1 )
//		score
//	}
//	/*********************BIC评分函数****************************/
//	def Q_BIC(bn: BayesianNetwork) = {
//
//		val n = bn.NodeNum
//		var model_dimension = 0
//		var likelihood = 0.0
//		var score = 0.0
//		var score1 = 0.0
//
//		for(i <- 0 until bn.NodeNum){
//			likelihood = 0.0
//			for(j <- 0 until bn.q(i))
//				for(k <- 0 until bn.r(i)) {
//					likelihood += bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))                                    //log似然度
//				}
//			model_dimension = bn.q(i) * (bn.r(i) - 1)  //独立参数个数
//			score1 = (model_dimension / 2.0).toDouble * log(reference.MendedData_Num.toDouble)
//			score = likelihood - score1
////			println(likelihood - score1 + " " + likelihood + " " +score1)
//			bn.Q_BIC(i) = score
//		}
//	}
//
//	def S_BIC(bn: BayesianNetwork, S_node_parents_Array_No_Related:Array[Int]) = {
//
//		val n = bn.NodeNum
//		var model_dimension = 0
//		var likelihood = 0.0
//		var score = 0.0
//		var score1 = 0.0
//
//		for(i <- 0 until bn.NodeNum){
//			if(S_node_parents_Array_No_Related(i) == 0){
//				likelihood = 0.0
//				for(j <- 0 until bn.q(i))
//					for(k <- 0 until bn.r(i)) {
//						likelihood += bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))                                    //log似然度
//					}
//				model_dimension = bn.q(i) * (bn.r(i) - 1)  //独立参数个数
//				score1 = (model_dimension / 2.0).toDouble * log(reference.MendedData_Num.toDouble)
//				score = likelihood - score1
////				println(likelihood - score1 + " " + likelihood + " " +score1)
//				bn.Q_BIC(i) = score
//			}
//		}
//	}

	def generate_Initial_Structure(bn:BayesianNetwork,structure: Array[Array[Int]], S_node_parents_Array_No_Related:Array[Int]): Unit ={
		for( i <- 0 to bn.NodeNum-1){
			for( j <- 0 to bn.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else if(S_node_parents_Array_No_Related(i) == 1 && S_node_parents_Array_No_Related(j) == 1) {
					structure(i)(j) = bn.structure(i)(j)
				}else{
					if(bn.l(i) == "U" || bn.l(i) == "R") {  //初始结构约束1、4
						structure(i)(j) = 0
					}else if(bn.l(i) == "L"){       //初始结构约束2、3、4
						if((bn.l(j) == "I" && j == i-reference.LatentNum) || bn.l(j) == "R")
							structure(i)(j) = 1
						else
							structure(i)(j) = 0
					}else if(bn.l(i) == "I"){       //初始结构约束4
						if(bn.l(j) == "R")
							structure(i)(j) = 1
						else
							structure(i)(j) = 0
					}
				}
			}
		}
	}

	def generate_EdgeConstraint_Violate(bn:BayesianNetwork,structure: Array[Array[Int]], S_node_parents_Array_No_Related:Array[Int]) {
		for( i <- 0 to bn.NodeNum-1){
			for( j <- 0 to bn.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else if(S_node_parents_Array_No_Related(j) == 1) {
					structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U") {
						if(bn.l(j) == "I")
							structure(i)(j) = 1
						else if(bn.l(j) == "R")
							structure(i)(j) = 0
						else if(bn.l(j) == "L")
							structure(i)(j) = 1
						else if(bn.l(j) == "U")
							structure(i)(j) = 1
					}else if(bn.l(i) == "L"){
						if(bn.l(j) == "U")
							structure(i)(j) = 0
						else if(bn.l(j) == "R" && j != i - 2 * reference.LatentNum)
							structure(i)(j) = 0
						else if(bn.l(j) == "I")
							structure(i)(j) = 0
					}else if(bn.l(i) == "I"){
						if(bn.l(j) == "U" || bn.l(j) == "L")
							structure(i)(j) = 0
						else if(bn.l(j) == "R" && j != i - reference.LatentNum)
							structure(i)(j) = 0
						else
							structure(i)(j) = 0
					}else if(bn.l(i) == "R"){
						structure(i)(j) = 0
					}
				}
			}
		}
	}



	def findSubgraph(bn: BayesianNetwork, ID_Array: Array[Double], S_node_Array: Array[Int], S_node_parents_Array: Array[Int]): Unit = {
		val SubGraph_threshold = reference.SubGraph_threshold
//		var S_node_initial = new Array[Int](bn.NodeNum) //节点ID大于阈值，或者，节点的父节点和子节点ID大于阈值
		var S_node = new Array[Int](bn.NodeNum) //节点ID大于阈值，或者，节点的父节点和子节点ID大于阈值
		for (i <- 0 until bn.NodeNum) {
			if (ID_Array(i) >= SubGraph_threshold) {
//				S_node_initial(i) = 1
				S_node(i) = 1
			}
		}
		println()
		bn.l.foreach(println(_))
		println("S_node_initial : ")
//		S_node_initial.foreach(println(_))
		println()
//		for (i <- 0 until bn.NodeNum) {
//			for (j <- i+1 until bn.NodeNum) {
//				if (bn.structure(i)(j) == 1)
//					if (S_node_initial(i) == 1)
//						S_node(j) = 1
//				if (bn.structure(j)(i) == 1)
//					if (S_node_initial(i) == 1 && bn.l(j)=="L")
//						S_node(j) = 1
//			}
//		}
//		// 隐变量的儿子
//		for (i <- 0 until bn.NodeNum) {
//			if (bn.l(i)=="L" || S_node(i) == 1 ){
//				for (j <- 0 until bn.NodeNum) {
//					if (bn.structure(i)(j) == 1)
//						S_node(j) = 1
//				}
//			}
//		}
		println()
		println("S_node : ")
		S_node.foreach(println(_))
		println()
		// 分支限界（广度优先遍历）寻找子图
		var livenode_queue = new ArrayBuffer[Int]
		var node_visit = new Array[Int](bn.NodeNum) // 标记节点是否被访问过
		for (i <- 0 until bn.NodeNum) {
			if (S_node(i) == 1) {
				node_visit(i) = 1
				livenode_queue += i
			}
		}


		var current_node = 0




		while (livenode_queue.length != 0) {
			livenode_queue.foreach(print(_))
			println()
			current_node = livenode_queue(0) // 获取活节点队列的第一个节点
			livenode_queue -= current_node // 将该节点移除活节点队列
			S_node_Array(current_node) = 1 // 将当前节点加入到子图

			for (i <- 0 until bn.NodeNum) {

				if (bn.structure(current_node)(i) == 1) { //当前节点的子节点
					if (node_visit(i) == 0) { //该节点未被访问过

//						if (S_node(i) == 1) {}
//						else if (S_node(current_node) == 1 && S_node(i) != 1) {
//							node_visit(i) = 1
//							livenode_queue += i
//						}
//						else if (S_node(current_node) != 1 && S_node(i) != 1) {}
//						else if (S_node_Array(current_node) == 1 && bn.l(current_node) == "L") {
//							node_visit(i) = 1
//							livenode_queue += i
//						}
						if (S_node(current_node) == 1) {
							node_visit(i) = 1
							livenode_queue += i
						}
						else if (S_node_Array(current_node) == 1 && bn.l(current_node) == "L") {
							node_visit(i) = 1
							livenode_queue += i
						}


					}
				}

				if (bn.structure(i)(current_node) == 1) { //当前节点的父节点
					if (node_visit(i) == 0) { //该节点未被访问过
						if (S_node_Array(current_node) == 1 && bn.l(i) == "L") {
							node_visit(i) = 1
							livenode_queue += i
						}
					}
				}

			}

		}


		for (i <- 0 until bn.NodeNum) {
			if (S_node_Array(i) == 1) {
				for (j <- 0 until bn.NodeNum) {
					if (bn.structure(j)(i) == 1 && S_node_Array(j) ==0){
						S_node_parents_Array(j) = 1
					}
				}
			}
		}
//					println("S_node_Array : ")
//					S_node_Array.foreach(println(_))
//					println("S_node_parents_Array : ")
//					S_node_parents_Array.foreach(println(_))
//					println("S_node_children_Array : ")
//					S_node_children_Array.foreach(println(_))
	}

	//后处理参数
	/** 根据M_ijk更新参数theta */
	def postTheta(bn: BayesianNetwork)={
		var flag0 = 0
		var k_Theta = new ArrayBuffer[Double]
		for (i <- 0 until bn.NodeNum){
			for (j <- 0 until bn.q(i)) {
				flag0 = 0
				k_Theta.clear()
				for (k <- 0 until bn.r(i) )
					k_Theta += bn.theta(i)(j)(k)
				val max = k_Theta.max
				val min = k_Theta.min

				if(min == 0){
//					println("postTheta : " + i + " " + j)
					if(max == 1){
						for (t <- 0 until k_Theta.length ) {
							if(k_Theta(t) == 1){
								k_Theta(t) = (bn.r(i)-1.0)/ bn.r(i)
							}else{
								k_Theta(t) = 1.0/ (bn.r(i)*(bn.r(i)-1))
							}

						}
					}else{
						var min_no0 = 1.0
						var temp0 = 0.0
						// 找到非0的最小值 min_no0
						for (t <- 0 until k_Theta.length ) {
							if(k_Theta(t) != 0 && k_Theta(t) < min_no0){
								min_no0 = k_Theta(t)
							}
						}
						// 令参数0等于非0的最小值 min_no0
						for (t <- 0 until k_Theta.length ) {
							if(k_Theta(t) == 0){
								k_Theta(t) = min_no0
							}
							temp0 += k_Theta(t)
						}
						// 重新计算参数
						for (t <- 0 until k_Theta.length ) {
							k_Theta(t) = k_Theta(t) / temp0
						}
					}
					for (k <- 0 until bn.r(i) ) {
						bn.theta(i)(j)(k) = k_Theta(k)
					}
				}
			}
		}
	}
}


