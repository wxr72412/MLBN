package BNLV_learning.Learning

import java.io.FileWriter
import java.text.NumberFormat
import java.util.Date

import BNLV_learning.Global._
import BNLV_learning.Input.BN_Input
import BNLV_learning.Learning.ParameterLearning.ijk_computation
import BNLV_learning.Output.BN_Output
import BNLV_learning._
import BNLV_learning.Incremental.IncrementalLearning
import BNLV_learning.Learning.StructureLearning.BIC_PD
import org.apache.spark.storage.StorageLevel
import BNLV_learning.Learning._

import scala.collection.mutable.ArrayBuffer
import scala.math._

/**
  * Created by Gao on 2016/6/27.
  */
object StructureLearning2 {

	val nf = NumberFormat.getNumberInstance()
	nf.setMaximumFractionDigits(4)                       //设置输出格式保留小数位数


	def learningProcess1(bn: BayesianNetwork, bn_Array: Array[BayesianNetwork], incremental_flag:Int, incremental_change_mark:Array[Array[Int]]) = {

		var P_time = 0.0
		var S_time = 0.0

		val bn = new BayesianNetwork(reference.NodeNum_Total)
//		println(reference.NodeNum_Total)
		println()
		if(reference.R_type == "chest_clinic_2L_merge"){
			for (i <- 0 to 7) {
//				println(i)
				bn.r(i) = 2
			}
//			bn.r.foreach(println(_))
//			System.exit(0)
			// 结构
//			val map=Map("1"->1,"2"->2,"3"->3,"4"->1,"5"->2,"6"->3,"7"->4,"8"->4)

//			合并后 -> 子图

//			"1"->1
//			"2"->2
//			"5"->3
//			"7"->4

//			"3"->1
//			"4"->2
//			"6"->3
//			"8"->4
			bn.structure(1-1)(1-1) = bn_Array(0).structure(1-1)(1-1)
			bn.structure(1-1)(2-1) = bn_Array(0).structure(1-1)(2-1)
			bn.structure(1-1)(5-1) = bn_Array(0).structure(1-1)(3-1)
			bn.structure(1-1)(7-1) = bn_Array(0).structure(1-1)(4-1)

			bn.structure(2-1)(1-1) = bn_Array(0).structure(2-1)(1-1)
			bn.structure(2-1)(2-1) = bn_Array(0).structure(2-1)(2-1)
			bn.structure(2-1)(5-1) = bn_Array(0).structure(2-1)(3-1)
			bn.structure(2-1)(7-1) = bn_Array(0).structure(2-1)(4-1)

			bn.structure(5-1)(1-1) = bn_Array(0).structure(3-1)(1-1)
			bn.structure(5-1)(2-1) = bn_Array(0).structure(3-1)(2-1)
			bn.structure(5-1)(5-1) = bn_Array(0).structure(3-1)(3-1)
			bn.structure(5-1)(7-1) = bn_Array(0).structure(3-1)(4-1)

			bn.structure(7-1)(1-1) = bn_Array(0).structure(4-1)(1-1)
			bn.structure(7-1)(2-1) = bn_Array(0).structure(4-1)(2-1)
			bn.structure(7-1)(5-1) = bn_Array(0).structure(4-1)(3-1)
			bn.structure(7-1)(7-1) = bn_Array(0).structure(4-1)(4-1)

			bn.structure(3-1)(3-1) = bn_Array(1).structure(1-1)(1-1)
			bn.structure(3-1)(4-1) = bn_Array(1).structure(1-1)(2-1)
			bn.structure(3-1)(6-1) = bn_Array(1).structure(1-1)(3-1)
			bn.structure(3-1)(8-1) = bn_Array(1).structure(1-1)(4-1)

			bn.structure(4-1)(3-1) = bn_Array(1).structure(2-1)(1-1)
			bn.structure(4-1)(4-1) = bn_Array(1).structure(2-1)(2-1)
			bn.structure(4-1)(6-1) = bn_Array(1).structure(2-1)(3-1)
			bn.structure(4-1)(8-1) = bn_Array(1).structure(2-1)(4-1)

			bn.structure(6-1)(3-1) = bn_Array(1).structure(3-1)(1-1)
			bn.structure(6-1)(4-1) = bn_Array(1).structure(3-1)(2-1)
			bn.structure(6-1)(6-1) = bn_Array(1).structure(3-1)(3-1)
			bn.structure(6-1)(8-1) = bn_Array(1).structure(3-1)(4-1)

			bn.structure(8-1)(3-1) = bn_Array(1).structure(4-1)(1-1)
			bn.structure(8-1)(4-1) = bn_Array(1).structure(4-1)(2-1)
			bn.structure(8-1)(6-1) = bn_Array(1).structure(4-1)(3-1)
			bn.structure(8-1)(8-1) = bn_Array(1).structure(4-1)(4-1)
			val output = new BN_Output
//			output.structureToConsole(bn)
//			System.exit(0)
			//创建CPT
			bn.qi_computation()
			bn.create_CPT()
			bn.create_M_ijk()
//			bn.r.foreach(println(_))
			var a = 0
			var b = 0
			a = 1
			b = 1
//			output.CPT_FormatToConsole(bn)
//			output.CPT_FormatToConsole(bn_Array(0))
//			output.CPT_FormatToConsole(bn_Array(1))
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(0).theta(b-1)(j)(k)
				}
			}
			a = 2
			b = 2
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(0).theta(b-1)(j)(k)
				}
			}
			a = 5
			b = 3
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(0).theta(b-1)(j)(k)
				}
			}
			a = 7
			b = 4
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(0).theta(b-1)(j)(k)
				}
			}
			a = 3
			b = 1
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
			a = 4
			b = 2
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
			a = 6
			b = 3
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
			a = 8
			b = 4
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
//			output.CPT_FormatToConsole(bn)
//			System.exit(0)
		}
		else if(reference.R_type == "chest_clinic_2L_3+5_merge"){
			for (i <- 0 to 7) {
				bn.r(i) = 2
			}

			bn.latent(1-1) = bn_Array(0).latent(1-1)
			bn.latent(5-1) = bn_Array(0).latent(2-1)
			bn.latent(7-1) = bn_Array(0).latent(3-1)

			bn.latent(2-1) = bn_Array(1).latent(1-1)
			bn.latent(3-1) = bn_Array(1).latent(2-1)
			bn.latent(4-1) = bn_Array(1).latent(3-1)
			bn.latent(6-1) = bn_Array(1).latent(4-1)
			bn.latent(8-1) = bn_Array(1).latent(5-1)

			bn.structure(1-1)(1-1) = bn_Array(0).structure(1-1)(1-1)
			bn.structure(1-1)(5-1) = bn_Array(0).structure(1-1)(2-1)
			bn.structure(1-1)(7-1) = bn_Array(0).structure(1-1)(3-1)

			bn.structure(5-1)(1-1) = bn_Array(0).structure(2-1)(1-1)
			bn.structure(5-1)(5-1) = bn_Array(0).structure(2-1)(2-1)
			bn.structure(5-1)(7-1) = bn_Array(0).structure(2-1)(3-1)

			bn.structure(7-1)(1-1) = bn_Array(0).structure(3-1)(1-1)
			bn.structure(7-1)(5-1) = bn_Array(0).structure(3-1)(2-1)
			bn.structure(7-1)(7-1) = bn_Array(0).structure(3-1)(3-1)

			bn.structure(2-1)(2-1) = bn_Array(1).structure(1-1)(1-1)
			bn.structure(2-1)(3-1) = bn_Array(1).structure(1-1)(2-1)
			bn.structure(2-1)(4-1) = bn_Array(1).structure(1-1)(3-1)
			bn.structure(2-1)(6-1) = bn_Array(1).structure(1-1)(4-1)
			bn.structure(2-1)(8-1) = bn_Array(1).structure(1-1)(5-1)

			bn.structure(3-1)(2-1) = bn_Array(1).structure(2-1)(1-1)
			bn.structure(3-1)(3-1) = bn_Array(1).structure(2-1)(2-1)
			bn.structure(3-1)(4-1) = bn_Array(1).structure(2-1)(3-1)
			bn.structure(3-1)(6-1) = bn_Array(1).structure(2-1)(4-1)
			bn.structure(3-1)(8-1) = bn_Array(1).structure(2-1)(5-1)

			bn.structure(4-1)(2-1) = bn_Array(1).structure(3-1)(1-1)
			bn.structure(4-1)(3-1) = bn_Array(1).structure(3-1)(2-1)
			bn.structure(4-1)(4-1) = bn_Array(1).structure(3-1)(3-1)
			bn.structure(4-1)(6-1) = bn_Array(1).structure(3-1)(4-1)
			bn.structure(4-1)(8-1) = bn_Array(1).structure(3-1)(5-1)

			bn.structure(6-1)(2-1) = bn_Array(1).structure(4-1)(1-1)
			bn.structure(6-1)(3-1) = bn_Array(1).structure(4-1)(2-1)
			bn.structure(6-1)(4-1) = bn_Array(1).structure(4-1)(3-1)
			bn.structure(6-1)(6-1) = bn_Array(1).structure(4-1)(4-1)
			bn.structure(6-1)(8-1) = bn_Array(1).structure(4-1)(5-1)

			bn.structure(8-1)(2-1) = bn_Array(1).structure(5-1)(1-1)
			bn.structure(8-1)(3-1) = bn_Array(1).structure(5-1)(2-1)
			bn.structure(8-1)(4-1) = bn_Array(1).structure(5-1)(3-1)
			bn.structure(8-1)(6-1) = bn_Array(1).structure(5-1)(4-1)
			bn.structure(8-1)(8-1) = bn_Array(1).structure(5-1)(5-1)
			val output = new BN_Output
			//			output.structureToConsole(bn)
			//			System.exit(0)
			//创建CPT
			bn.qi_computation()
			bn.create_CPT()
			bn.create_M_ijk()
			//			bn.r.foreach(println(_))
			var a = 0
			var b = 0
			a = 1
			b = 1
			//			output.CPT_FormatToConsole(bn)
			//			output.CPT_FormatToConsole(bn_Array(0))
			//			output.CPT_FormatToConsole(bn_Array(1))
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(0).theta(b-1)(j)(k)
				}
			}
			a = 5
			b = 2
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(0).theta(b-1)(j)(k)
				}
			}
			a = 7
			b = 3
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(0).theta(b-1)(j)(k)
				}
			}
			a = 2
			b = 1
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
			a = 3
			b = 2
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
			a = 4
			b = 3
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
			a = 6
			b = 4
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
			a = 8
			b = 5
			for (j <- 0 until bn.q(a-1)) {
				for (k <- 0 until bn.r(a-1)) {
					bn.theta(a-1)(j)(k) = bn_Array(1).theta(b-1)(j)(k)
				}
			}
		}
		else{
			for (i <- 0 until 3) {
				bn.r(i) = bn_Array(0).r(i)
				bn.l(i) = bn_Array(0).l(i)
			}
			for (ii <- 0 until reference.LatentNum_Total) {
				for (i <- 3 until reference.NodeNum) {
					bn.r(i + 3 * ii) = bn_Array(ii).r(i)
					bn.l(i + 3 * ii) = bn_Array(ii).l(i)
				}
			}
			//		  bn.r.foreach(println(_))
			//		  bn.l.foreach(println(_))
			//		  System.exit(0)

			for (ii <- 0 until reference.LatentNum_Total) {
				for (i <- 0 until reference.NodeNum) {
					for (j <- 0 until reference.NodeNum) {
						if( ii==0 && i < 3 && j < 3 )
							bn.structure(i)(j) = bn_Array(ii).structure(i)(j) //结构
						else if( i >= 3 && j >= 3)
							bn.structure(i + 3 * ii)(j + 3 * ii) = bn_Array(ii).structure(i)(j) //结构
						else if( i < 3 && j >= 3 )
							bn.structure(i)(j + 3 * ii) = bn_Array(ii).structure(i)(j) //结构
					}
				}
			}
			//			val output = new BN_Output
			//		  output.structureToConsole(bn)
			//		  System.exit(0)

			//创建CPT
			bn.qi_computation()
			bn.create_CPT()
			bn.r.foreach(println(_))
			for (ii <- 0 until reference.LatentNum_Total) {
				for (i <- 0 until reference.NodeNum) {
					if( ii==0 && i < 3){
						for (j <- 0 until bn_Array(ii).q(i)) {
							for (k <- 0 until bn_Array(ii).r(i)) {
								bn.theta(i)(j)(k) = bn_Array(ii).theta(i)(j)(k)
							}
						}
					}
					else if(i >= 3){
						for (j <- 0 until bn_Array(ii).q(i)) {
							for (k <- 0 until bn_Array(ii).r(i)) {
								bn.theta(i + 3 * ii)(j)(k) = bn_Array(ii).theta(i)(j)(k)
							}
						}
					}
				}
			}
		}

//		learningProcess2(bn)
//	}
//
//	def learningProcess2(bn: BayesianNetwork) = {
		println("******************结构学习开始 1 20180908*********************")

		val input = new BN_Input
		val output = new BN_Output

//		val iteration_EM = reference.EM2_iterationNum                            //EM迭代次数
		val EdgeConstraint = Array.ofDim[Int](bn.NodeNum, bn.NodeNum)            //边方向约束（必须保留的边）
		val EdgeConstraint_violate = Array.ofDim[Int](bn.NodeNum, bn.NodeNum)            //边方向约束（不能生成的边）

		var outLog = new FileWriter(logFile.parameterLearning)    //以“非”追加写入的方式创建FileWriter对象——参数学习日志写对象
		outLog.write("BNLV结构学习时的EM优化次数\r\n\r\n")
		outLog.close()
		println("******************结构学习开始 2 生成初始结构*********************")
		// 按文件追加的方式，输出BN的结构到文件
		output.structureAppendFile(bn, logFile.structureLearning)

		//复制初始结构给约束结构（必须保留的边）
		Search.copy_structure(EdgeConstraint, bn.structure, bn.NodeNum)




		////////////////////////////////////////////////////////////////////////////////////////////////////////////
		// 填充数据
		val Ln = reference.LatentNum_Total
		val Lr = new Array[Int](Ln)
		for(i <- 0 until Ln){
			Lr(i) = reference.Latent_r(i)
		}
		var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
		for(i <- 0 until Ln){
			c *= Lr(i)
		}
		reference.c = c

		//		println("====================data====================")
		//		var d = reference.inFile.collect()
		//		d.foreach(println(_))

		var complete_data = reference.inFile
		if(reference.R_type == "chest_clinic_2L_merge" || reference.R_type == "chest_clinic_2L_3+5_merge" && reference.LatentNum != 0){
			complete_data = reference.inFile.map { //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
				line => //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
					//截取与子图相关的数据
					val data_Array = line.split(separator.data)
					var lines = ""
					lines += data_Array(1-1) + separator.data
					lines += data_Array(3-1) + separator.data
					lines += data_Array(4-1) + separator.data
					lines += data_Array(6-1) + separator.data
					lines += data_Array(7-1) + separator.data
					lines += data_Array(8-1)
					var Lx = Array.ofDim[Int](Ln + 1)
					var paddingdata_c = ""
					while (Lx(0) == 0) {
						var paddingdata = ""
						for (i <- 1 until Ln + 1) { //1,2,3,....,Ln
							paddingdata += separator.data + (Lx(i) + 1).toString
						}
						paddingdata_c += lines + paddingdata + separator.tab
						Lx(Ln) += 1
						for (i <- 0 until Ln) { //0,1,2,....,Ln-1
							if (Lx(Ln - i) == Lr(Ln - i - 1)) {
								Lx(Ln - i) = 0
								Lx(Ln - i - 1) += 1
							}
						}
					}
					paddingdata_c
			}
		}else if(reference.R_type == "chest_clinic_2L_merge" || reference.R_type == "chest_clinic_2L_3+5_merge" && reference.LatentNum == 0){
			complete_data = reference.inFile.map { //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
				line =>
					val data_Array = line.split(separator.data)
					var lines = ""
					lines += data_Array(1-1) + separator.data
					lines += data_Array(3-1) + separator.data
					lines += data_Array(4-1) + separator.data
					lines += data_Array(6-1) + separator.data
					lines += data_Array(7-1) + separator.data
					lines += data_Array(8-1) + separator.data
					lines += data_Array(2-1) + separator.data
					lines += data_Array(5-1)
					lines
			}
		}
		else{
			//			System.exit(0)
			complete_data = reference.inFile.map { //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
				line => //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1

					val data_Array = line.split(separator.data)
					var users = ""
					for (j <- 0 to 2) {
						users += data_Array(j) + separator.data
					}

					var Lx = Array.ofDim[Int](Ln + 1)

					var paddingdata_c = ""
					while (Lx(0) == 0) {
						var paddingdata = ""
						for (i <- 1 until Ln) { //1,2,3,....,Ln
							paddingdata += data_Array(3) + separator.data + data_Array(3+i) + separator.data + (Lx(i) + 1).toString + separator.data
						}
						paddingdata += data_Array(3) + separator.data + data_Array(3+Ln) + separator.data + (Lx(Ln) + 1).toString

						paddingdata_c += users + paddingdata + separator.tab

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

			}
		}

//		println("====================complete_data====================")
//		var d = complete_data.collect()
//		d.foreach(println(_))
//		println("reference.LatentNum : "+reference.LatentNum)
//		System.exit(0)
		reference.Data = complete_data
		reference.Data.persist(StorageLevel.MEMORY_AND_DISK_SER)
		reference.SampleNum = reference.Data.count()                         //原始数据文档中的样本数
		if(reference.R_type == "chest_clinic_2L_merge" || reference.R_type == "chest_clinic_2L_3+5_merge")
			reference.EM_threshold = reference.EM_threshold_chestclinic
		else {
			reference.EM_threshold = reference.EM_threshold_origin * reference.SampleNum
			reference.SEM_threshold = reference.SEM_threshold_origin * reference.SampleNum
		}
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////



		val structure_before_incrementalupdating = Array.ofDim[Int](bn.NodeNum, bn.NodeNum)            //边方向约束（不能生成的边）

		if(incremental_flag == 0){
			//生成约束结构（不能生成的边）
			if(reference.R_type == "chest_clinic_2L_merge"){
//				EdgeConstraint_violate(2-1)(5-1) = 1
//				EdgeConstraint_violate(5-1)(3-1) = 1

				EdgeConstraint_violate(1-1)(3-1) = 1
				EdgeConstraint_violate(1-1)(4-1) = 1
				EdgeConstraint_violate(1-1)(6-1) = 1
				EdgeConstraint_violate(1-1)(8-1) = 1

				EdgeConstraint_violate(2-1)(3-1) = 1
				EdgeConstraint_violate(2-1)(4-1) = 1
				EdgeConstraint_violate(2-1)(6-1) = 1
				EdgeConstraint_violate(2-1)(8-1) = 1

				EdgeConstraint_violate(5-1)(3-1) = 1
				EdgeConstraint_violate(5-1)(4-1) = 1
				EdgeConstraint_violate(5-1)(6-1) = 1
				EdgeConstraint_violate(5-1)(8-1) = 1

				EdgeConstraint_violate(7-1)(3-1) = 1
				EdgeConstraint_violate(7-1)(4-1) = 1
				EdgeConstraint_violate(7-1)(6-1) = 1
				EdgeConstraint_violate(7-1)(8-1) = 1

				EdgeConstraint_violate(3-1)(1-1) = 1
				EdgeConstraint_violate(3-1)(2-1) = 1
				EdgeConstraint_violate(3-1)(5-1) = 1
				EdgeConstraint_violate(3-1)(7-1) = 1

				EdgeConstraint_violate(4-1)(1-1) = 1
				EdgeConstraint_violate(4-1)(2-1) = 1
				EdgeConstraint_violate(4-1)(5-1) = 1
				EdgeConstraint_violate(4-1)(7-1) = 1

				EdgeConstraint_violate(6-1)(1-1) = 1
				EdgeConstraint_violate(6-1)(2-1) = 1
				EdgeConstraint_violate(6-1)(5-1) = 1
				EdgeConstraint_violate(6-1)(7-1) = 1

				EdgeConstraint_violate(8-1)(1-1) = 1
				EdgeConstraint_violate(8-1)(2-1) = 1
				EdgeConstraint_violate(8-1)(5-1) = 1
				EdgeConstraint_violate(8-1)(7-1) = 1

			}
			else if(reference.R_type == "chest_clinic_2L_3+5_merge") {
				EdgeConstraint_violate(1-1)(2-1) = 1
				EdgeConstraint_violate(1-1)(3-1) = 1
				EdgeConstraint_violate(1-1)(4-1) = 1
//				EdgeConstraint_violate(1-1)(6-1) = 1
				EdgeConstraint_violate(1-1)(8-1) = 1

//				EdgeConstraint_violate(5-1)(2-1) = 1
//				EdgeConstraint_violate(5-1)(3-1) = 1
//				EdgeConstraint_violate(5-1)(4-1) = 1
				EdgeConstraint_violate(5-1)(6-1) = 1
//				EdgeConstraint_violate(5-1)(8-1) = 1

				EdgeConstraint_violate(7-1)(2-1) = 1
//				EdgeConstraint_violate(7-1)(3-1) = 1
//				EdgeConstraint_violate(7-1)(4-1) = 1
				EdgeConstraint_violate(7-1)(6-1) = 1
				EdgeConstraint_violate(7-1)(8-1) = 1

//				EdgeConstraint_violate(2-1)(1-1) = 1
				EdgeConstraint_violate(2-1)(5-1) = 1
				EdgeConstraint_violate(2-1)(7-1) = 1

				EdgeConstraint_violate(3-1)(1-1) = 1
//				EdgeConstraint_violate(3-1)(5-1) = 1
				EdgeConstraint_violate(3-1)(7-1) = 1

//				EdgeConstraint_violate(4-1)(1-1) = 1
				EdgeConstraint_violate(4-1)(5-1) = 1
//				EdgeConstraint_violate(4-1)(7-1) = 1

//				EdgeConstraint_violate(6-1)(1-1) = 1
				EdgeConstraint_violate(6-1)(5-1) = 1
//				EdgeConstraint_violate(6-1)(7-1) = 1

//				EdgeConstraint_violate(8-1)(1-1) = 1
				EdgeConstraint_violate(8-1)(5-1) = 1
				EdgeConstraint_violate(8-1)(7-1) = 1
			}
			else{
				generate_EdgeConstraint_Violate_noIncrenmental(bn,EdgeConstraint_violate)
			}
		}
		else{ // 增量更新 incremental_flag == 1

			input.structureFromFile(inputFile.incremental_structure, separator.structure, structure_before_incrementalupdating)
			if(reference.R_type == "chest_clinic"){
				EdgeConstraint_violate(2-1)(5-1) = 1
				EdgeConstraint_violate(5-1)(3-1) = 1
			}
			else if(reference.R_type == "chest_clinic_2L_3+5_merge") {
				Search.copy_structure(EdgeConstraint,structure_before_incrementalupdating,bn.NodeNum)

				EdgeConstraint(1-1)(1-1) = bn_Array(0).structure(1-1)(1-1)
				EdgeConstraint(1-1)(5-1) = bn_Array(0).structure(1-1)(2-1)
				EdgeConstraint(1-1)(7-1) = bn_Array(0).structure(1-1)(3-1)

				EdgeConstraint(5-1)(1-1) = bn_Array(0).structure(2-1)(1-1)
				EdgeConstraint(5-1)(5-1) = bn_Array(0).structure(2-1)(2-1)
				EdgeConstraint(5-1)(7-1) = bn_Array(0).structure(2-1)(3-1)

				EdgeConstraint(7-1)(1-1) = bn_Array(0).structure(3-1)(1-1)
				EdgeConstraint(7-1)(5-1) = bn_Array(0).structure(3-1)(2-1)
				EdgeConstraint(7-1)(7-1) = bn_Array(0).structure(3-1)(3-1)

				EdgeConstraint(2-1)(2-1) = bn_Array(1).structure(1-1)(1-1)
				EdgeConstraint(2-1)(3-1) = bn_Array(1).structure(1-1)(2-1)
				EdgeConstraint(2-1)(4-1) = bn_Array(1).structure(1-1)(3-1)
				EdgeConstraint(2-1)(6-1) = bn_Array(1).structure(1-1)(4-1)
				EdgeConstraint(2-1)(8-1) = bn_Array(1).structure(1-1)(5-1)

				EdgeConstraint(3-1)(2-1) = bn_Array(1).structure(2-1)(1-1)
				EdgeConstraint(3-1)(3-1) = bn_Array(1).structure(2-1)(2-1)
				EdgeConstraint(3-1)(4-1) = bn_Array(1).structure(2-1)(3-1)
				EdgeConstraint(3-1)(6-1) = bn_Array(1).structure(2-1)(4-1)
				EdgeConstraint(3-1)(8-1) = bn_Array(1).structure(2-1)(5-1)

				EdgeConstraint(4-1)(2-1) = bn_Array(1).structure(3-1)(1-1)
				EdgeConstraint(4-1)(3-1) = bn_Array(1).structure(3-1)(2-1)
				EdgeConstraint(4-1)(4-1) = bn_Array(1).structure(3-1)(3-1)
				EdgeConstraint(4-1)(6-1) = bn_Array(1).structure(3-1)(4-1)
				EdgeConstraint(4-1)(8-1) = bn_Array(1).structure(3-1)(5-1)

				EdgeConstraint(6-1)(2-1) = bn_Array(1).structure(4-1)(1-1)
				EdgeConstraint(6-1)(3-1) = bn_Array(1).structure(4-1)(2-1)
				EdgeConstraint(6-1)(4-1) = bn_Array(1).structure(4-1)(3-1)
				EdgeConstraint(6-1)(6-1) = bn_Array(1).structure(4-1)(4-1)
				EdgeConstraint(6-1)(8-1) = bn_Array(1).structure(4-1)(5-1)

				EdgeConstraint(8-1)(2-1) = bn_Array(1).structure(5-1)(1-1)
				EdgeConstraint(8-1)(3-1) = bn_Array(1).structure(5-1)(2-1)
				EdgeConstraint(8-1)(4-1) = bn_Array(1).structure(5-1)(3-1)
				EdgeConstraint(8-1)(6-1) = bn_Array(1).structure(5-1)(4-1)
				EdgeConstraint(8-1)(8-1) = bn_Array(1).structure(5-1)(5-1)

				// 以EdgeConstraint为全图结构，新子图生成全图CPT
				var PD_log:Double = ParameterLearning.EM_ProcessThreshold(bn, 1, reference.c)    //参数估计并计算M_ijk的权重值
				reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER)
				reference.MendedData_Num = reference.MendedData.count()
				Search.copy_structure(bn.structure,EdgeConstraint,bn.NodeNum)
				//创建CPT
				bn.qi_computation()
				bn.create_CPT()
				bn.create_M_ijk()
				//生成全图CPT
				SEM.Maxlikelihood(bn)
				reference.MendedData.unpersist()

				if(incremental_change_mark(0)(1-1) == 1){
					EdgeConstraint(1-1)(2-1) = 0
					EdgeConstraint(1-1)(3-1) = 0
					EdgeConstraint(1-1)(4-1) = 0
					EdgeConstraint(1-1)(6-1) = 0
					EdgeConstraint(1-1)(8-1) = 0

					EdgeConstraint(2-1)(1-1) = 0
					EdgeConstraint(3-1)(1-1) = 0
					EdgeConstraint(4-1)(1-1) = 0
					EdgeConstraint(6-1)(1-1) = 0
					EdgeConstraint(8-1)(1-1) = 0


					EdgeConstraint_violate(1-1)(2-1) = 1
					EdgeConstraint_violate(1-1)(3-1) = 1
					EdgeConstraint_violate(1-1)(4-1) = 1
//					EdgeConstraint_violate(1-1)(6-1) = 1
					EdgeConstraint_violate(1-1)(8-1) = 1

//					EdgeConstraint_violate(2-1)(1-1) = 1
//					EdgeConstraint_violate(3-1)(1-1) = 1
//					EdgeConstraint_violate(4-1)(1-1) = 1
//					EdgeConstraint_violate(6-1)(1-1) = 1
//					EdgeConstraint_violate(8-1)(1-1) = 1
				}
				if(incremental_change_mark(0)(2-1) == 1){
					EdgeConstraint(5-1)(2-1) = 0
					EdgeConstraint(5-1)(3-1) = 0
					EdgeConstraint(5-1)(4-1) = 0
					EdgeConstraint(5-1)(6-1) = 0
					EdgeConstraint(5-1)(8-1) = 0

					EdgeConstraint(2-1)(5-1) = 0
					EdgeConstraint(3-1)(5-1) = 0
					EdgeConstraint(4-1)(5-1) = 0
					EdgeConstraint(6-1)(5-1) = 0
					EdgeConstraint(8-1)(5-1) = 0

//					EdgeConstraint_violate(5-1)(2-1) = 1
//					EdgeConstraint_violate(5-1)(3-1) = 1
//					EdgeConstraint_violate(5-1)(4-1) = 1
					EdgeConstraint_violate(5-1)(6-1) = 1
//					EdgeConstraint_violate(5-1)(8-1) = 1

					EdgeConstraint_violate(2-1)(5-1) = 1
//					EdgeConstraint_violate(3-1)(5-1) = 1
					EdgeConstraint_violate(4-1)(5-1) = 1
					EdgeConstraint_violate(6-1)(5-1) = 1
					EdgeConstraint_violate(8-1)(5-1) = 1
				}
				if(incremental_change_mark(0)(3-1) == 1){
					EdgeConstraint(7-1)(2-1) = 0
					EdgeConstraint(7-1)(3-1) = 0
					EdgeConstraint(7-1)(4-1) = 0
					EdgeConstraint(7-1)(6-1) = 0
					EdgeConstraint(7-1)(8-1) = 0

					EdgeConstraint(2-1)(7-1) = 0
					EdgeConstraint(3-1)(7-1) = 0
					EdgeConstraint(4-1)(7-1) = 0
					EdgeConstraint(6-1)(7-1) = 0
					EdgeConstraint(8-1)(7-1) = 0

					EdgeConstraint_violate(7-1)(2-1) = 1
//					EdgeConstraint_violate(7-1)(3-1) = 1
//					EdgeConstraint_violate(7-1)(4-1) = 1
					EdgeConstraint_violate(7-1)(6-1) = 1
					EdgeConstraint_violate(7-1)(8-1) = 1

					EdgeConstraint_violate(2-1)(7-1) = 1
					EdgeConstraint_violate(3-1)(7-1) = 1
//					EdgeConstraint_violate(4-1)(7-1) = 1
//					EdgeConstraint_violate(6-1)(7-1) = 1
					EdgeConstraint_violate(8-1)(7-1) = 1
				}

				if(incremental_change_mark(1)(1-1) == 1){
					EdgeConstraint(2-1)(1-1) = 0
					EdgeConstraint(2-1)(5-1) = 0
					EdgeConstraint(2-1)(7-1) = 0

					EdgeConstraint(1-1)(2-1) = 0
					EdgeConstraint(5-1)(2-1) = 0
					EdgeConstraint(7-1)(2-1) = 0

//					EdgeConstraint_violate(2-1)(1-1) = 1
					EdgeConstraint_violate(2-1)(5-1) = 1
					EdgeConstraint_violate(2-1)(7-1) = 1

					EdgeConstraint_violate(1-1)(2-1) = 1
//					EdgeConstraint_violate(5-1)(2-1) = 1
					EdgeConstraint_violate(7-1)(2-1) = 1
				}
				if(incremental_change_mark(1)(2-1) == 1){
					EdgeConstraint(3-1)(1-1) = 0
					EdgeConstraint(3-1)(5-1) = 0
					EdgeConstraint(3-1)(7-1) = 0

					EdgeConstraint(1-1)(3-1) = 0
					EdgeConstraint(5-1)(3-1) = 0
					EdgeConstraint(7-1)(3-1) = 0

					EdgeConstraint_violate(3-1)(1-1) = 1
//					EdgeConstraint_violate(3-1)(5-1) = 1
					EdgeConstraint_violate(3-1)(7-1) = 1

					EdgeConstraint_violate(1-1)(3-1) = 1
//					EdgeConstraint_violate(5-1)(3-1) = 1
//					EdgeConstraint_violate(7-1)(3-1) = 1
				}
				if(incremental_change_mark(1)(3-1) == 1){
					EdgeConstraint(4-1)(1-1) = 0
					EdgeConstraint(4-1)(5-1) = 0
					EdgeConstraint(4-1)(7-1) = 0

					EdgeConstraint(1-1)(4-1) = 0
					EdgeConstraint(5-1)(4-1) = 0
					EdgeConstraint(7-1)(4-1) = 0

//					EdgeConstraint_violate(4-1)(1-1) = 1
					EdgeConstraint_violate(4-1)(5-1) = 1
//					EdgeConstraint_violate(4-1)(7-1) = 1

					EdgeConstraint_violate(1-1)(4-1) = 1
//					EdgeConstraint_violate(5-1)(4-1) = 1
//					EdgeConstraint_violate(7-1)(4-1) = 1
				}
				if(incremental_change_mark(1)(4-1) == 1){
					EdgeConstraint(6-1)(1-1) = 0
					EdgeConstraint(6-1)(5-1) = 0
					EdgeConstraint(6-1)(7-1) = 0

					EdgeConstraint(1-1)(6-1) = 0
					EdgeConstraint(5-1)(6-1) = 0
					EdgeConstraint(7-1)(6-1) = 0

//					EdgeConstraint_violate(6-1)(1-1) = 1
					EdgeConstraint_violate(6-1)(5-1) = 1
//					EdgeConstraint_violate(6-1)(7-1) = 1

//					EdgeConstraint_violate(1-1)(6-1) = 1
					EdgeConstraint_violate(5-1)(6-1) = 1
					EdgeConstraint_violate(7-1)(6-1) = 1
				}
				if(incremental_change_mark(1)(5-1) == 1){
					EdgeConstraint(8-1)(1-1) = 0
					EdgeConstraint(8-1)(5-1) = 0
					EdgeConstraint(8-1)(7-1) = 0

					EdgeConstraint(1-1)(8-1) = 0
					EdgeConstraint(5-1)(8-1) = 0
					EdgeConstraint(7-1)(8-1) = 0

//					EdgeConstraint_violate(8-1)(1-1) = 1
					EdgeConstraint_violate(8-1)(5-1) = 1
					EdgeConstraint_violate(8-1)(7-1) = 1

					EdgeConstraint_violate(1-1)(8-1) = 1
//					EdgeConstraint_violate(5-1)(8-1) = 1
					EdgeConstraint_violate(7-1)(8-1) = 1
				}
//				output.structureToConsole(bn)     //输出BN结构
//				output.CPT_FormatToConsole(bn)        //输出BN参数
//				System.exit(0)

			}
			else {
//				边方向约束结构（1代表必须保留的边）
				generate_Initial_Structure_Increnmental(bn, EdgeConstraint, incremental_change_mark)
//				 边方向约束结构（0代表不能生成的边）
				generate_EdgeConstraint_Violate_Increnmental(bn, EdgeConstraint_violate, incremental_change_mark)
			}
		}


		/**************输出中间模型及其参数*****************/
		output.structureToFile(bn, outputFile.initial_structure)     //输出BN结构
		output.CPT_FormatToFile_ijk(bn, outputFile.initial_theta)        //输出BN参数
//		System.exit(0)
		////////////////////////////////////////////////////////////////////////////////////////////////////////////

		outLog = new FileWriter(logFile.structureLearning)                   //以“非”追加写入的方式创建FileWriter对象——结构学习日志写对象
		outLog.write("BNLV结构学习日志\r\n" + "数据集: " + inputFile.primitiveData + " " + reference.SampleNum + "\r\n")
		outLog.write("属性的势: ")
		for (i <- 0 until bn.NodeNum)
		outLog.write(bn.r(i) + ",")
		outLog.write("\r\n")
		outLog.write("隐变量标志: ")
		for (i <- 0 until bn.NodeNum)
			outLog.write(bn.latent(i) + ",")
		outLog.write("\r\n")

//		outLog.write("EM迭代次数上限: " + iteration_EM + "\r\n")
//		outLog.write("EM收敛阈值: " + reference.EM_threshold + "\r\n\r\n")
		outLog.write("SEM迭代次数上限: " + reference.SEM2_iteration + "\r\n")
		outLog.write("SEM收敛阈值: " + reference.SEM_threshold + "\r\n\r\n")
		outLog.write("BIC: " + reference.BIC + "\r\n\r\n")
		outLog.write("BIC_latent: " + reference.BIC_latent + "\r\n\r\n")
		outLog.write("BNLV初始结构：\r\n")
		outLog.close()
		output.structureAppendFile(bn.structure, logFile.structureLearning, bn.NodeNum)

		outLog = new FileWriter(logFile.structureLearning, true)                      //以追加写入的方式创建FileWriter对象
		outLog.write("边方向约束结构（1代表必须保留的边）：\r\n")
		outLog.close()
		// 按文件追加的方式，输出结构约束到文件
		output.structureAppendFile(EdgeConstraint, logFile.structureLearning, bn.NodeNum)

		outLog = new FileWriter(logFile.structureLearning, true)                      //以追加写入的方式创建FileWriter对象
		outLog.write("边方向约束结构（0代表不能生成的边）：\r\n")
		outLog.close()
		// 按文件追加的方式，输出结构约束到文件
		output.structureAppendFile(EdgeConstraint_violate, logFile.structureLearning, bn.NodeNum)

//		System.exit(0)

//		//后处理参数
//		/** 根据M_ijk更新参数theta */
//		var flag0 = 0
//		var k_Theta = new ArrayBuffer[Double]
//		for (i <- 0 until bn.NodeNum){
//			for (j <- 0 until bn.q(i)) {
//				flag0 = 0
//				k_Theta.clear()
//				for (k <- 0 until bn.r(i) )
//					k_Theta += bn.theta(i)(j)(k)
//				val max = k_Theta.max
//				val min = k_Theta.min
//
//				if(min == 0){
//					println("后处理参数 : " + (i+1) + " " + (j+1))
//					if(max == 1){
//						for (t <- 0 until k_Theta.length ) {
//							if(k_Theta(t) == 1){
//								k_Theta(t) = 2.0/ (bn.r(i)+1)
//							}else{
//								k_Theta(t) = 1.0/ (bn.r(i)+1)
//							}
//						}
//					}else{
//						var min_no0 = 1.0
//						var temp0 = 0.0
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
//							temp0 += k_Theta(t)
//						}
//						// 重新计算参数
//						for (t <- 0 until k_Theta.length ) {
//							k_Theta(t) = k_Theta(t) / temp0
//						}
//					}
//					for (k <- 0 until bn.r(i) ) {
//						bn.theta(i)(j)(k) = k_Theta(k)
//					}
//				}
//			}
//		}

		outLog = new FileWriter(outputFile.temporary_theta1)                         //以追加写入的方式创建FileWriter对象
		outLog.write("EM后\r\n")
		outLog.close()
		outLog = new FileWriter(outputFile.temporary_mijk1)                         //以追加写入的方式创建FileWriter对象
		outLog.write("EM后\r\n")
		outLog.close()
		outLog = new FileWriter(outputFile.temporary_theta2)                         //以追加写入的方式创建FileWriter对象
		outLog.write("最后候选模型 EM前\r\n")
		outLog.close()
		outLog = new FileWriter(outputFile.temporary_mijk2)                         //以追加写入的方式创建FileWriter对象
		outLog.write("最后候选模型 EM前\r\n")
		outLog.close()

		/*
		//随机生成多组初始参数(三种方式: Random、WeakConstraint1、WeakConstraint3，选择似然度最大者为最终初始参数
		RandomThetaSelection(bn, M_ijk, ThetaNum, reference.ThetaGeneratedKind, 100)
		output.ObjectToFile(bn, intermediate.initial_BN_object)         //以文件的形式将BN对象存储于intermediate.initial_BN_object中
		println("初始BN对象已保存!")
		System.exit(0)
		*/

//		//读取文件形式存储的BN对象为初始模型
//		val bnFromFile = input.ObjectFromFile(intermediate.initial_BN_object)
//		if (OtherFunction.isEqualGraph(bn.structure, bnFromFile.structure, bn.NodeNum)){  //若bn与bnFromFile的结构相等，则将bnFromFile的CPT赋给bn
//			bnFromFile.CPT_to_Theta_ijk(Theta_ijk)
//			bn.Theta_ijk_to_CPT(Theta_ijk)
//		}
//		else {
//			println("当前BN结构与文件对象的结构不一致")
//			System.exit(0)
//		}

    //output.CPT_FormatToConsole(bn)                             //输出参数内容到控制台

//		outLog = new FileWriter(logFile.structureLearning, true)    //以追加写入的方式创建FileWriter对象
//		outLog.write("初始CPT:\r\n")
//		outLog.close()
//		output.CPT_FormatAppendFile(bn, logFile.structureLearning)

		var outCandidate = new FileWriter(logFile.candidateStructure)                      //以非追加写入的方式创建FileWriter对象
		outCandidate.write("BN各节点的候选模型\r\n")
		outCandidate.close()

		var outBIC_score = new FileWriter(logFile.BIC_score)                      //以非追加写入的方式创建FileWriter对象
		outBIC_score.write("每次结构学习中 EM迭代后的 BIC分数:\r\n")
		outBIC_score.close()

		var outTime_P = new FileWriter(logFile.parameterLearningTime)
		outTime_P.write("EM算法迭代时间 ： " + "\r\n")
		outTime_P.close()

		var outTime_S = new FileWriter(logFile.structureLearningTime)
		outTime_S.write("SEM算法迭代时间 ： " + "\r\n")
		outTime_S.close()
		println("******************结构学习开始 4 *********************")
		//iteration_ClimbHill = 2
		//for (i <- 1 to iteration_ClimbHill) {
		IncrementalLearning.postTheta(bn)

		val t_start_P = new Date()
		////////////////////////////////////////////////////////////////////////////////////////////////////////////
//		var PD_accumulator = SparkConf.sc.accumulator(0.0)
//		val mended_data = reference.Data.map {  //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
//			line =>                         //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
//				val t = line.split(separator.tab)
//				val theta_product = new Array[Double](c) //每行补后数据对应的n个θijk的乘积
//				val m = new Array[Array[String]](c) //存储c行补后数据对应的期望统计量m_ijk——字符串类型，大小为NUM的数组m[0]存储第一行对应的所有m_ijk标记——m[0][0]存储第一行的第一个属性值对应的m_ijk标号
//				for (i <- 0 until c) {
//					m(i) = new Array[String](bn.NodeNum)
//				}
//				/** 考虑每行补后数据t[i]，并计算相应的m_ijk */
//				for (i <- 0 until c) {
//					val tokens = t(i).split(separator.data)
//					val t_int = tokens.map(_.toInt) //存储t[i]中各变量的整数形式,如：t[i]="3 1 2"——t_int[0]=3, t_int[1]=1, t_int[2]=2
//					val theta_temp = new Array[Double](bn.NodeNum) //存储每行补后数据对应的n个θijk的值
//					ijk_computation(t_int, bn, m(i), theta_temp) //计算每行补后样本所对应的期望统计量m_ijk,其中theta_temp (θijk) = EM初始参数
//					theta_product(i) = 1
//					for (j <- 0 until bn.NodeNum) {
//						theta_product(i) *= theta_temp(j)
//					}
//				}
//				/** 计算每行补后数据的权重,共c行 */
//				val weight = new Array[Double](c) //存储每行补后数据的权重weight[i]
//				var temp1 = 0.0
//				for (i <- 0 until c)
//					temp1 += theta_product(i)
//				///////////////////////////P(D)////////////////////////////////
//				PD_accumulator += log(temp1)
//				for (i <- 0 until c){
//					weight(i) = theta_product(i) / temp1  //P(L|D)
//				}
//				var lines = ""
//				for (i <- 0 until c - 1) {
//					lines = lines + t(i) + separator.space + weight(i) //以空格符分隔补后数据与其权重值
//					lines += separator.tab //以制表符分隔每条补后数据
//				}
//				lines += t(c - 1) + separator.space + weight(c - 1)
//				lines
//		}
//		reference.MendedData = mended_data.flatMap(line => line.split(separator.tab)) //补后样本以RDD的形式保存于MendedData中,用于候选模型参数及期望BIC分数的计算
//		reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER)
//		reference.MendedData_Num = reference.MendedData.count()
//		reference.Data.unpersist()
//		var PD_log = PD_accumulator.value
		////////////////////////////////////////////////////////////////////////////////////////////////////////////
		var PD_log: Double = ParameterLearning.EM_ProcessThreshold(bn, reference.EM2_iterationNum , reference.c) //参数估计并计算M_ijk的权重值
		reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER)
		reference.MendedData_Num = reference.MendedData.count()
		reference.Data.unpersist()
//		reference.MendedData.foreach(println(_))
//		System.exit(0)
		////////////////////////////////////////////////////////////////////////////////////////////////////////////
		val t_end_P = new Date()                                      //getTime获取得到的数值单位：毫秒数
		val runtime_P = (t_end_P.getTime - t_start_P.getTime)/1000.0
		P_time += runtime_P
		outTime_P = new FileWriter(logFile.parameterLearningTime, true)
		outTime_P.write("第1次EM 生成破权样本 " + runtime_P + "\r\n")
		outTime_P.close()
		////////////////////////////////////////////////////////////////////////////////////////////////////////////

		if(reference.BIC_type == "BIC" || reference.BIC_type == "BIC+QBIC")
			StructureLearning.old_score_BIC = StructureLearning.BIC_PD(bn, PD_log)
		else if(reference.BIC_type == "QBIC"){
			StructureLearning.Q_BIC(bn)
			StructureLearning.old_score_BIC = bn.Q_BIC.sum
		}

		outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
		outLog.write("reference.MendedData 个数 : " + reference.MendedData_Num + "\r\n")
		outLog.close()

		var flag = 1
		var structureLearning_iteration_count = 1
		val iteration_SEM = reference.SEM2_iteration
		while (flag == 1) {
			/** 创建统计所有样本m_ijk权重之和的M_ijk表 */
			var ThetaNum = 0                                        //期望统计量或参数的总数量
			for (i <- 0 until bn.NodeNum)
				ThetaNum += bn.q(i) * bn.r(i)

			outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
			outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************\r\n")
			outLog.write("CPT参数个数 ThetaNum: " + ThetaNum + "\r\n\r\n")
			//outLog.write("EM前 CPT:\r\n")
			outLog.close()
			//output.CPT_FormatAppendFile(bn, logFile.structureLearning)

			println("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************")



//			/**************输出最优模型及其参数*****************/
//			output.structureToFile(bn, outputFile.optimal_structure)     //输出BN结构到outputFile.optimal_structure中
//			output.CPT_FormatToFile(bn, outputFile.optimal_theta)        //输出BN参数到theta_file中
//			System.exit(0)


			///////////////////////////////////////////////////
			///////////////////////////////////////////////////
			///////////////////////////////////////////////////
			reference.MendedData.unpersist()
			val t_start_P = new Date()
			var PD_log:Double = ParameterLearning.EM_ProcessThreshold(bn, reference.EM2_iterationNum, reference.c)    //参数估计并计算M_ijk的权重值
			val t_end_P = new Date()                                      //getTime获取得到的数值单位：毫秒数
			val runtime_P = (t_end_P.getTime - t_start_P.getTime)/1000.0
			P_time += runtime_P
			val outTime_P = new FileWriter(logFile.parameterLearningTime, true)
			outTime_P.write("第 " + structureLearning_iteration_count + " 次迭代 ： " + runtime_P + "\r\n")
			outTime_P.close()
			println("reference.PD_log: " + reference.PD_log)
			println("PD_log: " + PD_log)
			if(reference.BIC_type == "BIC" || reference.BIC_type == "BIC+QBIC")
				StructureLearning.old_score_BIC = BIC_PD(bn, PD_log)
			else if(reference.BIC_type == "QBIC"){
				StructureLearning.Q_BIC(bn)
				StructureLearning.old_score_BIC = bn.Q_BIC.sum
			}
			reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER)
			reference.MendedData_Num = reference.MendedData.count()
			///////////////////////////////////////////////////
			///////////////////////////////////////////////////
			///////////////////////////////////////////////////

			outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
			outLog.write("当前模型的BIC评分: " + StructureLearning.old_score_BIC + "\r\n")
			outLog.close()

			var outBIC_score = new FileWriter(logFile.BIC_score,true)                      //以非追加写入的方式创建FileWriter对象
			outBIC_score.write("结构学习 第" + structureLearning_iteration_count + "次迭代 EM迭代后 BIC分数：" + StructureLearning.old_score_BIC + "\r\n")
			outBIC_score.close()

			//存放BN学习过程中，每个节点的候选模型
			var outCandidate = new FileWriter(logFile.candidateStructure, true)
			outCandidate.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************\r\n")
			outCandidate.close()


			val t_start_S = new Date()
			flag = StructureLearning.searchProcess_1(bn, EdgeConstraint, EdgeConstraint_violate)
			val t_end_S = new Date()                                      //getTime获取得到的数值单位：毫秒数
			val runtime_S = (t_end_S.getTime - t_start_S.getTime)/1000.0
			S_time += runtime_S
			outLog = new FileWriter(logFile.structureLearning, true)    //以追加写入的方式创建FileWriter对象
			outLog.write("BNLV结构学习时间: " + runtime_S + " 秒\r\n\r\n")
			outLog.close()

			val outTime_S = new FileWriter(logFile.structureLearningTime, true)
			outTime_S.write("第 " + structureLearning_iteration_count + " 次迭代 ： " + runtime_S + "\r\n")
			outTime_S.close()


			if (structureLearning_iteration_count == iteration_SEM){
				println("结构学习迭代次数达到上限 "+ iteration_SEM + " 次，结果EM迭代")
				flag = 0
			}
			structureLearning_iteration_count += 1
		}

		reference.MendedData.unpersist()

		/**************输出最优模型及其参数*****************/
		output.structureToFile(bn, outputFile.optimal_structure)     //输出BN结构到outputFile.optimal_structure中
		output.CPT_FormatToFile(bn, outputFile.optimal_theta)        //输出BN参数到theta_file中

		outTime_P = new FileWriter(logFile.parameterLearningTime, true)
		outTime_P.write(P_time + "\r\n")
		outTime_P.close()

		outTime_S = new FileWriter(logFile.structureLearningTime, true)
		outTime_S.write(S_time + "\r\n")
		outTime_S.close()

		println("结构学习完成!")
	}



	def generate_EdgeConstraint_Violate_noIncrenmental(bn:BayesianNetwork,structure: Array[Array[Int]]) {
		for( i <- 0 to bn.NodeNum-1){
			for( j <- 0 to bn.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else{
					if(bn.l(i) == "L") {
						if(bn.l(j) == "L")
							structure(i)(j) = 1
					}else
						structure(i)(j) = 0
				}
			}
		}
	}

	// 边方向约束结构（1代表必须保留的边）
	def generate_Initial_Structure_Increnmental(bn:BayesianNetwork, structure: Array[Array[Int]], incremental_change_mark:Array[Array[Int]]): Unit = {
		for( i <- 0 to bn.NodeNum-1){
			for( j <- 0 to bn.NodeNum-1){
				if(bn.l(i) == "L" && bn.l(j) == "L") {
					var a = (i-5)/3
					var b = (j-5)/3
					if(incremental_change_mark(a).sum > 0 || incremental_change_mark(b).sum > 0)
						structure(i)(j) = 0
				}
			}
		}
	}

	// 边方向约束结构（0代表不能生成的边）
	def generate_EdgeConstraint_Violate_Increnmental(bn:BayesianNetwork,structure: Array[Array[Int]], incremental_change_mark:Array[Array[Int]]) {
		for( i <- 0 to bn.NodeNum-1){
			for( j <- 0 to bn.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else if(bn.l(i) == "L" && bn.l(j) == "L") {
					var a = (i-5)/3
					var b = (j-5)/3
					if(incremental_change_mark(a).sum > 0 || incremental_change_mark(b).sum > 0)
						structure(i)(j) = 1
				}
			}
		}
	}


}
