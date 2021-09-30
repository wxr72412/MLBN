package BNLV_learning.Learning

import java.io.FileWriter
import java.text.NumberFormat
import java.util.Date

import BNLV_learning.Global._

import scala.math._
import BNLV_learning._
import BNLV_learning.Input.BN_Input
import BNLV_learning.Output.BN_Output
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break

/**
  * Created by Gao on 2016/6/27.
  */
object StructureLearning {


	val nf = NumberFormat.getNumberInstance()
	nf.setMaximumFractionDigits(4)                       //设置输出格式保留小数位数

	//var iteration_ClimbHill = -1                                            //爬山过程的迭代次数
	//var old_score = 0.0
	var old_score_BIC = Double.NegativeInfinity

	/********************从数据中学习带一个隐变量的BN**********************/
	def learningProcess(bn: BayesianNetwork) {

		println("******************结构学习开始 1 20190930*********************")

		var P_time = 0.0
		var S_time = 0.0

		val input = new BN_Input
		val output = new BN_Output

		val iteration_EM = reference.EM_iterationNum                            //EM迭代次数

		val EdgeConstraint = Array.ofDim[Int](bn.NodeNum, bn.NodeNum)            //边方向约束（必须保留的边）
		val EdgeConstraint_violate = Array.ofDim[Int](bn.NodeNum, bn.NodeNum)            //边方向约束（不能生成的边）

		var outLog = new FileWriter(logFile.parameterLearning)    //以“非”追加写入的方式创建FileWriter对象——参数学习日志写对象
		outLog.write("BNLV结构学习时的EM优化次数\r\n\r\n")
		outLog.close()
		println("******************结构学习开始 2 生成初始结构*********************")
		//生成初始结构
		//		generate_Initial_Structure(bn)

		if(reference.R_type == "1")
			generate_Initial_Structure2(bn) // 我的结构约束
		else if(reference.R_type == "n")
			generate_Initial_StructureR(bn) // 我的结构约束
		else if(reference.R_type == "chest_clinic_1L"){ // 1个隐变量
			bn.structure(8-1)(6-1) = 1
//			bn.structure(1-1)(8-1) = 1
		}
		else if(reference.R_type == "chest_clinic_2L"){ // 2个隐变量
//			bn.structure(1-1)(7-1) = 1
			bn.structure(7-1)(5-1) = 1

//			bn.structure(3-1)(8-1) = 1
			bn.structure(8-1)(4-1) = 1
			bn.structure(2-1)(4-1) = 1

		}
		else if(reference.R_type == "chest_clinic_3L"){ // 3个隐变量
			//			bn.structure(1-1)(7-1) = 1
			bn.structure(6-1)(4-1) = 1

			//			bn.structure(3-1)(8-1) = 1
			bn.structure(8-1)(3-1) = 1
			bn.structure(7-1)(3-1) = 1

		}
		else if(reference.R_type == "chest_clinic_2L_1"){ // 2个隐变量
			bn.structure(1-1)(4-1) = 1
			bn.structure(4-1)(3-1) = 1
		}
		else if(reference.R_type == "chest_clinic_2L_2"){ // 2个隐变量
			bn.structure(1-1)(4-1) = 1
			bn.structure(4-1)(2-1) = 1
		}
		else if(reference.R_type == "chest_clinic_2L_3+5_1"){ // 2个隐变量
//			bn.structure(1-1)(3-1) = 1
			bn.structure(3-1)(2-1) = 1
		}
		else if(reference.R_type == "chest_clinic_2L_3+5_2"){ // 2个隐变量
//			bn.structure(2-1)(5-1) = 1
			bn.structure(5-1)(3-1) = 1
			bn.structure(1-1)(3-1) = 1
		}
		else if(reference.R_type == "alarm_4L"){
			bn.structure(35-1)(27-1) = 1
			bn.structure(36-1)(28-1) = 1
			bn.structure(34-1)(31-1) = 1
			bn.structure(37-1)(29-1) = 1
		}
		else if(reference.R_type == "alarm_5L"){
			bn.structure(35-1)(26-1) = 1
			bn.structure(36-1)(27-1) = 1
			bn.structure(33-1)(30-1) = 1
			bn.structure(37-1)(28-1) = 1
		}
		else if(reference.R_type == "alarm_6L") {
			bn.structure(32-1)(25-1) = 1
			bn.structure(35-1)(25-1) = 1
			bn.structure(36-1)(26-1) = 1
			bn.structure(33-1)(29-1) = 1
			bn.structure(37-1)(27-1) = 1
		}


		//从文件中读取表示BN初始结构的邻接矩阵到bn.structure中
//		input.structureFromFile(inputFile.initial_structure, separator.structure, bn)

		// 按文件追加的方式，输出BN的结构到文件
//		output.structureAppendFile(bn, logFile.structureLearning)
		output.structureToConsole(bn)
//		System.exit(0)


		//复制初始结构给约束结构（1代表必须保留的边）
		//		generate_Initial_Structure(bn, EdgeConstraint)
		if(reference.R_type == "1")
			generate_Initial_Structure2(bn, EdgeConstraint)
		else if(reference.R_type == "n")
			generate_Initial_StructureR(bn, EdgeConstraint)
		else if(reference.R_type == "chest_clinic_1L") { // 1个隐变量
			EdgeConstraint(8 - 1)(6 - 1) = 0
		}
		else if(reference.R_type == "chest_clinic_1L"){ // 1个隐变量
			EdgeConstraint(8-1)(6-1) = 1
			//			bn.structure(1-1)(8-1) = 1
		}
		else if(reference.R_type == "chest_clinic_2L"){ // 2个隐变量
			//			bn.structure(1-1)(7-1) = 1
			EdgeConstraint(7-1)(5-1) = 1

			//			bn.structure(3-1)(8-1) = 1
			EdgeConstraint(8-1)(4-1) = 1
			EdgeConstraint(2-1)(4-1) = 1

		}
		else if(reference.R_type == "chest_clinic_3L"){ // 3个隐变量
			//			bn.structure(1-1)(7-1) = 1
			EdgeConstraint(6-1)(4-1) = 1

			//			bn.structure(3-1)(8-1) = 1
			EdgeConstraint(8-1)(3-1) = 1
			EdgeConstraint(7-1)(3-1) = 1

		}
		else{
			Search.copy_structure(EdgeConstraint, bn.structure, bn.NodeNum)
		}
		println("EdgeConstraint:")
		output.structureToConsole_matrix(EdgeConstraint)
		println(reference.R_type)
//		System.exit(0)


		//Search.copy_structure(EdgeConstraint, bn.structure, reference.NodeNum) //令bn为当前模型
		//从文件中读取表示BN结构约束的邻接矩阵到EdgeConstraint中
		//input.structureFromFile(inputFile.EdgeConstraint_structure, separator.structure, EdgeConstraint)

		//生成约束结构（0代表不能生成的边）
		//		generate_EdgeConstraint_Violate(bn,EdgeConstraint_violate)
		if(reference.R_type == "1")
			generate_EdgeConstraint_Violate2(bn,EdgeConstraint_violate)
		else if(reference.R_type == "n")
			generate_EdgeConstraint_ViolateR(bn,EdgeConstraint_violate)
		else if(reference.R_type == "U"){
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					if(bn.l(i) == "U" && bn.l(j) == "U")
						EdgeConstraint_violate(i)(j) = 1
		}
//		else if(reference.R_type == "chest_clinic_1"){
//			EdgeConstraint_violate(1-1)(2-1) = 1
//			EdgeConstraint_violate(1-1)(4-1) = 1
//			EdgeConstraint_violate(4-1)(3-1) = 1
//		}
//		else if(reference.R_type == "chest_clinic_2"){
//			EdgeConstraint_violate(4-1)(1-1) = 1
//			EdgeConstraint_violate(1-1)(2-1) = 1
//			EdgeConstraint_violate(2-1)(3-1) = 1
//		}
		else if(reference.R_type == "chest_clinic_1L") {
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
			EdgeConstraint_violate(7-1)(1-1) = 0
			EdgeConstraint_violate(7-1)(2-1) = 0
			EdgeConstraint_violate(7-1)(3-1) = 0
			EdgeConstraint_violate(7-1)(4-1) = 0
			EdgeConstraint_violate(7-1)(5-1) = 0
			EdgeConstraint_violate(7-1)(8-1) = 0

			EdgeConstraint_violate(6-1)(1-1) = 0
			EdgeConstraint_violate(6-1)(2-1) = 0
			EdgeConstraint_violate(6-1)(3-1) = 0
			EdgeConstraint_violate(6-1)(4-1) = 0
			EdgeConstraint_violate(6-1)(5-1) = 0
			EdgeConstraint_violate(6-1)(8-1) = 0

			EdgeConstraint_violate(2-1)(1-1) = 0
			EdgeConstraint_violate(4-1)(1-1) = 0
			EdgeConstraint_violate(5-1)(1-1) = 0
			EdgeConstraint_violate(6-1)(1-1) = 0
			EdgeConstraint_violate(7-1)(1-1) = 0
			EdgeConstraint_violate(8-1)(1-1) = 0

			EdgeConstraint_violate(2-1)(3-1) = 0
			EdgeConstraint_violate(4-1)(3-1) = 0
			EdgeConstraint_violate(5-1)(3-1) = 0
			EdgeConstraint_violate(6-1)(3-1) = 0
			EdgeConstraint_violate(7-1)(3-1) = 0
			EdgeConstraint_violate(8-1)(3-1) = 0

			EdgeConstraint_violate(5-1)(8-1) = 0
			EdgeConstraint_violate(8-1)(5-1) = 0

			EdgeConstraint_violate(1-1)(7-1) = 0
			EdgeConstraint_violate(1-1)(6-1) = 0

			EdgeConstraint_violate(3-1)(7-1) = 0
			EdgeConstraint_violate(3-1)(6-1) = 0
		}
		else if(reference.R_type == "chest_clinic_2L") {
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
			EdgeConstraint_violate(5-1)(1-1) = 0
			EdgeConstraint_violate(5-1)(2-1) = 0
			EdgeConstraint_violate(5-1)(3-1) = 0
			EdgeConstraint_violate(5-1)(4-1) = 0
			EdgeConstraint_violate(5-1)(7-1) = 0
			EdgeConstraint_violate(5-1)(8-1) = 0

			EdgeConstraint_violate(6-1)(1-1) = 0
			EdgeConstraint_violate(6-1)(2-1) = 0
			EdgeConstraint_violate(6-1)(3-1) = 0
			EdgeConstraint_violate(6-1)(4-1) = 0
			EdgeConstraint_violate(6-1)(7-1) = 0
			EdgeConstraint_violate(6-1)(8-1) = 0

			EdgeConstraint_violate(2-1)(1-1) = 0
			EdgeConstraint_violate(4-1)(1-1) = 0
			EdgeConstraint_violate(5-1)(1-1) = 0
			EdgeConstraint_violate(6-1)(1-1) = 0
			EdgeConstraint_violate(7-1)(1-1) = 0
			EdgeConstraint_violate(8-1)(1-1) = 0

			EdgeConstraint_violate(2-1)(3-1) = 0
			EdgeConstraint_violate(4-1)(3-1) = 0
			EdgeConstraint_violate(5-1)(3-1) = 0
			EdgeConstraint_violate(6-1)(3-1) = 0
			EdgeConstraint_violate(7-1)(3-1) = 0
			EdgeConstraint_violate(8-1)(3-1) = 0

			EdgeConstraint_violate(4-1)(7-1) = 0
			EdgeConstraint_violate(7-1)(4-1) = 0

			EdgeConstraint_violate(1-1)(5-1) = 0
			EdgeConstraint_violate(1-1)(6-1) = 0

			EdgeConstraint_violate(3-1)(5-1) = 0
			EdgeConstraint_violate(3-1)(6-1) = 0
		}
		else if(reference.R_type == "chest_clinic_3L") {
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
			EdgeConstraint_violate(4-1)(1-1) = 0
			EdgeConstraint_violate(4-1)(2-1) = 0
			EdgeConstraint_violate(4-1)(3-1) = 0
			EdgeConstraint_violate(4-1)(6-1) = 0
			EdgeConstraint_violate(4-1)(7-1) = 0
			EdgeConstraint_violate(4-1)(8-1) = 0

			EdgeConstraint_violate(5-1)(1-1) = 0
			EdgeConstraint_violate(5-1)(2-1) = 0
			EdgeConstraint_violate(5-1)(3-1) = 0
			EdgeConstraint_violate(5-1)(6-1) = 0
			EdgeConstraint_violate(5-1)(7-1) = 0
			EdgeConstraint_violate(5-1)(8-1) = 0

			EdgeConstraint_violate(3-1)(1-1) = 0
			EdgeConstraint_violate(4-1)(1-1) = 0
			EdgeConstraint_violate(5-1)(1-1) = 0
			EdgeConstraint_violate(6-1)(1-1) = 0
			EdgeConstraint_violate(7-1)(1-1) = 0
			EdgeConstraint_violate(8-1)(1-1) = 0

			EdgeConstraint_violate(3-1)(2-1) = 0
			EdgeConstraint_violate(4-1)(2-1) = 0
			EdgeConstraint_violate(5-1)(2-1) = 0
			EdgeConstraint_violate(6-1)(2-1) = 0
			EdgeConstraint_violate(7-1)(2-1) = 0
			EdgeConstraint_violate(8-1)(2-1) = 0

			EdgeConstraint_violate(3-1)(6-1) = 0
			EdgeConstraint_violate(6-1)(3-1) = 0

			EdgeConstraint_violate(1-1)(4-1) = 0
			EdgeConstraint_violate(1-1)(5-1) = 0
			EdgeConstraint_violate(2-1)(4-1) = 0
			EdgeConstraint_violate(2-1)(5-1) = 0
		}
		else if(reference.R_type == "chest_clinic_2L_1"){ // 2个隐变量
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
			EdgeConstraint_violate(3-1)(1-1) = 0
			EdgeConstraint_violate(3-1)(2-1) = 0
			EdgeConstraint_violate(3-1)(4-1) = 0
		}
		else if(reference.R_type == "chest_clinic_2L_2"){ // 2个隐变量
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
			EdgeConstraint_violate(3-1)(1-1) = 0
			EdgeConstraint_violate(3-1)(2-1) = 0
			EdgeConstraint_violate(3-1)(4-1) = 0
		}
		else if(reference.R_type == "chest_clinic_2L_3+5_1"){ // 2个隐变量
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
			EdgeConstraint_violate(2-1)(1-1) = 0
			EdgeConstraint_violate(3-1)(1-1) = 0

			EdgeConstraint_violate(2-1)(1-1) = 0
			EdgeConstraint_violate(2-1)(3-1) = 0

			EdgeConstraint_violate(1-1)(2-1) = 0
		}
		else if(reference.R_type == "chest_clinic_2L_3+5_2"){ // 2个隐变量
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
			EdgeConstraint_violate(1-1)(2-1) = 0
			EdgeConstraint_violate(3-1)(2-1) = 0
			EdgeConstraint_violate(4-1)(2-1) = 0
			EdgeConstraint_violate(5-1)(2-1) = 0

			EdgeConstraint_violate(4-1)(1-1) = 0
			EdgeConstraint_violate(4-1)(2-1) = 0
			EdgeConstraint_violate(4-1)(3-1) = 0
			EdgeConstraint_violate(4-1)(5-1) = 0

			EdgeConstraint_violate(2-1)(4-1) = 0
		}

		else{
			for( i <- 0 to bn.NodeNum-1)
				for( j <- 0 to bn.NodeNum-1)
					EdgeConstraint_violate(i)(j) = 1
		}
		println("EdgeConstraint_violate:")
		output.structureToConsole_matrix(EdgeConstraint_violate)
//		System.exit(0)


		println("******************结构学习开始 3 创建CPT*********************")
		//根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
		bn.qi_computation()
		bn.q.foreach(println(_))
		//创建CPT
		bn.create_CPT()
		bn.create_M_ijk()
		println("******************结构学习开始 4  随机生成CPT*********************")
		//bn.RandomTheta()                                           //生成随机初始参数
		//bn.RandomTheta_WeakConstraint1()                             //生成约束一随机初始参数
//		bn.RandomTheta(bn)
		//		bn.RandomTheta_WeakConstraint(bn)
		//		bn.RandomTheta_WeakConstraint1(bn)

		if(reference.R_type == "1")
			bn.RandomTheta_WeakConstraint2(bn)   // 我的参数约束
		else if(reference.R_type == "n")
			bn.RandomTheta_WeakConstraintR(bn)   // 我的参数约束
//		else if(reference.R_type == "chest_clinic_1"){
//			bn.AveregeTheta(bn)
//			bn.theta(3-1)(1-1)(1-1) = 0.6
//			bn.theta(3-1)(1-1)(2-1) = 0.4
//			bn.theta(3-1)(2-1)(1-1) = 0.15
//			bn.theta(3-1)(2-1)(2-1) = 0.85
//			bn.theta(4-1)(1-1)(1-1) = 0.55
//			bn.theta(4-1)(1-1)(2-1) = 0.45
//		}
//		else if(reference.R_type == "chest_clinic_2"){
//			bn.AveregeTheta(bn)
//			bn.theta(1-1)(1-1)(1-1) = 0.99
//			bn.theta(1-1)(1-1)(2-1) = 0.01
//			bn.theta(1-1)(2-1)(1-1) = 0.95
//			bn.theta(1-1)(2-1)(2-1) = 0.05
//			bn.theta(4-1)(1-1)(1-1) = 0.99
//			bn.theta(4-1)(1-1)(2-1) = 0.01
//		}
		else if(reference.R_type == "chest_clinic_1L"){ // 1个隐变量
			bn.RandomTheta(bn)
//			bn.theta(8-1)(1-1)(1-1) = 0.7
//			bn.theta(8-1)(1-1)(2-1) = 0.3
//			bn.theta(8-1)(2-1)(1-1) = 0.4
//			bn.theta(8-1)(2-1)(2-1) = 0.6
			bn.theta(8-1)(1-1)(1-1) = 0.55
			bn.theta(8-1)(1-1)(2-1) = 0.45

			bn.theta(6-1)(1-1)(1-1) = 0.87
			bn.theta(6-1)(1-1)(2-1) = 0.13
			bn.theta(6-1)(2-1)(1-1) = 0.192
			bn.theta(6-1)(2-1)(2-1) = 0.808
		}
		else if(reference.R_type == "chest_clinic_2L"){ // 1个隐变量
			bn.RandomTheta(bn)
			bn.theta(7-1)(1-1)(1-1) = 0.55
			bn.theta(7-1)(1-1)(2-1) = 0.45

//			bn.theta(7-1)(1-1)(1-1) = 0.7
//			bn.theta(7-1)(1-1)(2-1) = 0.3
//			bn.theta(7-1)(2-1)(1-1) = 0.4
//			bn.theta(7-1)(2-1)(2-1) = 0.6

			bn.theta(5-1)(1-1)(1-1) = 0.87
			bn.theta(5-1)(1-1)(2-1) = 0.13
			bn.theta(5-1)(2-1)(1-1) = 0.192
			bn.theta(5-1)(2-1)(2-1) = 0.808

			bn.theta(8-1)(1-1)(1-1) = 0.99
			bn.theta(8-1)(1-1)(2-1) = 0.01

//			bn.theta(8-1)(1-1)(1-1) = 0.99
//			bn.theta(8-1)(1-1)(2-1) = 0.01
//			bn.theta(8-1)(2-1)(1-1) = 0.95
//			bn.theta(8-1)(2-1)(2-1) = 0.05

//			bn.theta(4-1)(1-1)(1-1) = 0.95
//			bn.theta(4-1)(1-1)(2-1) = 0.05
//			bn.theta(4-1)(2-1)(1-1) = 0
//			bn.theta(4-1)(2-1)(2-1) = 1

			bn.theta(4-1)(1-1)(1-1) = 1
			bn.theta(4-1)(1-1)(2-1) = 0
			bn.theta(4-1)(2-1)(1-1) = 0
			bn.theta(4-1)(2-1)(2-1) = 1
			bn.theta(4-1)(3-1)(1-1) = 0
			bn.theta(4-1)(3-1)(2-1) = 1
			bn.theta(4-1)(4-1)(1-1) = 0
			bn.theta(4-1)(4-1)(2-1) = 1
		}
		else if(reference.R_type == "chest_clinic_3L"){ // 1个隐变量
			bn.RandomTheta(bn)
			bn.theta(6-1)(1-1)(1-1) = 0.55
			bn.theta(6-1)(1-1)(2-1) = 0.45

			bn.theta(4-1)(1-1)(1-1) = 0.87
			bn.theta(4-1)(1-1)(2-1) = 0.13
			bn.theta(4-1)(2-1)(1-1) = 0.192
			bn.theta(4-1)(2-1)(2-1) = 0.808

			bn.theta(8-1)(1-1)(1-1) = 0.99
			bn.theta(8-1)(1-1)(2-1) = 0.01

			bn.theta(7-1)(1-1)(1-1) = 0.95
			bn.theta(7-1)(1-1)(2-1) = 0.05

			bn.theta(3-1)(1-1)(1-1) = 1
			bn.theta(3-1)(1-1)(2-1) = 0
			bn.theta(3-1)(2-1)(1-1) = 0
			bn.theta(3-1)(2-1)(2-1) = 1
			bn.theta(3-1)(3-1)(1-1) = 0
			bn.theta(3-1)(3-1)(2-1) = 1
			bn.theta(3-1)(4-1)(1-1) = 0
			bn.theta(3-1)(4-1)(2-1) = 1
		}
		else if(reference.R_type == "chest_clinic_2L_1"){ // 2个隐变量
			bn.RandomTheta(bn)
			bn.theta(4-1)(1-1)(1-1) = 0.7
			bn.theta(4-1)(1-1)(2-1) = 0.3
			bn.theta(4-1)(2-1)(1-1) = 0.4
			bn.theta(4-1)(2-1)(2-1) = 0.6
			bn.theta(3-1)(1-1)(1-1) = 0.87
			bn.theta(3-1)(1-1)(2-1) = 0.13
			bn.theta(3-1)(2-1)(1-1) = 0.192
			bn.theta(3-1)(2-1)(2-1) = 0.808
		}
		else if(reference.R_type == "chest_clinic_2L_2"){ // 2个隐变量
			bn.RandomTheta(bn)
			bn.theta(4-1)(1-1)(1-1) = 0.99
			bn.theta(4-1)(1-1)(2-1) = 0.01
			bn.theta(4-1)(2-1)(1-1) = 0.95
			bn.theta(4-1)(2-1)(2-1) = 0.05
			bn.theta(2-1)(1-1)(1-1) = 0.947
			bn.theta(2-1)(1-1)(2-1) = 0.053
			bn.theta(2-1)(2-1)(1-1) = 0
			bn.theta(2-1)(2-1)(2-1) = 1
		}
		else if(reference.R_type == "chest_clinic_2L_3+5_1"){ // 2个隐变量
			bn.RandomTheta(bn)
//			bn.theta(3-1)(1-1)(1-1) = 0.7
//			bn.theta(3-1)(1-1)(2-1) = 0.3
//			bn.theta(3-1)(2-1)(1-1) = 0.4
//			bn.theta(3-1)(2-1)(2-1) = 0.6
			bn.theta(3-1)(1-1)(1-1) = 0.55
			bn.theta(3-1)(1-1)(2-1) = 0.45

			bn.theta(2-1)(1-1)(1-1) = 0.87
			bn.theta(2-1)(1-1)(2-1) = 0.13
			bn.theta(2-1)(2-1)(1-1) = 0.192
			bn.theta(2-1)(2-1)(2-1) = 0.808
		}
		else if(reference.R_type == "chest_clinic_2L_3+5_2"){ // 2个隐变量
			bn.RandomTheta(bn)
//			bn.theta(5-1)(1-1)(1-1) = 0.99
//			bn.theta(5-1)(1-1)(2-1) = 0.01
//			bn.theta(5-1)(2-1)(1-1) = 0.95
//			bn.theta(5-1)(2-1)(2-1) = 0.05
			bn.theta(5-1)(1-1)(1-1) = 0.99
			bn.theta(5-1)(1-1)(2-1) = 0.01

//			bn.theta(3-1)(1-1)(1-1) = 0.947
//			bn.theta(3-1)(1-1)(2-1) = 0.053
//			bn.theta(3-1)(2-1)(1-1) = 0
//			bn.theta(3-1)(2-1)(2-1) = 1
			bn.theta(3-1)(1-1)(1-1) = 1
			bn.theta(3-1)(1-1)(2-1) = 0
			bn.theta(3-1)(2-1)(1-1) = 0
			bn.theta(3-1)(2-1)(2-1) = 1
			bn.theta(3-1)(3-1)(1-1) = 0
			bn.theta(3-1)(3-1)(2-1) = 1
			bn.theta(3-1)(4-1)(1-1) = 0
			bn.theta(3-1)(4-1)(2-1) = 1
		}
		else
//			bn.AveregeTheta(bn)
			bn.RandomTheta(bn)
		//bn.RandomTheta_WeakConstraint3(bn)                             //生成约束三随机初始参数

//		output.CPT_FormatToConsole(bn)
//		input.CPT_FromFile(inputFile.initial_theta_structureLearning, separator.theta, bn)          //从文件中读取参数θ到动态三维数组bn.theta
//		System.exit(0)
		/**************输出中间模型及其参数*****************/
		output.structureToFile(bn, outputFile.initial_structure)     //输出BN结构
		output.CPT_FormatToFile_ijk(bn, outputFile.initial_theta)        //输出BN参数
//		System.exit(0)
		////////////////////////////////////////////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
		val complete_data = reference.inFile_Data.map { //将每行原始样本映射处理为多条补后样本组成字符串，其中每条补后数据以“空格”为分隔符
			line => //line存储从文件中读取的每行数据，其中每行数据包含多个被分隔符（separator.data）分开的字符串,如1,1
//				println(line)
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
//		println("====================imcomplete_data====================")
//		reference.inFile_Data.foreach(println(_))
//		println("====================complete_data====================")
//		complete_data.foreach(println(_))
//		System.exit(0)
		reference.Data = complete_data
		reference.Data.persist(StorageLevel.MEMORY_AND_DISK_SER)
		reference.SampleNum = reference.Data.count()                         //原始数据文档中的样本数
		if(reference.R_type == "chest_clinic_1" || reference.R_type == "chest_clinic_2"||
				reference.R_type == "chest_clinic_1L"|| reference.R_type == "chest_clinic_2L"|| reference.R_type == "chest_clinic_3L"||
				reference.R_type == "chest_clinic_2L_1"|| reference.R_type == "chest_clinic_2L_2"||
				reference.R_type == "chest_clinic_2L_3+5_1" || reference.R_type == "chest_clinic_2L_3+5_2" ||
				reference.R_type == "alarm_4L" || reference.R_type == "alarm_5L" || reference.R_type == "alarm_6L")
			reference.EM_threshold = reference.EM_threshold_chestclinic
		else
			reference.EM_threshold = reference.EM_threshold_origin * reference.SampleNum
//		reference.SEM_threshold = reference.SEM_threshold_origin * reference.SampleNum
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////
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
		outLog.write("BIC类型: " + reference.BIC_type + "\r\n")
		outLog.write("隐变量组合数: " + reference.c + "\r\n")
		outLog.write("BIC: " + reference.BIC + "\r\n\r\n")
		outLog.write("BIC_latent: " + reference.BIC_latent + "\r\n\r\n")
		outLog.write("EM迭代次数上限: " + iteration_EM + "\r\n")
		outLog.write("EM收敛阈值: " + reference.EM_threshold + "\r\n\r\n")
		outLog.write("SEM迭代次数上限: " + reference.SEM_iteration + "\r\n")
		outLog.write("SEM收敛阈值: " + reference.SEM_threshold + "\r\n\r\n")
		outLog.write("BNLV初始结构：\r\n")
		outLog.close()
		// 按文件追加的方式，输出结构约束到文件
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

//		outLog = new FileWriter(outputFile.mendeddata_theta)                         //以追加写入的方式创建FileWriter对象
//		outLog.write("生成碎全样本：\r\n")
//		outLog.close()
//		outLog = new FileWriter(outputFile.mendeddata_structure)                         //以追加写入的方式创建FileWriter对象
//		outLog.write("生成碎全样本：\r\n")
//		outLog.close()


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
		var flag = 1
		var structureLearning_iteration_count = 1
		val iteration_SEM = reference.SEM_iteration

//		val bn_temp1 = new BayesianNetwork(bn.NodeNum)
//		val bn_temp2 = new BayesianNetwork(bn.NodeNum)
//		Search.copy_bn(bn_temp1, bn)
//		bn_temp1.l.foreach(println(_))
//		Search.copy_bn(bn_temp2, bn)
//		output.CPT_FormatToConsole(bn_temp1)
//		System.exit(0)

		while (flag == 1) {

			/** 创建统计所有样本m_ijk权重之和的M_ijk表 */
			var ThetaNum = 0                                        //期望统计量或参数的总数量
			for (i <- 0 until bn.NodeNum)
				ThetaNum += bn.q(i) * bn.r(i)
//			val M_ijk = new Array[M_table](ThetaNum)                //创建一个M_ijk表存储每个M_ijk的标号及其权重——所有样本的m_ijk权重统计之和
//			for (v <- 0 until ThetaNum)                              //创建数组中每个对象
//				M_ijk(v) =  new M_table
//			//val Theta_ijk = new Array[Double](ThetaNum)             //模型参数顺序表——类似M_ijk表
//			bn.initialize_M_ijk(M_ijk, ThetaNum)                     //初始化M_ijk的标号及值，每个M_ijk的权重置0

			outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
			outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************\r\n")
			outLog.write("CPT参数个数 ThetaNum: " + ThetaNum + "\r\n\r\n")
			//outLog.write("EM前 CPT:\r\n")
			outLog.close()
			//output.CPT_FormatAppendFile(bn, logFile.structureLearning)



			println("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************")
			                              //获取起始时间对象

//			bn.RandomTheta_WeakConstraint222(bn)   // 我的参数约束


//			var PD_log:Double = ParameterLearning.EM_ProcessThreshold1105(bn,bn_temp1, bn_temp2, iteration_EM, reference.c)    //参数估计并计算M_ijk的权重值
			val t_start_P = new Date()
			var PD_log:Double = ParameterLearning.EM_ProcessThreshold(bn, iteration_EM, reference.c)    //参数估计并计算M_ijk的权重值
//			System.exit(0)
			val t_end_P = new Date()                                      //getTime获取得到的数值单位：毫秒数
			val runtime_P = (t_end_P.getTime - t_start_P.getTime)/1000.0
			P_time += runtime_P
			val outTime_P = new FileWriter(logFile.parameterLearningTime, true)
			outTime_P.write("第 " + structureLearning_iteration_count + " 次迭代 ： " + runtime_P + "\r\n")
			outTime_P.close()
			println("reference.PD_log: " + reference.PD_log)
			println("PD_log: " + PD_log)


			/**************输出最优模型及其参数*****************/
//			output.structureToFile(bn, outputFile.optimal_structure)     //输出BN结构到outputFile.optimal_structure中
//			output.CPT_FormatToFile(bn, outputFile.optimal_theta)        //输出BN参数到theta_file中

//			output.structureToFile(bn_temp1, outputFile.mendeddata_structure)     //输出BN结构到outputFile.optimal_structure中
//			output.CPT_FormatToFile(bn_temp1, outputFile.mendeddata_theta)        //输出BN参数到theta_file中


			/**************输出中间模型及其参数*****************/
//						outLog = new FileWriter(outputFile.temporary_theta1, true)                         //以追加写入的方式创建FileWriter对象
//						outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代 EM后*********************\r\n")
//						outLog.close()
//						output.CPT_AppendFile(bn, outputFile.temporary_theta1)        //输出BN参数到theta_file中
//						outLog = new FileWriter(outputFile.temporary_mijk1, true)                         //以追加写入的方式创建FileWriter对象
//						outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代 EM后*********************\r\n")
//						outLog.close()
//						output.Mijk_AppendFile(bn, outputFile.temporary_mijk1)
//			output.CPT_FormatToConsole(bn)
//			System.exit(0)



			//		println("=====================PD_log============================")
			//		println(PD_log)
			//		SEM.Maxlikelihood(reference.MendedData, candidate_model(i), M_ijk, Theta_ijk, ThetaNum)

			/**************输出中间模型及其参数*****************/
//			output.structureToFile(bn, outputFile.temporary_structure1)     //输出BN结构到outputFile.optimal_structure中
//			outLog = new FileWriter(outputFile.temporary_theta1, true)                         //以追加写入的方式创建FileWriter对象
//			outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代 EM后*********************\r\n")
//			outLog.close()
//			output.CPT_AppendFile(bn, outputFile.temporary_theta1)        //输出BN参数到theta_file中
//			outLog = new FileWriter(outputFile.temporary_mijk1, true)                         //以追加写入的方式创建FileWriter对象
//			outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代 EM后*********************\r\n")
//			outLog.close()
//			output.Mijk_AppendFile(bn, outputFile.temporary_mijk1)
//			System.exit(0)

			//bn.CPT_to_Theta_ijk(Theta_ijk)                                          //参数转换为顺序表形式以便计算BIC
			//StructureLearning.old_score = BIC(bn, M_ijk, Theta_ijk, reference.SampleNum)              //初始模型BIC分值
			if(reference.BIC_type == "BIC" || reference.BIC_type == "BIC+QBIC")
				StructureLearning.old_score_BIC = BIC_PD(bn, PD_log)
			else if(reference.BIC_type == "QBIC"){
				StructureLearning_alarm.Q_BIC(bn)
				StructureLearning.old_score_BIC = bn.Q_BIC.sum
			}


//			println("==================当前模型==================")
//			output.CPT_FormatToConsole(bn)
//			output.structureToConsole(bn)
//			println("==================父节点的组合情况数qi==================")
//			for (s <- bn.q)
//				print(s + " ")
//			println()
//			println("=================节点的取值数ri==================")
//			for (s <- bn.r)
//				print(s + " ")
//			println()

			outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
			outLog.write("当前模型的BIC评分: " + StructureLearning.old_score_BIC + "\r\n")
//			outLog.write("EM后 CPT:\r\n")
			outLog.close()
//			output.CPT_FormatAppendFile(bn, logFile.structureLearning)

//			println("=====================BIC_Q============================")
//			println(StructureLearning.old_score)
//			println("=====================当前模型的BIC评分============================")
//			println(StructureLearning.old_score_BIC)


			var outBIC_score = new FileWriter(logFile.BIC_score,true)                      //以非追加写入的方式创建FileWriter对象
			outBIC_score.write("结构学习 第" + structureLearning_iteration_count + "次迭代 EM迭代后 BIC分数：" + StructureLearning.old_score_BIC + "\r\n")
			outBIC_score.close()


//			System.exit(0)

//			//后处理参数
//			/** 根据M_ijk更新参数theta */
//			var flag0 = 0
//			var k_Theta = new ArrayBuffer[Double]
//			for (i <- 0 until bn.NodeNum){
//				for (j <- 0 until bn.q(i)) {
//					flag0 = 0
//					k_Theta.clear()
//					for (k <- 0 until bn.r(i) )
//						k_Theta += bn.theta(i)(j)(k)
//					val max = k_Theta.max
//					val min = k_Theta.min
//
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
//						for (k <- 0 until bn.r(i) ) {
//							bn.theta(i)(j)(k) = k_Theta(k)
//						}
//					}
//				}
//			}


			//存放BN学习过程中，每个节点的候选模型
			var outCandidate = new FileWriter(logFile.candidateStructure, true)
			outCandidate.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************\r\n")
			outCandidate.close()

			                                         //获取起始时间对象
			println("******************结构学习 第" + structureLearning_iteration_count + "次迭代******  reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER) ***************")
			reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER)
			reference.MendedData_Num = reference.MendedData.count()                         //将RDD保存
			outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
			outLog.write("reference.MendedData 个数 : " + reference.MendedData_Num + "\r\n")
			outLog.close()
			//flag = searchProcess_1(intermediate.mendedData, reference.SampleNum, bn, EdgeConstraint, iteration_EM)
			println("******************reference.MendedData.persist(StorageLevel.MEMORY_AND_DISK_SER) 开始结构学习***************")
//			output.CPT_FormatToFile(bn, outputFile.theta_structureLearning)
			val t_start_S = new Date()
			flag = searchProcess_1(bn, EdgeConstraint, EdgeConstraint_violate)
			val t_end_S = new Date()                                      //getTime获取得到的数值单位：毫秒数
			val runtime_S = (t_end_S.getTime - t_start_S.getTime)/1000.0
			S_time += runtime_S
			outLog = new FileWriter(logFile.structureLearning, true)    //以追加写入的方式创建FileWriter对象
			outLog.write("BNLV结构学习时间: " + runtime_S + " 秒\r\n\r\n")
			outLog.close()

			reference.MendedData.unpersist()
			val outTime_S = new FileWriter(logFile.structureLearningTime, true)
			outTime_S.write("第 " + structureLearning_iteration_count + " 次迭代 ： " + runtime_S + "\r\n")
			outTime_S.close()

//			Search.copy_bn(bn_temp1, bn)
//			bn_temp1.l.foreach(println(_))
//			bn_temp1.RandomTheta_WeakConstraint2(bn_temp1)
//			bn_temp1.mendedData_flag = 1

			/**************输出中间模型及其参数*****************/
//			outLog = new FileWriter(outputFile.temporary_theta2, true)                         //以追加写入的方式创建FileWriter对象
//			outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代 EM后*********************\r\n")
//			outLog.close()
//			output.CPT_AppendFile(bn, outputFile.temporary_theta2)        //输出BN参数到theta_file中
//			outLog = new FileWriter(outputFile.temporary_mijk2, true)                         //以追加写入的方式创建FileWriter对象
//			outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代 EM后*********************\r\n")
//			outLog.close()
//			output.Mijk_AppendFile(bn, outputFile.temporary_mijk2)
//
//			outLog = new FileWriter(logFile.structureLearning, true)               //以追加写入的方式创建FileWriter对象
//			outLog.write("第" + structureLearning_iteration_count + "次迭代计算后的最优模型结构:\r\n")
//			outLog.close()
//			output.structureAppendFile(bn, logFile.structureLearning)
////			output.CPT_FormatAppendFile(bn, logFile.structureLearning)



			if (structureLearning_iteration_count == iteration_SEM){
				println("结构学习迭代次数达到上限 "+ iteration_SEM + " 次，结果EM迭代")
				flag = 0
			}
			structureLearning_iteration_count += 1
		}

		reference.Data.unpersist()



		/**************最优模型对象存储于文件中*****************/
		//output.ObjectToFile(bn, outputFile.learned_BN_object)        //以文件的形式将BN对象存储于outputFile.learned_BN_object中

		/**************向控制台输出最优模型及其参数*****************/
		println("==============最优模型==============")
	    output.structureToConsole(bn)
		println("==============参数==============")
	//    output.CPT_FormatToConsole(bn)

		/**************输出最优模型及其参数*****************/
		output.structureToFile(bn, outputFile.optimal_structure)     //输出BN结构到outputFile.optimal_structure中
		output.CPT_FormatToFile(bn, outputFile.optimal_theta)        //输出BN参数到theta_file中

//		output.structureToFile(bn_temp1, outputFile.mendeddata_structure)     //输出BN结构到outputFile.optimal_structure中
//		output.CPT_FormatToFile(bn_temp1, outputFile.mendeddata_theta)        //输出BN参数到theta_file中

		outTime_P = new FileWriter(logFile.parameterLearningTime, true)
		outTime_P.write(P_time + "\r\n")
		outTime_P.close()

		outTime_S = new FileWriter(logFile.structureLearningTime, true)
		outTime_S.write(S_time + "\r\n")
		outTime_S.close()

		println("结构学习完成!")

	}

  /** 对当前节点的一次搜索过程(书中算法版——Algorithm1)
    * SEM中局部最优模型先根据BIC评分与oldScore比较，若大于oldScore，则将其参数使用EM迭代数次，然后作为新的oldScore.
    */
//def searchProcess_1(mendedData: String, sample_num: Long, bn: BayesianNetwork, EdgeConstraint: Array[Array[Int]], iteration_EM: Int):Int = {
def searchProcess_1(bn: BayesianNetwork, EdgeConstraint: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]]):Int = {
	val sample_num = reference.SampleNum
	val output = new BN_Output
	val n = bn.NodeNum

	var candidate_model = new Array[BayesianNetwork](3 * (n - 1)) //每种算子操作最多产生n-1个候选模型,因此每次爬山过程，最多有3*(n-1)个候选模型

	var optimal_model_F = new BayesianNetwork(n) //局部最优模型
	var optimal_model = new BayesianNetwork(n) //局部最优模型

	val current_model = Array.ofDim[Int](n, n) //当前模型——int current_model[n][n]
	Search.copy_structure(current_model, bn.structure, n) //令bn为当前模型


	var optimal_Theta = new ArrayBuffer[Double] //局部最优模型的参数顺序表，因候选模型的ThetaNum一直在变化，所以使用变长数组

//	var outLog_BIC = new FileWriter(outputFile.temporary_BIC) //以追加写入的方式创建FileWriter对象
//	outLog_BIC.write("当前模型的期望BIC评分\r\n\r\n")
//	outLog_BIC.close()
//	Q_BIC(bn)
//	val old_Q_score = bn.Q_BIC.sum
//	println("当前模型的期望BIC评分: " + old_Q_score)
	println("当前模型的BIC评分: " + StructureLearning.old_score_BIC)
	println()
	var temp_Q_score = 0.0
	var new_Q_score = Double.NegativeInfinity //初始分值为负无穷

	for (current_node <- 0 until bn.NodeNum) { //爬山搜索,模型优化当前执行节点current_node

		//current_node由learningProcess控制

		for (i <- 0 until 3 * (n - 1)) //创建数组中每个对象
			candidate_model(i) = new BayesianNetwork(n)

		val candidate = new Candidate //记录候选模型数量
		var existed_candidate_num = 0 //当前候选模型的数量

		//进行三种算子操作,候选模型存储于candidate_model中
		//加边
		Search.add_edge(current_model, EdgeConstraint_violate, n, candidate_model, candidate, current_node)
		var outCandidate = new FileWriter(logFile.candidateStructure, true)
		outCandidate.write("【第" + (current_node + 1) + "个节点的候选模型】\r\n")
		outCandidate.write("加边操作得到的候选模型:\r\n")
		outCandidate.close()
		output.candidateStructureAppendFile(candidate_model, existed_candidate_num, candidate.num, logFile.candidateStructure, current_node, n)
		existed_candidate_num = candidate.num
		//减边
		Search.delete_edge(current_model, EdgeConstraint, n, candidate_model, candidate, current_node)
		outCandidate = new FileWriter(logFile.candidateStructure, true)
		outCandidate.write("减边操作得到的候选模型:\r\n")
		outCandidate.close()
		output.candidateStructureAppendFile(candidate_model, existed_candidate_num, candidate.num, logFile.candidateStructure, current_node, n)
		existed_candidate_num = candidate.num
		//反转边
		Search.reverse_edge(current_model, EdgeConstraint, EdgeConstraint_violate, n, candidate_model, candidate, current_node)
		outCandidate = new FileWriter(logFile.candidateStructure, true)
		outCandidate.write("转边操作得到的候选模型:\r\n")
		outCandidate.close()
		output.candidateStructureAppendFile(candidate_model, existed_candidate_num, candidate.num, logFile.candidateStructure, current_node, n)

		outCandidate = new FileWriter(logFile.candidateStructure, true)
		outCandidate.write("得到的候选模型: " + candidate.num + " 个\r\n")
		outCandidate.close()

		/** ******************计算每个候选模型变量的势r与其父节点的组合情况数q ************************/
		for (v <- 0 until candidate.num) {
			for (i <- 0 until n) {
				candidate_model(v).r(i) = bn.r(i) //设置BN中每个变量的势ri
				candidate_model(v).latent(i) = bn.latent(i)
			}
			candidate_model(v).qi_computation() //根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
		}


		//println("Test*************************:     " + current_node)
		var outLog = new FileWriter(logFile.structureLearning, true) //以追加写入的方式创建FileWriter对象
		outLog.write("【第" + (current_node + 1) + "个节点的候选模型评分计算过程】\r\n")
		outLog.close()
		//对每个候选模型进行期望BIC评分，并选择出最大者
//		var ThetaNum = 0 //期望统计量或参数的总数量
		for (i <- 0 until candidate.num) {
			//FOR 每个对G做一次加边、减边或转边而得到的模型结构G' DO
//			ThetaNum = 0
//			for (j <- 0 until n)
//				ThetaNum += candidate_model(i).q(j) * candidate_model(i).r(j)
//			val M_ijk = new Array[M_table](ThetaNum) //创建一个M_ijk表存储每个M_ijk的标号及其权重——所有样本的m_ijk权重统计之和
//			for (v <- 0 until ThetaNum) //创建数组中每个对象
//				M_ijk(v) = new M_table
//			val Theta_ijk = new Array[Double](ThetaNum) //模型参数顺序表——类似M_ijk表,由SEM_Maxlikelihood计算得出


//			println("========================结构学习 完整数据 及其 P(L|D)==============")
//			reference.MendedData.foreach(println(_))
			//计算当前候选模型的期望BIC评分
//			SEM.Maxlikelihood(candidate_model(i), M_ijk, Theta_ijk, ThetaNum)
//			temp_score = BIC(candidate_model(i), M_ijk, Theta_ijk, sample_num)

			println("============第" + (current_node + 1) + "个节点 第" + (i + 1) + "个候选模型===========")


			if(reference.Family_type == "0"){
				candidate_model(i).create_CPT()
				candidate_model(i).create_M_ijk()
				SEM.Maxlikelihood(candidate_model(i))
			}
			else{
				candidate_model(i).create_CPT_F()
				candidate_model(i).create_M_ijk_F()
				SEM_F.Maxlikelihood_F(candidate_model(i))
			}



////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////			println("--------------------Q_BIC_F----------------")
////			var outLog_BIC = new FileWriter(outputFile.temporary_BIC, true) //以追加写入的方式创建FileWriter对象
////			outLog_BIC.write("第" + (i + 1) + "个候选模型的期望BIC评分: " + "\r\n\r\n")
////			outLog_BIC.close()
//			Q_BIC_F(candidate_model(i))
////			println("--------------------bn.Q_BIC----------------")
////			bn.Q_BIC.foreach(println(_))
////			println("--------------------candidate_model(i).Q_BIC----------------")
////			candidate_model(i).Q_BIC.foreach(println(_))
//			temp_Q_score = old_Q_score
//			for (j <- 0 until candidate_model(i).change_node_num){
//				var num_id = candidate_model(i).change_node(j)
//				println("num_id ： " + num_id + "  temp_Q_score = " + temp_Q_score + " - " + bn.Q_BIC(num_id) + " + " + candidate_model(i).Q_BIC(num_id))
//				temp_Q_score = temp_Q_score - bn.Q_BIC(num_id) + candidate_model(i).Q_BIC(num_id)
//			}
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			var temp_model = new BayesianNetwork(n) //局部最优模型

			if(reference.Family_type == "0"){
				temp_model = candidate_model(i)
			}
			else{
				var temp_model_F = candidate_model(i)
				Search.copy_model(temp_model, temp_model_F)
				for (i <- 0 until n) {
					temp_model.r(i) = bn.r(i) //设置BN中每个变量的势ri
					temp_model.latent(i) = bn.latent(i)
				}
				temp_model.qi_computation() //根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
				temp_model.create_CPT()                                           //optimal_model的CPT结构与原bn的CPT结构不同，因此需要先释放旧的CPT，再创建新的CPT
				temp_model.create_M_ijk()

				//	optimal_model.optimal_Theta_to_CPT(optimal_Theta)
				for (i <- 0 until n) {
					if(temp_model_F.change_node_num == 1 && temp_model_F.change_node(0) == i){
						for (j <- 0 until temp_model.q(i)) {
							for (k <- 0 until temp_model.r(i)) {
								temp_model.theta(i)(j)(k) = temp_model_F.theta(0)(j)(k)
								temp_model.M_ijk(i)(j)(k) = temp_model_F.M_ijk(0)(j)(k)
							}
						}
					}
					else if(temp_model_F.change_node_num == 2 && temp_model_F.change_node(0) == i){
						for (j <- 0 until temp_model.q(i)) {
							for (k <- 0 until temp_model.r(i)) {
								temp_model.theta(i)(j)(k) = temp_model_F.theta(0)(j)(k)
								temp_model.M_ijk(i)(j)(k) = temp_model_F.M_ijk(0)(j)(k)
							}
						}
					}
					else if(temp_model_F.change_node_num == 2 && temp_model_F.change_node(1) == i){
						for (j <- 0 until temp_model.q(i)) {
							for (k <- 0 until temp_model.r(i)) {
								temp_model.theta(i)(j)(k) = temp_model_F.theta(1)(j)(k)
								temp_model.M_ijk(i)(j)(k) = temp_model_F.M_ijk(1)(j)(k)
							}
						}
					}else{
						for (j <- 0 until temp_model.q(i)) {
							for (k <- 0 until temp_model.r(i)) {
								temp_model.theta(i)(j)(k) = bn.theta(i)(j)(k)
								temp_model.M_ijk(i)(j)(k) = bn.M_ijk(i)(j)(k)
							}
						}
					}
				}
			}


			if(reference.BIC_type == "BIC" || reference.LatentNum != 0){
				var PD_log:Double = SEM.PD(temp_model)
				temp_Q_score = BIC_PD(temp_model, PD_log)
			}
			else if(reference.BIC_type == "QBIC"|| reference.BIC_type == "BIC+QBIC"){
				StructureLearning.Q_BIC(temp_model)
				temp_Q_score = temp_model.Q_BIC.sum
			}

//			for(i <- 0 until n) {
//				print(temp_model.Q_BIC(i)+"  ")
//			}
//			println()
//			output.CPT_FormatToConsole(temp_model)
//			temp_model.Q_BIC.foreach(println(_))
//			System.exit(0)

//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			println("第" + (i + 1) + "个候选模型的期望BIC评分: " + temp_Q_score)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//			println("--------------------CPT_F----------------")
//			var temp = 0.0
//			for(ii <- 0 until candidate_model(i).change_node_num){
//				for(j <- 0 until candidate_model(i).q(candidate_model(i).change_node(ii))){
//					temp = 0.0
//					for(k <- 0 until candidate_model(i).r(candidate_model(i).change_node(ii))) {
//						print(candidate_model(i).theta(ii)(j)(k) + " ")
//						temp += candidate_model(i).theta(ii)(j)(k)
//					}
//					print(" = "+temp)
//					println()
//				}
//			}
//			println("--------------------CPT----------------")
//			candidate_model(i).create_CPT()
//			candidate_model(i).Theta_ijk_to_CPT(Theta_ijk)
//			var iii = 0
//			for(ii <- 0 until candidate_model(i).change_node_num){
//				iii = candidate_model(i).change_node(ii)
//				for(j <- 0 until candidate_model(i).q(iii)){
//					temp = 0.0
//					for(k <- 0 until candidate_model(i).r(iii)) {
//						print(candidate_model(i).theta(iii)(j)(k) + " ")
//						temp += candidate_model(i).theta(iii)(j)(k)
//					}
//					print(" = "+temp)
//					println()
//				}
//			}
			println()
//			System.exit(0)
			var outLog = new FileWriter(logFile.structureLearning, true) //以追加写入的方式创建FileWriter对象
			outLog.write("第" + (i + 1) + "个候选模型的期望BIC评分: " + temp_Q_score + "\r\n\r\n")
//			output.CPT_FormatToConsole(temp_model)
			outLog.close()
			println("第" + (current_node+1) + "个节点 第" + (i+1) + "个候选模型 QBIC：" + temp_Q_score+ " 当前最优候选模型 QBIC：" + new_Q_score)
			if (temp_Q_score > new_Q_score) {
//				Search.copy_model(optimal_model, candidate_model(i)) //optimal_model←candidate_model[i]
//				optimal_Theta.clear()
//				optimal_Theta ++= Theta_ijk //optimal_Theta←Theta_ijk
				optimal_model_F = candidate_model(i)
				println("第" + (current_node+1) + "个节点 第" + (i+1) + "个候选模型 QBIC：" + temp_Q_score+ " > 当前最优候选模型 QBIC：" + new_Q_score)
				output.structureToConsole(optimal_model_F)
				new_Q_score = temp_Q_score

			}
		}
	}
//	output.structureToConsole(optimal_model_F)
//	System.exit(0)

	//计算最优候选模型的BIC评分,若new_score > StructureLearning.old_score，则执行EM算法；否则，

	Search.copy_model(optimal_model, optimal_model_F)
	for (i <- 0 until n){
		optimal_model.r(i) = bn.r(i) //设置BN中每个变量的势ri
		optimal_model.l(i) = bn.l(i)
	}

	optimal_model.qi_computation() //根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
	optimal_model.create_CPT()                                           //optimal_model的CPT结构与原bn的CPT结构不同，因此需要先释放旧的CPT，再创建新的CPT
	optimal_model.create_M_ijk()
//	optimal_model.optimal_Theta_to_CPT(optimal_Theta)
	if(reference.Family_type == "0"){
		for (i <- 0 until n) {
			for (j <- 0 until optimal_model.q(i)) {
				for (k <- 0 until optimal_model.r(i)) {
					optimal_model.theta(i)(j)(k) = optimal_model_F.theta(i)(j)(k)

				}
			}
		}
	}
	else{
		for (i <- 0 until n) {
			if(optimal_model_F.change_node_num == 1 && optimal_model_F.change_node(0) == i){
				for (j <- 0 until optimal_model.q(i)) {
					for (k <- 0 until optimal_model.r(i)) {
						optimal_model.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
						optimal_model.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(0)(j)(k)
					}
				}
			}
			else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(0) == i){
				for (j <- 0 until optimal_model.q(i)) {
					for (k <- 0 until optimal_model.r(i)) {
						optimal_model.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
						optimal_model.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(0)(j)(k)
					}
				}
			}
			else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(1) == i){
				for (j <- 0 until optimal_model.q(i)) {
					for (k <- 0 until optimal_model.r(i)) {
						optimal_model.theta(i)(j)(k) = optimal_model_F.theta(1)(j)(k)
						optimal_model.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(1)(j)(k)
					}
				}
			}else{
				for (j <- 0 until optimal_model.q(i)) {
					for (k <- 0 until optimal_model.r(i)) {
						optimal_model.theta(i)(j)(k) = bn.theta(i)(j)(k)
						optimal_model.M_ijk(i)(j)(k) = bn.M_ijk(i)(j)(k)
					}
				}
			}
		}
	}


	//θ←optimal_Theta
//	println("==================最优候选模型==================")
//	output.CPT_FormatToConsole(optimal_model)
//	output.structureToConsole(optimal_model)
//	println("==================父节点的组合情况数qi==================")
//	for (s <- optimal_model.q)
//		print(s + " ")
//	println()
//	println("=================节点的取值数ri==================")
//	for (s <- optimal_model.r)
//		print(s + " ")
//	println()
	var new_score_BIC = 0.0
	if(reference.BIC_type == "BIC"|| reference.BIC_type == "BIC+QBIC" || reference.LatentNum != 0){
		var PD_log:Double = SEM.PD(optimal_model)
		reference.PD_log = PD_log
		new_score_BIC = BIC_PD(optimal_model, PD_log)
	}
	else if(reference.BIC_type == "QBIC"){
		StructureLearning.Q_BIC(optimal_model)
		new_score_BIC = optimal_model.Q_BIC.sum
	}




//	println(PD_log)
//	var ThetaNum = 0                                        //期望统计量或参数的总数量
//	for (i <- 0 until bn.NodeNum)
//		ThetaNum += optimal_model.q(i) * optimal_model.r(i)                         //节点的  势 * 父节点取值组合数
//	val M_ijk = new Array[M_table](ThetaNum)               //创建一个M_ijk表存储每个M_ijk的标号及其权重——所有样本的m_ijk权重统计之和
//	for (v <- 0 until ThetaNum)                            //创建数组中每个对象
//		M_ijk(v) =  new M_table
//	ParameterLearning.EM_ProcessThreshold(optimal_model, M_ijk, ThetaNum, iteration_EM)




	//////////////////////////////////////////////////////////////////////////////////////////////
	val similarity1 = new_score_BIC - StructureLearning.old_score_BIC                   //参数相似度
//	val similarity1 = new_Q_score - old_Q_score             //参数相似度
	/////////////////////////////////////////////////////////////////////////////////////////////////
	println("候选模型与当前模型的BIC评分之差: " + similarity1)


	var outLog = new FileWriter(logFile.structureLearning, true) //以追加写入的方式创建FileWriter对象
	outLog.write("当前候选模型中的最优结构, 期望BIC评分: " + new_Q_score + "\r\n")
	outLog.write("当前候选模型中的最优结构, BIC评分: " + new_score_BIC + "\r\n")
	outLog.write("候选模型与当前模型的BIC评分之差: " + similarity1 + "\r\n")
	outLog.write("当前候选模型中的最优结构: " + "\r\n")
    outLog.close()
    output.structureAppendFile(optimal_model, logFile.structureLearning)

//	println("当前候选模型中的最优结构, 期望BIC评分: " + new_score)
//	println("当前候选模型中的最优结构, BIC评分: " + new_score_BIC)


	//如果新的最优候选模型的BIC评分高，则将bn设置为新的最优候选模型

	if (similarity1 > reference.SEM_threshold) {
    //if (new_score_BIC > StructureLearning.old_score_BIC) {   //参数未迭代优化的局部最优模型BIC评分 > 先前最优模型BIC评分
		StructureLearning.old_score_BIC = new_score_BIC
		Search.copy_model(bn,optimal_model);                      //G←optimal_model
		bn.qi_computation()
//		bn.create_CPT()                                           //optimal_model的CPT结构与原bn的CPT结构不同，因此需要先释放旧的CPT，再创建新的CPT
//		bn.optimal_Theta_to_CPT(optimal_Theta)                    //θ←optimal_Theta
		if(reference.Family_type == "0"){
			for (i <- 0 until n) {
				bn.theta(i) = new Array[Array[Double]](bn.q(i))
				for (j <- 0 until optimal_model.q(i)) {
					bn.theta(i)(j) = new Array[Double](bn.r(i))
					for (k <- 0 until optimal_model.r(i)) {
						bn.theta(i)(j)(k) = optimal_model.theta(i)(j)(k)
					}
				}
			}
		}
		// 更新BN的局部参数
		else{
			for (i <- 0 until n) {
				if(optimal_model_F.change_node_num == 1 && optimal_model_F.change_node(0) == i){
					bn.theta(i) = new Array[Array[Double]](bn.q(i))
					bn.M_ijk(i) = new Array[Array[Double]](bn.q(i))
					for (j <- 0 until bn.q(i)){
						bn.theta(i)(j) = new Array[Double](bn.r(i))     //theta[i][j][k]==θijk
						bn.M_ijk(i)(j) = new Array[Double](bn.r(i))
					}

					for (j <- 0 until bn.q(i)) {
						for (k <- 0 until bn.r(i)) {
							bn.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
							bn.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(0)(j)(k)
						}
					}
				}
				else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(0) == i){
					bn.theta(i) = new Array[Array[Double]](bn.q(i))
					bn.M_ijk(i) = new Array[Array[Double]](bn.q(i))
					for (j <- 0 until bn.q(i)){
						bn.theta(i)(j) = new Array[Double](bn.r(i))     //theta[i][j][k]==θijk
						bn.M_ijk(i)(j) = new Array[Double](bn.r(i))
					}
					for (j <- 0 until bn.q(i)) {
						for (k <- 0 until bn.r(i)) {
							bn.theta(i)(j)(k) = optimal_model_F.theta(0)(j)(k)
							bn.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(0)(j)(k)
						}
					}
				}
				else if(optimal_model_F.change_node_num == 2 && optimal_model_F.change_node(1) == i){
					bn.theta(i) = new Array[Array[Double]](bn.q(i))
					bn.M_ijk(i) = new Array[Array[Double]](bn.q(i))
					for (j <- 0 until bn.q(i)){
						bn.theta(i)(j) = new Array[Double](bn.r(i))     //theta[i][j][k]==θijk
						bn.M_ijk(i)(j) = new Array[Double](bn.r(i))
					}
					for (j <- 0 until bn.q(i)) {
						for (k <- 0 until bn.r(i)) {
							bn.theta(i)(j)(k) = optimal_model_F.theta(1)(j)(k)
							bn.M_ijk(i)(j)(k) = optimal_model_F.M_ijk(1)(j)(k)
						}
					}
				}
			}
		}


//		var PD_log1:Double = SEM.PD(bn)
//		val new_score_BIC1 = BIC_PD(bn, PD_log)
		println("当前候选模型中的最优结构, BIC评分: " + new_score_BIC)

//		println("==================最优候选模型==================")
//		output.CPT_FormatToConsole(bn)
//		output.structureToConsole(bn)

//		println("==================父节点的组合情况数qi==================")
//		for (s <- bn.q)
//			print(s + " ")
//		println()
//		println("=================节点的取值数ri==================")
//		for (s <- bn.r)
//			print(s + " ")
//		println()

		var outBIC_score = new FileWriter(logFile.BIC_score,true)                      //以非追加写入的方式创建FileWriter对象
		outBIC_score.write("EM迭代前 最优候选模型       BIC分数：" + StructureLearning.old_score_BIC + "\r\n")
//		var PD_log1:Double = SEM.PD(bn)
//		val new_score_BIC1 = BIC_PD(bn, PD_log)
//		outBIC_score.write("EM迭代前 最优候选模型       BIC分数：" + new_score_BIC1 + "\r\n")
		outBIC_score.close()

		return 1
      //书中的算法如上，未知——局部最优模型的BIC在与old_score比较前先进行EM迭代优化后的结果（未知策略没有书中策略好）
    }else{
		println("====================================== 结构学习 收敛 ======================================")
		var outLog = new FileWriter(logFile.structureLearning, true) //以追加写入的方式创建FileWriter对象
		outLog.write("====================================== 结构学习 收敛 ==========================================\r\n")
		outLog.close()
		return 0
	}


//    if ((bn.NodeNum - 1) == current_node && 3 == iteration_ClimbHill) {  //最后一次模型选择后，将最优模型的BIC评分输出至logFile.BIC_score
//      val outBIC = new FileWriter(logFile.BIC_score)
//      outBIC.write(old_score + "\r\n")
//      outBIC.close()
//    }
//
//    outLog = new FileWriter(logFile.structureLearning, true)
//    outLog.write("当前最优模型, BIC评分: " + StructureLearning.old_score_BIC + "\r\n")
//    outLog.write("当前最优模型的结构与CPT: \r\n")
//    outLog.close()
//    output.structureAppendFile(bn, logFile.structureLearning)
//    output.CPT_FormatAppendFile(bn, logFile.structureLearning)
//    outLog = new FileWriter(logFile.structureLearning, true)
//    outLog.write("\r\n")
//    outLog.close()
}

	/*********************BIC评分函数****************************/
	def BIC_PD(bn: BayesianNetwork, PD_log: Double): Double = {

		val n = bn.NodeNum
		var model_dimension = 0.0
		var score = 0.0

		for (i <- 0 until n){

			if(bn.latent(i) == 1) { //隐变量
				model_dimension += bn.q(i) * (bn.r(i) - 1) * reference.BIC_latent + model_dimension //独立参数个数
//				println("BIC_PD BIC_latent :" + i + " : "  + bn.q(i) * (bn.r(i) - 1) + " : "  + reference.BIC_latent)
			}else{
				model_dimension += bn.q(i) * (bn.r(i) - 1) * reference.BIC + model_dimension //独立参数个数
//				println("BIC_PD BIC :" +i + " : "  + bn.q(i) * (bn.r(i) - 1) + " : "  + reference.BIC)
			}
//			model_dimension += bn.q(i) * (bn.r(i) - 1)  //独立参数个数

		}
//		System.exit(0)

		val score1 = (model_dimension / 2.0).toDouble * log(reference.SampleNum.toDouble * reference.BIC_data_number)
/////////////////////////////////////////////////////////////////////////////////////////////////
//		score = PD_log  - score1        //BIC分值
/////////////////////////////////////////////////////////////////////////////////////////////////
		score = PD_log  - score1
//		score = PD_log - reference.BIC * score1
/////////////////////////////////////////////////////////////////////////////////////////////////
//		score = PD_log
/////////////////////////////////////////////////////////////////////////////////////////////////
//		println("model_dimension / 2.0 : " + model_dimension / 2.0 + " log(reference.SampleNum): "+log(reference.SampleNum) + " reference.SampleNum: "+reference.SampleNum)
//		println("独立参数个数 : " + model_dimension + " PD_log - score1: "+PD_log + " - "+ score1)
//		println(score + " = " + PD_log + " - " + score1 )
		score
	}
	/*********************BIC评分函数****************************/
//	def BIC(bn: BayesianNetwork, M_ijk: Array[M_table], Theta_ijk: Array[Double], sample_num: Long): Double = {
//
//		val n = bn.NodeNum
//		var model_dimension = 0
//		var likelihood = 0.0
//		var score = 0.0
//		var score1 = 0.0
//		var v = -1
//
//		for ( i <- 0 until n) {
//			likelihood = 0.0
//			for (j <- 0 until bn.q(i))
//				for (k <- 0 until bn.r(i)) {
//					v = bn.hash_ijk(i, j, k)
//					likelihood += M_ijk(v).value * log(Theta_ijk(v)) //log似然度
//				}
//			model_dimension = bn.q(i) * (bn.r(i) - 1)  //独立参数个数
//			score1 = (model_dimension / 2.0).toDouble * log(sample_num.toDouble)
////			println(likelihood - score1 + " " + likelihood + " " +score1)
//			score = score + likelihood - score1
//		}
//
//
//
//		//val score1 = (model_dimension / 2).toDouble * log(sample_num.toDouble)  //高的代码    (5 / 2).toDouble  = 2.0 是不正确的
//		       //BIC分值
//		//		println("独立参数个数 : " + model_dimension)
//		//		println(score + " = " + likelihood + " - " + score1 )
//		score
//	}

	/*********************期望BIC评分函数****************************/
	def Q_BIC(bn: BayesianNetwork) = {

		val n = bn.NodeNum
		var model_dimension = 0.0
		var likelihood = 0.0
		var score = 0.0
		var score1 = 0.0

		var a = 0.0
		var b = 0.0
		//		var outLog_BIC = new FileWriter(outputFile.temporary_BIC, true) //以追加写入的方式创建FileWriter对象

		for(i <- 0 until bn.NodeNum){
			likelihood = 0.0
			score1 = 0.0
			for(j <- 0 until bn.q(i))
				for(k <- 0 until bn.r(i)) {
					likelihood += bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))                                    //log似然度
					//					outLog_BIC.write(bn.M_ijk(i)(j)(k) + "*" + log(bn.theta(i)(j)(k)) + "=" + bn.M_ijk(i)(j)(k) + "*" + "log("+bn.theta(i)(j)(k)+")" + "=" + (bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))) + "\r\n")
				}

			if(bn.latent(i) == 1) { //隐变量
				model_dimension = bn.q(i) * (bn.r(i) - 1) * reference.BIC_latent //独立参数个数
//				println("Q_BIC BIC_latent :" +i + " : "  + bn.q(i) * (bn.r(i) - 1))
			}
			else {
				model_dimension = bn.q(i) * (bn.r(i) - 1) * reference.BIC //独立参数个数
//				println("Q_BIC BIC :" +i + " : "  + bn.q(i) * (bn.r(i) - 1))
			}
			//			model_dimension = bn.q(i) * (bn.r(i) - 1)  //独立参数个数

			score1 = (model_dimension / 2.0).toDouble * log(reference.SampleNum.toDouble * reference.BIC_data_number)

			a += likelihood
			b += score1
			// ///////////////////////////////////////////////////////////////////////////////////////////////
			//			score = likelihood  - score1
			/////////////////////////////////////////////////////////////////////////////////////////////////
			score =  likelihood  - score1
//			score = likelihood  - reference.BIC * score1
			/////////////////////////////////////////////////////////////////////////////////////////////////
			//			score = likelihood
			/////////////////////////////////////////////////////////////////////////////////////////////////
			//
			bn.Q_BIC(i) = score
		}
		bn.likelihood = a
		bn.penalty = b
		println(bn.likelihood + " - " + bn.penalty)
		//		outLog_BIC.close()
	}

//	def Q_BIC_F(bn: BayesianNetwork) = {
//
//		val n = bn.NodeNum
//		var model_dimension = 0
//		var likelihood = 0.0
//		var score = 0.0
//		var score1 = 0.0
//
////		var outLog_BIC = new FileWriter(outputFile.temporary_BIC, true) //以追加写入的方式创建FileWriter对象
//
//		for(i <- 0 until bn.change_node_num){
//			likelihood = 0.0
//			for(j <- 0 until bn.q(bn.change_node(i)))
//				for(k <- 0 until bn.r(bn.change_node(i))) {
//					likelihood += bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))                                    //log似然度
////					outLog_BIC.write(bn.M_ijk(i)(j)(k) + "*" + log(bn.theta(i)(j)(k)) + "=" + bn.M_ijk(i)(j)(k) + "*" + "log("+bn.theta(i)(j)(k)+")" + "=" + (bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))) + "\r\n")
//				}
//			model_dimension = bn.q(bn.change_node(i)) * (bn.r(bn.change_node(i)) - 1)  //独立参数个数
//			score1 = (model_dimension / 2.0).toDouble * log(reference.SampleNum.toDouble)
//			// ///////////////////////////////////////////////////////////////////////////////////////////////
//			//			score = likelihood  - score1
//			/////////////////////////////////////////////////////////////////////////////////////////////////
//			score = likelihood  - reference.BIC * score1
//			/////////////////////////////////////////////////////////////////////////////////////////////////
////			score = likelihood
//			/////////////////////////////////////////////////////////////////////////////////////////////////
////			println(likelihood - score1 + " " + likelihood + " " +score1)
//			bn.Q_BIC(bn.change_node(i)) = score
//		}
//
////		outLog_BIC.close()
//	}


//  /** 随机生成多组初始参数，并计算似然度，选取似然度最大者作为最终初始参数 */
//  def RandomThetaSelection(bn: BayesianNetwork, M_ijk: Array[M_table], ThetaNum: Int, ThetaGeneratedKind: Int, Num: Int) {
//    //ThetaNum——参数总数量，Num——初始参数的组数
//    /*  // 测试使用
//    var outTest = new FileWriter(logFile.testFile)           //以“非”追加写入的方式创建FileWriter对象——测试日志写对象
//    outTest.close()
//    val output = new BN_Output
//    */
//
//    val M_ijk_Set = new Array[Array[M_table]](Num)
//    for (v <- 0 until Num){
//      M_ijk_Set(v) = new Array[M_table](ThetaNum)                //创建一个M_ijk表存储每个M_ijk的标号及其权重——所有样本的m_ijk权重统计之和
//      for (i <- 0 until ThetaNum)                               //创建数组中每个对象
//        M_ijk_Set(v)(i) =  new M_table
//    }
//
//    val ThetaSet = new Array[Array[Double]](Num)             //ThetaSet存储Num组参数
//    for (i <- 0 until Num)
//      ThetaSet(i) = new Array[Double](ThetaNum)
//
//    if ( 0 == ThetaGeneratedKind ) {                    //"Random"方式随机生成初始参数
//      for (i <-0 until Num) {                                //随机生成Num组初始参数
//        bn.RandomTheta()
//        bn.CPT_to_Theta_ijk(ThetaSet(i))
//        ParameterLearning.EM_Process(bn, M_ijk_Set(i), ThetaNum, 1)    //计算每组参数一次迭代后的M_ijk值
//      }
//    }
//    else if ( 1 == ThetaGeneratedKind ) {               //"WeakConstraint3"方式随机生成初始参数
//      for (i <-0 until Num) {
//        bn.RandomTheta_WeakConstraint3()
//        /*
//        outTest = new FileWriter(logFile.testFile, true)
//        outTest.write("第" + (i + 1) + "组参数的CPT:\r\n")
//        outTest.close()
//        output.CPT_FormatAppendFile(bn, logFile.testFile)
//        */
//        bn.CPT_to_Theta_ijk(ThetaSet(i))
//        ParameterLearning.EM_Process(bn, M_ijk_Set(i), ThetaNum, 1)    //计算每组参数一次迭代后的M_ijk值
//      }
//    }
//    else if ( 2 == ThetaGeneratedKind ) {               //"WeakConstraint1"方式随机生成初始参数
//      for (i <-0 until Num) {
//        bn.RandomTheta_WeakConstraint1()
//        bn.CPT_to_Theta_ijk(ThetaSet(i))
//        ParameterLearning.EM_Process(bn, M_ijk_Set(i), ThetaNum, 1)    //计算每组参数一次迭代后的M_ijk值
//      }
//    }
//    else {
//      println("ThetaGeneratedKind 错误!")
//      System.exit(0)                                    //终止程序
//    }
//
//    //outTest = new FileWriter(logFile.testFile, true)
//    val LikelihoodSet = new Array[Double](Num)    //求解每个随机初始参数的似然度，并存储与likelihoodSet中
//    for (v <- 0 until Num){
//      LikelihoodSet(v) = ParameterLearning.Likelihood(bn, M_ijk_Set(v), ThetaSet(v))
//      //outTest.write("初始参数似然度: " + LikelihoodSet(v) + "\r\n")
//    }
//
//    val MaxLikelihood = LikelihoodSet.max  //似然度最大值
//    //outTest.write("\r\n最大参数似然度: " + MaxLikelihood + "\r\n")
//    //outTest.close()
//    val MaxLikelihoodIndex = LikelihoodSet.indexOf(MaxLikelihood)  //求似然度最大者在参数集中的位置
//    bn.Theta_ijk_to_CPT(ThetaSet(MaxLikelihoodIndex))
//  }


	def generate_Initial_Structure(bn:BayesianNetwork): Unit ={
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
					bn.structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U" || bn.l(i) == "R") {  //初始结构约束1、4
						bn.structure(i)(j) = 0
					}else if(bn.l(i) == "L"){       //初始结构约束2、3、4
						if(bn.l(j) == "I" && j == i-reference.LatentNum)
							bn.structure(i)(j) = 1
						else if(bn.l(j) == "R")
//							bn.structure(i)(j) = 0
							bn.structure(i)(j) = 1
						else
							bn.structure(i)(j) = 0
					}else if(bn.l(i) == "I"){       //初始结构约束4
						if(bn.l(j) == "R")
//							bn.structure(i)(j) = 0
							bn.structure(i)(j) = 1
						else
							bn.structure(i)(j) = 0
					}
				}
			}
		}
	}

	def generate_Initial_Structure2(bn:BayesianNetwork): Unit ={
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
					bn.structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U") {
						if(bn.l(j) == "I")
							bn.structure(i)(j) = 0
						else if(bn.l(j) == "R")
							bn.structure(i)(j) = 0
						else if(bn.l(j) == "L")
							bn.structure(i)(j) = 0
					}else if(bn.l(i) == "R"){
						bn.structure(i)(j) = 0
					}else if(bn.l(i) == "L"){       //初始结构约束2、3、4
						if(bn.l(j) == "I" && j == i-reference.LatentNum)
							bn.structure(i)(j) = 1
						else if(bn.l(j) == "R")
							bn.structure(i)(j) = 1
						else
							bn.structure(i)(j) = 0
					}else if(bn.l(i) == "I"){       //初始结构约束4
						if(bn.l(j) == "R")
							bn.structure(i)(j) = 1
						else
							bn.structure(i)(j) = 0
					}
				}
			}
		}
	}

	def generate_Initial_StructureR(bn:BayesianNetwork): Unit ={
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
					bn.structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U") {
						if(bn.l(j) == "I")
							bn.structure(i)(j) = 0
						else if(bn.l(j) == "R")
							bn.structure(i)(j) = 0
						else if(bn.l(j) == "L")
							bn.structure(i)(j) = 0
					}else if(bn.l(i) == "R"){
						bn.structure(i)(j) = 0
					}else if(bn.l(i) == "L"){       //初始结构约束2、3、4
						if(bn.l(j) == "I" && j == i - reference.LatentNum)
							bn.structure(i)(j) = 1
						else if(bn.l(j) == "R" && j == i - 2 * reference.LatentNum)
							bn.structure(i)(j) = 1
						else
							bn.structure(i)(j) = 0
					}else if(bn.l(i) == "I"){       //初始结构约束4
						if(bn.l(j) == "R"  && j == i - reference.LatentNum)
							bn.structure(i)(j) = 1
						else
							bn.structure(i)(j) = 0
					}
				}
			}
		}
	}

	def generate_Initial_Structure(bn:BayesianNetwork,structure: Array[Array[Int]]): Unit ={
		for( i <- 0 to bn.NodeNum-1){
			for( j <- 0 to bn.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
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

	def generate_Initial_Structure2(bn:BayesianNetwork,structure: Array[Array[Int]]): Unit ={
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U") {
						if(bn.l(j) == "I")
							structure(i)(j) = 0
						else if(bn.l(j) == "R")
							structure(i)(j) = 0
						else if(bn.l(j) == "L")
							structure(i)(j) = 0
					}else if(bn.l(i) == "R"){
						structure(i)(j) = 0
					}else if(bn.l(i) == "L"){       //初始结构约束2、3、4
						if(bn.l(j) == "I" && j == i-reference.LatentNum)
							structure(i)(j) = 1
						else if(bn.l(j) == "R")
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

	def generate_Initial_StructureR(bn:BayesianNetwork,structure: Array[Array[Int]]): Unit ={
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U") {
						if(bn.l(j) == "I")
							structure(i)(j) = 0
						else if(bn.l(j) == "R")
							structure(i)(j) = 0
						else if(bn.l(j) == "L")
							structure(i)(j) = 0
					}else if(bn.l(i) == "R"){
						structure(i)(j) = 0
					}else if(bn.l(i) == "L"){       //初始结构约束2、3、4
						if(bn.l(j) == "I" && j == i - reference.LatentNum)
							structure(i)(j) = 1
						else if(bn.l(j) == "R" && j == i - 2 * reference.LatentNum)
							structure(i)(j) = 1
						else
							structure(i)(j) = 0
					}else if(bn.l(i) == "I"){       //初始结构约束4
						if(bn.l(j) == "R" && j == i - reference.LatentNum)
							structure(i)(j) = 1
						else
							structure(i)(j) = 0
					}
				}
			}
		}
	}

	def generate_EdgeConstraint_Violate(bn:BayesianNetwork,structure: Array[Array[Int]]) {
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U") {
						if(bn.l(j) == "I" || bn.l(j) == "R")
							structure(i)(j) = 0
						else
							structure(i)(j) = 1
					}else if(bn.l(i) == "L"){
						if(bn.l(j) == "U")
							structure(i)(j) = 0
						else
							structure(i)(j) = 1
					}else if(bn.l(i) == "I"){
						if(bn.l(j) == "U" || bn.l(j) == "L")
							structure(i)(j) = 0
						else
							structure(i)(j) = 1
					}else if(bn.l(i) == "R"){
						structure(i)(j) = 0
					}
				}
			}
		}
	}

	def generate_EdgeConstraint_Violate2(bn:BayesianNetwork,structure: Array[Array[Int]]) {
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
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
							structure(i)(j) = 0
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

	def generate_EdgeConstraint_ViolateR(bn:BayesianNetwork,structure: Array[Array[Int]]) {
		for( i <- 0 to reference.NodeNum-1){
			for( j <- 0 to reference.NodeNum-1){
				if(i == j) {
					structure(i)(j) = 0
				}else{
					if(bn.l(i) == "U") {
						if(bn.l(j) == "I")
							structure(i)(j) = 1
						else if(bn.l(j) == "R")
							structure(i)(j) = 0
						else
							structure(i)(j) = 1
					}else if(bn.l(i) == "L"){
						if(bn.l(j) == "U")
							structure(i)(j) = 0
						else
							structure(i)(j) = 1
					}else if(bn.l(i) == "I"){
						if(bn.l(j) == "U" || bn.l(j) == "L")
							structure(i)(j) = 0
						else
							structure(i)(j) = 1
					}else if(bn.l(i) == "R"){
						structure(i)(j) = 0
					}
				}
			}
		}
	}

}
