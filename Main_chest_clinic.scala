package BNLV_learning

import java.io.FileWriter
import java.util.Date

import BNLV_learning.Global._
import BNLV_learning.Incremental.IncrementalLearning
import BNLV_learning.Input.BN_Input
import BNLV_learning.Learning.{SEM, StructureLearning, StructureLearning2, StructureLearning_chest_clinic}
import BNLV_learning.Output.BN_Output


object Main_chest_clinic {
	def main(args: Array[String]) {
		val t_start = new Date()   //获取起始时间对象
    	println("BNLV Learning Based on Spark")
		inputFile.primitiveData = "data/chest_clinic/chest-clinic-1000.txt"
//		inputFile.primitiveData = "data/chest_clinic/chest-clinic-1000-origin.txt"
//		inputFile.primitiveData = "data/chest_clinic/chest-clinic-1000.txt"
//		inputFile.primitiveData = "data/chest_clinic/chest-clinic-1.txt"
//		inputFile.primitiveData = "data/chest_clinic/add-1-edge_chest-clinic-5000.txt"
//		inputFile.primitiveData = "data/chest_clinic/delete-1-edge_chest-clinic-5000.txt"
//		inputFile.primitiveData = "data/chest_clinic/add-delete-1-edge_chest-clinic-7500.txt"
//		inputFile.primitiveData = "data/chest_clinic/changeCPT-add-delete-1-edge_chest-clinic-7500.txt"


		reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
		println("数据量: " + reference.inFile.count())
		// System.exit(0)

		reference.NodeNum_Total = 8      //设置BN总变量数
		reference.NodeNum = 8      //设置BN总变量数

//		Smoking = 1
//		Bronchitis = 2
//		LungCancer = 3
//		VisitToAsia = 4
//		TB = 5
//		TBorCancer = 6
//		Dys = 7
//		Xray = 8

		var bn = new BayesianNetwork(reference.NodeNum)
		for (i <- 0 until reference.NodeNum) { //创建数组中每个对象
			bn.r(i) = 2;
		}

        bn.l(1-1) = "C";bn.l(2-1) = "D";bn.l(3-1) = "D";bn.l(4-1) = "C";bn.l(5-1) = "D";bn.l(6-1) = "D";bn.l(7-1) = "S";bn.l(8-1) = "S";


		var operation = 7
		println("当前操作为 ： " + operation)
		operation match {
			case 0 => // 无隐变量
				reference.LatentNum_Total = 0     //设置BN隐变量数
				reference.Data = reference.inFile
				StructureLearning_chest_clinic.learningProcess_no_latent(bn)

			case 10 => // 无隐变量
				reference.NodeNum_Total = 3      //设置BN总变量数
				reference.NodeNum = 3      //设置BN总变量数
				var bn = new BayesianNetwork(reference.NodeNum)
				for (i <- 0 until reference.NodeNum) { //创建数组中每个对象
					bn.r(i) = 2;
				}
				reference.LatentNum_Total = 0     //设置BN隐变量数
				reference.Data = reference.inFile.map {
					line =>
						val data_Array = line.split(separator.data)
						var lines = ""
						lines += data_Array(1-1) + separator.data
						lines += data_Array(2-1) + separator.data
						lines += data_Array(7-1)
						lines
				}
				reference.Data.count()
				StructureLearning_chest_clinic.learningProcess_no_latent(bn)

			case 1 => // 无隐变量 2个子图学习
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 2                       //设置BN隐变量数
				reference.NodeNum_Total = 4              //设置BN变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.NodeNum = 4                          //设置BN节点数
				for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
					bn_Array(i) = new BayesianNetwork(reference.NodeNum)
					bn_Array(i).r(0) = 2;
					bn_Array(i).r(1) = 2;
					bn_Array(i).r(2) = 2;
					bn_Array(i).r(3) = 2;
				}
				for(i <- 0 until reference.LatentNum_Total){
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
					//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
					//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
					//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
					//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
					//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
					//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
					//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					outputFile.initial_structure = "out/Log/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////
					//截取与子图相关的数据
					if(i==0){
						reference.Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(2-1) + separator.data
								lines += data_Array(3-1) + separator.data
								lines += data_Array(7-1)
								lines
						}
					}else if(i==1){
						reference.Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(4-1) + separator.data
								lines += data_Array(5-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(8-1)
								lines
						}
					}
					reference.Data.count()
//					println()
//					System.exit(0)
					// 结构学习
					StructureLearning_chest_clinic.learningProcess_no_latent(bn_Array(i))
				}
				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()


			case 21=> // 1个隐变量
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 1                       //设置BN隐变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.Latent_r = Array(2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.LatentNum = 1                        //设置BN隐变量数
				var bn = new BayesianNetwork(reference.NodeNum)
				for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
					bn_Array(i) = new BayesianNetwork(reference.NodeNum)
					bn_Array(i).r(0) = 2;
					bn_Array(i).r(1) = 2;
					bn_Array(i).r(2) = 2;
					bn_Array(i).r(3) = 2;
					bn_Array(i).r(4) = 2
					bn_Array(i).r(5) = 2
					bn_Array(i).r(6) = 2
					bn_Array(i).r(7) = 2
				}
				for(i <- 0 until reference.LatentNum_Total){
					reference.NodeNum_Total = 8             //设置BN变量数
					reference.NodeNum = 8                   //设置BN节点数
					reference.R_type = "chest_clinic_1L".toString
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
					//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
					//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
					//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
					//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
					//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
					//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
					//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					outputFile.initial_structure = "out/Log/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////
					//截取与子图相关的数据
					if(i==0){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(3-1) + separator.data
								lines += data_Array(4-1) + separator.data
								lines += data_Array(5-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(7-1) + separator.data
								lines += data_Array(8-1)
								lines
						}
					}else{
						println("运行错误！")
					}
					reference.inFile_Data.count()
//					reference.inFile_Data.foreach(println(_))
//					println()
//					println("11111")
//					System.exit(0)
					// 结构学习
					StructureLearning.learningProcess(bn_Array(i))
				}

				reference.BIC = 1.0
				reference.BIC_latent = 1.0
				var PD_log:Double = SEM.PD(bn_Array(0))
				var BIC_score = StructureLearning.BIC_PD(bn_Array(0), PD_log)
				println("最后的BIC评分为：" + BIC_score)

				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()

			case 22=> // 2个隐变量
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 2                       //设置BN隐变量数
				var bn_Array = new Array[BayesianNetwork](1)
				reference.Latent_r = Array(2,2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.LatentNum = 2                        //设置BN隐变量数
				var bn = new BayesianNetwork(reference.NodeNum)
				for (i <- 0 until 1) { //创建数组中每个对象
					bn_Array(i) = new BayesianNetwork(reference.NodeNum)
					bn_Array(i).r(0) = 2;
					bn_Array(i).r(1) = 2;
					bn_Array(i).r(2) = 2;
					bn_Array(i).r(3) = 2;
					bn_Array(i).r(4) = 2
					bn_Array(i).r(5) = 2
					bn_Array(i).r(6) = 2
					bn_Array(i).r(7) = 2
				}
				for(i <- 0 until 1){
					reference.NodeNum_Total = 8             //设置BN变量数
					reference.NodeNum = 8                   //设置BN节点数
					reference.R_type = "chest_clinic_2L".toString
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
					//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
					//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
					//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
					//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
					//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
					//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
					//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					outputFile.initial_structure = "out/Log/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////
					//截取与子图相关的数据
					if(i==0){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(3-1) + separator.data
								lines += data_Array(4-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(7-1) + separator.data
								lines += data_Array(8-1)
								lines
						}
					}else{
						println("运行错误！")
					}
					reference.inFile_Data.count()
					//					reference.inFile_Data.foreach(println(_))
					//					println()
					//					println("11111")
					//					System.exit(0)
					// 结构学习
					StructureLearning.learningProcess(bn_Array(i))
				}

				reference.BIC = 1.0
				reference.BIC_latent = 1.0
				var PD_log:Double = SEM.PD(bn_Array(0))
				var BIC_score = StructureLearning.BIC_PD(bn_Array(0), PD_log)
				println("最后的BIC评分为：" + BIC_score)

				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()

			case 23=> // 3个隐变量
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 3                       //设置BN隐变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.Latent_r = Array(2,2,2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.LatentNum = 3                        //设置BN隐变量数
				var bn = new BayesianNetwork(reference.NodeNum)
				for (i <- 0 until 1) { //创建数组中每个对象
					bn_Array(i) = new BayesianNetwork(reference.NodeNum)
					bn_Array(i).r(0) = 2;
					bn_Array(i).r(1) = 2;
					bn_Array(i).r(2) = 2;
					bn_Array(i).r(3) = 2;
					bn_Array(i).r(4) = 2
					bn_Array(i).r(5) = 2
					bn_Array(i).r(6) = 2
					bn_Array(i).r(7) = 2
				}
				for(i <- 0 until 1){
					reference.NodeNum_Total = 8             //设置BN变量数
					reference.NodeNum = 8                   //设置BN节点数
					reference.R_type = "chest_clinic_3L".toString
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
					//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
					//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
					//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
					//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
					//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
					//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
					//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					outputFile.initial_structure = "out/Log/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////
					//截取与子图相关的数据
					if(i==0){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(4-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(7-1) + separator.data
								lines += data_Array(8-1)
								lines
						}
					}else{
						println("运行错误！")
					}
					reference.inFile_Data.count()
					//					reference.inFile_Data.foreach(println(_))
					//					println()
					//					println("11111")
					//					System.exit(0)
					// 结构学习
					StructureLearning.learningProcess(bn_Array(i))
				}
				reference.BIC = 1.0
				reference.BIC_latent = 1.0
				var PD_log:Double = SEM.PD(bn_Array(0))
				var BIC_score = StructureLearning.BIC_PD(bn_Array(0), PD_log)
				println("最后的BIC评分为：" + BIC_score)

				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()

			case 32 => // 2个隐变量  各子图学习
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 2                       //设置BN隐变量数
				reference.NodeNum_Total = 4              //设置BN变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.Latent_r = Array(2,2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.NodeNum = 4                          //设置BN节点数
				reference.LatentNum = 1                        //设置BN隐变量数
				var bn = new BayesianNetwork(reference.NodeNum)
				for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
					bn_Array(i) = new BayesianNetwork(reference.NodeNum)
					bn_Array(i).r(0) = 2;
					bn_Array(i).r(1) = 2;
					bn_Array(i).r(2) = 2;
					bn_Array(i).r(3) = 2;
				}
				for(i <- 0 until reference.LatentNum_Total){
					reference.R_type = "chest_clinic_2L_" + (i+1).toString
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					outputFile.initial_structure = "out/Log/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////
					//截取与子图相关的数据
					if(i==0){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(3-1) + separator.data
								lines += data_Array(7-1)
								lines
						}
					}else if(i==1){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(4-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(8-1)
								lines
						}
					}
					reference.inFile_Data.count()
//					println()
//					System.exit(0)
					// 结构学习
					StructureLearning.learningProcess(bn_Array(i))
				}
				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()

			case 322 => // 2个隐变量  各子图学习
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 2                       //设置BN隐变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.Latent_r = Array(2,2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.LatentNum = 1                        //设置BN隐变量数
				for(i <- 0 until reference.LatentNum_Total){
					reference.R_type = "chest_clinic_2L_3+5_" + (i+1).toString
					if(i == 0) { //创建数组中每个对象
						reference.NodeNum = 3                          //设置BN节点数
						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2
						bn_Array(i).r(1) = 2
						bn_Array(i).r(2) = 2
						bn_Array(i).latent(3-1) = 1
					}else if(i == 1) { //创建数组中每个对象
						reference.NodeNum = 5                          //设置BN节点数
						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2
						bn_Array(i).r(1) = 2
						bn_Array(i).r(2) = 2
						bn_Array(i).r(3) = 2
						bn_Array(i).r(4) = 2
						bn_Array(i).latent(5-1) = 1
					}

					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
					//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
					//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
					//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
					//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
					//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
					//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
					//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					outputFile.initial_structure = "out/Log/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////
					//截取与子图相关的数据
					if(i==0){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(7-1)
								lines
						}
					}else if(i==1){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(3-1) + separator.data
								lines += data_Array(4-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(8-1)
								lines
						}
					}
					reference.inFile_Data.count()
					//					println()
					//					System.exit(0)
					// 结构学习
					StructureLearning.learningProcess(bn_Array(i))
				}
				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()

			case 42 => // 2个隐变量 子图合并
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 2                       //设置BN隐变量数
				reference.NodeNum_Total = 8              //设置BN变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.Latent_r = Array(2,2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.NodeNum = 4                          //设置BN节点数
				reference.LatentNum = 1                        //设置BN隐变量数
				var bn = new BayesianNetwork(reference.NodeNum)
				for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
					bn_Array(i) = new BayesianNetwork(reference.NodeNum)
					bn_Array(i).r(0) = 2;
					bn_Array(i).r(1) = 2;
					bn_Array(i).r(2) = 2;
					bn_Array(i).r(3) = 2;
				}
				reference.R_type = "chest_clinic_2L_merge"
				for(i <- 0 until reference.LatentNum_Total){
					//修改文件名
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
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
//					val output = new BN_Output
//					output.CPT_FormatToConsole(bn_Array(i))
				}
//				System.exit(0)
				//修改文件名
				var temp_string = ".txt"
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//				outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
//				outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
//				outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
//				outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
//				logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
//				logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
//				logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
//				logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
//				logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
//				logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				outputFile.initial_structure = "out/Log/initial_structure" + temp_string
				outputFile.initial_theta = "out/Log/initial_theta" + temp_string
				outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
				outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
				logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
				logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
				logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
				logFile.BIC_score  = "out/Log/BIC_score" + temp_string
				logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
				logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				var incremental_change_mark = new Array[Array[Int]](reference.LatentNum_Total)
				// 结构合并
				StructureLearning2.learningProcess1(bn, bn_Array, 0, incremental_change_mark)
				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()

			case 422 => // 2个隐变量 子图合并
				reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
				reference.LatentNum_Total = 2                       //设置BN隐变量数
				reference.NodeNum_Total = 8              //设置BN变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.Latent_r = Array(2,2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.NodeNum = 4                          //设置BN节点数
				reference.LatentNum = 1                        //设置BN隐变量数
				for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
					if(i == 0) { //创建数组中每个对象
						reference.NodeNum = 3                          //设置BN节点数
						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2
						bn_Array(i).r(1) = 2
						bn_Array(i).r(2) = 2
						bn_Array(i).latent(3-1) = 1
					}else if(i == 1) { //创建数组中每个对象
						reference.NodeNum = 5                          //设置BN节点数
						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2
						bn_Array(i).r(1) = 2
						bn_Array(i).r(2) = 2
						bn_Array(i).r(3) = 2
						bn_Array(i).r(4) = 2
						bn_Array(i).latent(5-1) = 1
					}
				}
				reference.R_type = "chest_clinic_2L_3+5_merge"
				for(i <- 0 until reference.LatentNum_Total){
					//修改文件名
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
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
					//					val output = new BN_Output
					//					output.CPT_FormatToConsole(bn_Array(i))
				}
				//				System.exit(0)
				//修改文件名
				var temp_string = ".txt"
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				//				outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
				//				outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
				//				outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
				//				outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
				//				logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
				//				logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
				//				logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
				//				logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
				//				logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
				//				logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				outputFile.initial_structure = "out/Log/initial_structure" + temp_string
				outputFile.initial_theta = "out/Log/initial_theta" + temp_string
				outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
				outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
				logFile.candidateStructure  = "out/Log/candidateStructure_file" + temp_string
				logFile.parameterLearning  = "out/Log/parameterLearning_log" + temp_string
				logFile.structureLearning  = "out/Log/structureLeaning_log" + temp_string
				logFile.BIC_score  = "out/Log/BIC_score" + temp_string
				logFile.structureLearningTime  = "out/Log/structureLeaning_time" + temp_string
				logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				var incremental_change_mark = new Array[Array[Int]](reference.LatentNum_Total)
				// 结构合并
				StructureLearning2.learningProcess1(bn, bn_Array, 0, incremental_change_mark)
				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()


            case 7 => // 无隐变量 增量学习
                reference.BIC_latent = 0.1
                reference.LatentNum_Total = 0                       //设置BN隐变量数


                var incremental_change_flag = 0
                var incremental_change_mark = new Array[Int](reference.NodeNum)

                //修改文件名
                var temp_string = ".txt"
                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                //					inputFile.incremental_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
                //					inputFile.incremental_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
                //
                //					outputFile.incremental_optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure" + temp_string
                //					outputFile.incremental_optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta" + temp_string
                //					outputFile.incremental_optimal_structure_S = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure_S" + temp_string
                //					outputFile.incremental_optimal_theta_S = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta_S" + temp_string
                //
                //					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_candidateStructure" + temp_string
                //					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
                //					logFile.incremental_Learning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_log" + temp_string
                //					logFile.incremental_BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_BIC_score" + temp_string
                //					//				logFile.incremental_LearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_LearningTime" + temp_string
                //					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
                //					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
                //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                inputFile.incremental_structure = "out/Log/incremental/optimal_structure" + temp_string
                inputFile.incremental_theta = "out/Log/Incremental/optimal_theta" + temp_string

                outputFile.incremental_optimal_structure = "out/Log/Incremental/incremental_optimal_structure" + temp_string
                outputFile.incremental_optimal_theta = "out/Log/Incremental/incremental_optimal_theta" + temp_string
                outputFile.incremental_optimal_structure_S = "out/Log/Incremental/incremental_optimal_structure_S" + temp_string
                outputFile.incremental_optimal_theta_S = "out/Log/Incremental/incremental_optimal_theta_S" + temp_string

//                logFile.incremental_candidateStructure  = "out/Log/Incremental/incremental_candidateStructure" + temp_string
                logFile.parameterLearning  = "out/Log/Incremental/parameterLearning_log" + temp_string
                logFile.incremental_Learning  = "out/Log/Incremental/incremental_log" + temp_string
                logFile.incremental_BIC_score  = "out/Log/Incremental/incremental_BIC_score" + temp_string
                logFile.incremental_LearningTime  = "out/Log/Incremental/incremental_LearningTime" + temp_string
                logFile.parameterLearningTime = "out/Log/Incremental/parameterLeaning_time" + temp_string
                //////////////////////////////////////////////////////////////////////////////////////////////////////////

                //截取与子图相关的数据
                reference.inFile_Data = reference.inFile.map {
                    line =>
                        val data_Array = line.split(separator.data)
                        var lines = ""
                        lines += data_Array(1-1) + separator.data
                        lines += data_Array(2-1) + separator.data
                        lines += data_Array(3-1) + separator.data
                        lines += data_Array(4-1) + separator.data
                        lines += data_Array(5-1) + separator.data
                        lines += data_Array(6-1) + separator.data
                        lines += data_Array(7-1) + separator.data
                        lines += data_Array(8-1) + separator.data
                        lines
                }

                reference.inFile_Data.count()
                // 增量学习
                incremental_change_mark = new Array[Int](reference.NodeNum)
                incremental_change_mark = IncrementalLearning.learningProcess(bn)
                if(incremental_change_mark.sum > 0)
                    incremental_change_flag = 1
                println("incremental_change_flag : " + incremental_change_flag)
                for(j <- 0 until reference.NodeNum){
                    print(incremental_change_mark(j) + " ")
                }
                println()

                if(incremental_change_flag == 1 && reference.LatentNum_Total > 1){
                    print("无子图需要增量学习，无需合并增量学习！")
                }else if (incremental_change_flag == 0){
                    print("无子图需要增量学习，无需合并增量学习！")
                }else if (reference.LatentNum_Total == 1){
                    print("仅有一个隐变量，无需合并增量学习！")
                }else{
                    print("无隐变量，无需合并增量学习！")
                }
                val t_end = new Date()                                      //getTime获取得到的数值单位：数
                val runtime = (t_end.getTime - t_start.getTime)/1000.0
                val outTime = new FileWriter(logFile.totalTime)
                outTime.write(runtime + "\r\n")
                outTime.close()


			case 8 => // 无隐变量 各子图增量学习
				reference.BIC_latent = 0.1

				reference.LatentNum_Total = 0                       //子图个数
				var bn_Array = new Array[BayesianNetwork](2)
				reference.Latent_r = Array(0,0)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.LatentNum = 0                        //设置BN隐变量数

				var incremental_change_flag = 0
				var incremental_change_mark = new Array[Array[Int]](2)
				for(i <- 0 until 2){
					reference.R_type = "chest_clinic_2L_3+5_" + (i+1).toString
					if(i == 0) { //创建数组中每个对象
						reference.NodeNum = 3                          //设置BN节点数

						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2; bn_Array(i).r(1) = 2; bn_Array(i).r(2) = 2 ;
						bn_Array(i).l(0) = "C"; bn_Array(i).l(1) = "S";bn_Array(i).l(2) = "D"
						bn_Array(i).latent(3-1) = 1
					}else if(i == 1) { //创建数组中每个对象
						reference.NodeNum = 5                          //设置BN节点数
						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2; bn_Array(i).r(1) = 2; bn_Array(i).r(2) = 2; bn_Array(i).r(3) = 2; bn_Array(i).r(4) = 2
						bn_Array(i).l(0) = "D"; bn_Array(i).l(1) = "C"; bn_Array(i).l(2) = "D"; bn_Array(i).l(3) = "S"; bn_Array(i).l(4) = "D";
						bn_Array(i).latent(5-1) = 1
					}
					//修改文件名
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					inputFile.incremental_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					inputFile.incremental_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
					//
					//					outputFile.incremental_optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure" + temp_string
					//					outputFile.incremental_optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta" + temp_string
					//					outputFile.incremental_optimal_structure_S = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure_S" + temp_string
					//					outputFile.incremental_optimal_theta_S = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta_S" + temp_string
					//
					//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_candidateStructure" + temp_string
					//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
					//					logFile.incremental_Learning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_log" + temp_string
					//					logFile.incremental_BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_BIC_score" + temp_string
					//					//				logFile.incremental_LearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_LearningTime" + temp_string
					//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					inputFile.incremental_structure = "out/Log/incremental/optimal_structure" + temp_string
					inputFile.incremental_theta = "out/Log/Incremental/optimal_theta" + temp_string

					outputFile.incremental_optimal_structure = "out/Log/Incremental/incremental_optimal_structure" + temp_string
					outputFile.incremental_optimal_theta = "out/Log/Incremental/incremental_optimal_theta" + temp_string
					outputFile.incremental_optimal_structure_S = "out/Log/Incremental/incremental_optimal_structure_S" + temp_string
					outputFile.incremental_optimal_theta_S = "out/Log/Incremental/incremental_optimal_theta_S" + temp_string

//					logFile.incremental_candidateStructure  = "out/Log/Incremental/incremental_candidateStructure" + temp_string
					logFile.parameterLearning  = "out/Log/Incremental/parameterLearning_log" + temp_string
					logFile.incremental_Learning  = "out/Log/Incremental/incremental_log" + temp_string
					logFile.incremental_BIC_score  = "out/Log/Incremental/incremental_BIC_score" + temp_string
					logFile.incremental_LearningTime  = "out/Log/Incremental/incremental_LearningTime" + temp_string
					logFile.parameterLearningTime = "out/Log/Incremental/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////

					//截取与子图相关的数据
					if(i==0){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(7-1) + separator.data
								lines += data_Array(2-1)
								lines
						}
					}else if(i==1){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(3-1) + separator.data
								lines += data_Array(4-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(8-1) + separator.data
								lines += data_Array(5-1)
								lines
						}
					}
					reference.inFile_Data.count()
//					reference.inFile_Data.foreach(println(_))
//					System.exit(0)
					// 增量学习
					incremental_change_mark(i) = new Array[Int](reference.NodeNum)
					incremental_change_mark(i) = IncrementalLearning.learningProcess(bn_Array(i))
					if(incremental_change_mark(i).sum > 0)
						incremental_change_flag = 1
					println("incremental_change_flag : " + incremental_change_flag)
					for(j <- 0 until reference.NodeNum){
						print(incremental_change_mark(i)(j) + " ")
					}
					println()

				}

				//				System.exit(0)

				if(incremental_change_flag == 1 && 2 > 1){

					reference.BIC_latent = 0.26

					reference.R_type = "chest_clinic_2L_3+5_merge"
					for(i <- 0 until 2){
						//修改文件名
						var temp_string = (i+1).toString + ".txt"
						//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
						//						outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure" + temp_string
						//						outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta" + temp_string
						//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
						outputFile.optimal_structure = "out/Log/Incremental/incremental_optimal_structure" + temp_string
						outputFile.optimal_theta = "out/Log/Incremental/incremental_optimal_theta" + temp_string
						//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
						val input = new BN_Input
						//从文件中读取表示BN初始结构的邻接矩阵到bn.structure中
						input.structureFromFile(outputFile.optimal_structure, separator.structure, bn_Array(i))
						bn_Array(i).qi_computation()
						bn_Array(i).create_CPT()
						//从文件中读取参数θ到动态三维数组bn.theta
						input.CPT_FromFile(outputFile.optimal_theta, separator.theta, bn_Array(i))
					}
					//修改文件名
					var temp_string = ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
					//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
					//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
					//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
					//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
					//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
					//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
					//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
					//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					inputFile.incremental_structure = "out/Log/incremental/optimal_structure" + temp_string
					inputFile.incremental_theta = "out/Log/incremental/optimal_theta" + temp_string

					outputFile.initial_structure = "out/Log/Incremental/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/Incremental/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/Incremental/incremental_optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/Incremental/incremental_optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/Incremental/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/Incremental/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/Incremental/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/Incremental/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/Incremental/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/Incremental/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					// 结构合并
					print("incremental_change_mark : ")
					for(i <- 0 until 2){
						print(incremental_change_mark(i) + " ")
					}
					println()
					//					System.exit(0)
					StructureLearning2.learningProcess1(bn, bn_Array, 1, incremental_change_mark)
				}else if (incremental_change_flag == 0){
					print("无子图需要增量学习，无需合并增量学习！")
				}else if (reference.LatentNum_Total == 1){
					print("仅有一个隐变量，无需合并增量学习！")
				}
				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()

			case 9 => // 各子图增量学习
				reference.BIC_latent = 0.1

				reference.LatentNum_Total = 2                       //设置BN隐变量数
				var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)
				reference.Latent_r = Array(2,2)
				println("隐变量的势：")
				reference.Latent_r.foreach(println(_))
				reference.LatentNum = 1                        //设置BN隐变量数

				var incremental_change_flag = 0
				var incremental_change_mark = new Array[Array[Int]](reference.LatentNum_Total)
				for(i <- 0 until reference.LatentNum_Total){
					reference.R_type = "chest_clinic_2L_3+5_" + (i+1).toString
					if(i == 0) { //创建数组中每个对象
						reference.NodeNum = 3                          //设置BN节点数

						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2; bn_Array(i).r(1) = 2; bn_Array(i).r(2) = 2 ;
						bn_Array(i).l(0) = "C"; bn_Array(i).l(1) = "S";bn_Array(i).l(2) = "D"
						bn_Array(i).latent(3-1) = 1
					}else if(i == 1) { //创建数组中每个对象
						reference.NodeNum = 5                          //设置BN节点数
						bn_Array(i) = new BayesianNetwork(reference.NodeNum)
						bn_Array(i).r(0) = 2; bn_Array(i).r(1) = 2; bn_Array(i).r(2) = 2; bn_Array(i).r(3) = 2; bn_Array(i).r(4) = 2
						bn_Array(i).l(0) = "D"; bn_Array(i).l(1) = "C"; bn_Array(i).l(2) = "D"; bn_Array(i).l(3) = "S"; bn_Array(i).l(4) = "D";
						bn_Array(i).latent(5-1) = 1
					}
					//修改文件名
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//					inputFile.incremental_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
//					inputFile.incremental_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
//
//					outputFile.incremental_optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure" + temp_string
//					outputFile.incremental_optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta" + temp_string
//					outputFile.incremental_optimal_structure_S = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure_S" + temp_string
//					outputFile.incremental_optimal_theta_S = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta_S" + temp_string
//
//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_candidateStructure" + temp_string
//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
//					logFile.incremental_Learning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_log" + temp_string
//					logFile.incremental_BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_BIC_score" + temp_string
//					//				logFile.incremental_LearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_LearningTime" + temp_string
//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					inputFile.incremental_structure = "out/Log/incremental/optimal_structure" + temp_string
					inputFile.incremental_theta = "out/Log/Incremental/optimal_theta" + temp_string

					outputFile.incremental_optimal_structure = "out/Log/Incremental/incremental_optimal_structure" + temp_string
					outputFile.incremental_optimal_theta = "out/Log/Incremental/incremental_optimal_theta" + temp_string
					outputFile.incremental_optimal_structure_S = "out/Log/Incremental/incremental_optimal_structure_S" + temp_string
					outputFile.incremental_optimal_theta_S = "out/Log/Incremental/incremental_optimal_theta_S" + temp_string

//					logFile.incremental_candidateStructure  = "out/Log/Incremental/incremental_candidateStructure" + temp_string
					logFile.parameterLearning  = "out/Log/Incremental/parameterLearning_log" + temp_string
					logFile.incremental_Learning  = "out/Log/Incremental/incremental_log" + temp_string
					logFile.incremental_BIC_score  = "out/Log/Incremental/incremental_BIC_score" + temp_string
					logFile.incremental_LearningTime  = "out/Log/Incremental/incremental_LearningTime" + temp_string
					logFile.parameterLearningTime = "out/Log/Incremental/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////

					//截取与子图相关的数据
					if(i==0){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(1-1) + separator.data
								lines += data_Array(7-1)
								lines
						}
					}else if(i==1){
						reference.inFile_Data = reference.inFile.map {
							line =>
								val data_Array = line.split(separator.data)
								var lines = ""
								lines += data_Array(3-1) + separator.data
								lines += data_Array(4-1) + separator.data
								lines += data_Array(6-1) + separator.data
								lines += data_Array(8-1)
								lines
						}
					}
					reference.inFile_Data.count()
					// 增量学习
					incremental_change_mark(i) = new Array[Int](reference.NodeNum)
					incremental_change_mark(i) = IncrementalLearning.learningProcess(bn_Array(i))
					if(incremental_change_mark(i).sum > 0)
						incremental_change_flag = 1
					println("incremental_change_flag : " + incremental_change_flag)
					for(j <- 0 until reference.NodeNum){
						print(incremental_change_mark(i)(j) + " ")
					}
					println()

				}

//				System.exit(0)

				if(incremental_change_flag == 1 && reference.LatentNum_Total > 1){

					reference.BIC_latent = 0.26

					reference.R_type = "chest_clinic_2L_3+5_merge"
					for(i <- 0 until reference.LatentNum_Total){
						//修改文件名
						var temp_string = (i+1).toString + ".txt"
						//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//						outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_structure" + temp_string
//						outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/Incremental/incremental_optimal_theta" + temp_string
						//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
						outputFile.optimal_structure = "out/Log/Incremental/incremental_optimal_structure" + temp_string
						outputFile.optimal_theta = "out/Log/Incremental/incremental_optimal_theta" + temp_string
						//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
						val input = new BN_Input
						//从文件中读取表示BN初始结构的邻接矩阵到bn.structure中
						input.structureFromFile(outputFile.optimal_structure, separator.structure, bn_Array(i))
						bn_Array(i).qi_computation()
						bn_Array(i).create_CPT()
						//从文件中读取参数θ到动态三维数组bn.theta
						input.CPT_FromFile(outputFile.optimal_theta, separator.theta, bn_Array(i))
					}
					//修改文件名
					var temp_string = ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//					outputFile.initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure" + temp_string
//					outputFile.initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta" + temp_string
//					outputFile.optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure" + temp_string
//					outputFile.optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta" + temp_string
//					logFile.candidateStructure  = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file" + temp_string
//					logFile.parameterLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log" + temp_string
//					logFile.structureLearning  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log" + temp_string
//					logFile.BIC_score  = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score" + temp_string
//					logFile.structureLearningTime  = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time" + temp_string
//					logFile.parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					inputFile.incremental_structure = "out/Log/incremental/optimal_structure" + temp_string
					inputFile.incremental_theta = "out/Log/incremental/optimal_theta" + temp_string

					outputFile.initial_structure = "out/Log/Incremental/initial_structure" + temp_string
					outputFile.initial_theta = "out/Log/Incremental/initial_theta" + temp_string
					outputFile.optimal_structure = "out/Log/Incremental/incremental_optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/Incremental/incremental_optimal_theta" + temp_string
					logFile.candidateStructure  = "out/Log/Incremental/candidateStructure_file" + temp_string
					logFile.parameterLearning  = "out/Log/Incremental/parameterLearning_log" + temp_string
					logFile.structureLearning  = "out/Log/Incremental/structureLeaning_log" + temp_string
					logFile.BIC_score  = "out/Log/Incremental/BIC_score" + temp_string
					logFile.structureLearningTime  = "out/Log/Incremental/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "out/Log/Incremental/parameterLeaning_time" + temp_string
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					// 结构合并
					print("incremental_change_mark : ")
					for(i <- 0 until reference.LatentNum_Total){
						print(incremental_change_mark(i) + " ")
					}
					println()
//					System.exit(0)
					StructureLearning2.learningProcess1(bn, bn_Array, 1, incremental_change_mark)
				}else if (incremental_change_flag == 0){
					print("无子图需要增量学习，无需合并增量学习！")
				}else if (reference.LatentNum_Total == 1){
					print("仅有一个隐变量，无需合并增量学习！")
				}
				val t_end = new Date()                                      //getTime获取得到的数值单位：数
				val runtime = (t_end.getTime - t_start.getTime)/1000.0
				val outTime = new FileWriter(logFile.totalTime)
				outTime.write(runtime + "\r\n")
				outTime.close()


			case 0 =>
				println("程序执行结束!")
				SparkConf.sc.stop()                                    //关闭SparkContext
				return
			case _ =>  println("输入的操作码不对!")
		}

		val t_end = new Date() //getTime获取得到的数值单位：数
		val runtime = (t_end.getTime - t_start.getTime) / 1000.0
		val outTime = new FileWriter(logFile.totalTime)
		outTime.write(runtime + "\r\n")
		outTime.close()
    	SparkConf.sc.stop()  //关闭SparkContext
  	}
}