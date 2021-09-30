package BNLV_learning.Learning

import java.io.FileWriter
import java.text.NumberFormat
import java.util.Date

import BNLV_learning.Global._
import BNLV_learning.Input.BN_Input
import BNLV_learning.Output.BN_Output
import BNLV_learning._
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable.ArrayBuffer

import scala.math._

import BNLV_learning.Learning.StructureLearning_alarm

object StructureLearning_chest_clinic {


	val nf = NumberFormat.getNumberInstance()
	nf.setMaximumFractionDigits(4)                       //设置输出格式保留小数位数
	var old_score_BIC:Double = 0.0

	def learningProcess_no_latent(bn: BayesianNetwork) {

		println("******************结构学习开始 1 *********************")
		var S_time = 0.0
		val input = new BN_Input
		val output = new BN_Output

		var outLog = new FileWriter(logFile.parameterLearning)    //以“非”追加写入的方式创建FileWriter对象——参数学习日志写对象
		outLog.close()


		println("******************结构学习开始 2 生成初始结构*********************")
		val alarm_structure = Array.ofDim[Int](reference.NodeNum_Total, reference.NodeNum_Total)          //邻接矩阵——表示BN的结构
//		output.structureToConsole(bn)
//		generate_Chest_Clinic_Structure(alarm_structure)
//		output.structureToConsole_matrix(alarm_structure)

		//边方向约束结构（1代表必须保留的边）
		val EdgeConstraint = Array.ofDim[Int](reference.NodeNum_Total, reference.NodeNum_Total)          //邻接矩阵——表示BN的结构
		//边方向约束结构（0代表不能生成的边）
		val EdgeConstraint_violate = Array.ofDim[Int](reference.NodeNum_Total, reference.NodeNum_Total)          //邻接矩阵——表示BN的结构

		for( i <- 0 to reference.NodeNum_Total-1){
			for( j <- 0 to reference.NodeNum_Total-1){
				EdgeConstraint(i)(j) = 0

				EdgeConstraint_violate(i)(j) = 1
				if(bn.l(i) == "S" && bn.l(j) != "S")
					EdgeConstraint_violate(i)(j) = 0
				else if(bn.l(i) != "C" && bn.l(j) == "C")
					EdgeConstraint_violate(i)(j) = 0
				else if(bn.l(i) == "C" && bn.l(j) == "S")
					EdgeConstraint_violate(i)(j) = 0
			}
		}

//		generate_Chest_Clinic_Structure(EdgeConstraint_violate)


		println("******************结构学习开始 3 创建CPT*********************")
		//根据模型结构与变量的势计算  每个节点的  父节点组合数  q[]
		bn.qi_computation()
		bn.q.foreach(println(_))
		bn.create_CPT()
		bn.create_M_ijk()

//		output.CPT_FormatToConsole(bn)
//		output.structureToConsole(bn)
//		System.exit(0)
		println("******************结构学习开始 4  随机生成CPT*********************")

		/**************输出中间模型及其参数*****************/
		output.structureToFile(bn, outputFile.initial_structure)     //输出BN结构
		output.CPT_FormatToFile_ijk(bn, outputFile.initial_theta)        //输出BN参数

		reference.Data.persist(StorageLevel.MEMORY_AND_DISK_SER)
		reference.SampleNum = reference.Data.count()                         //原始数据文档中的样本数
		//////////////////////////////////////////////////////////////////////////////////////////////////////////////
		outLog = new FileWriter(logFile.structureLearning)                   //以“非”追加写入的方式创建FileWriter对象——结构学习日志写对象
		outLog.write("结构学习日志\r\n" + "数据集: " + inputFile.primitiveData + " " + reference.SampleNum + "\r\n")
		outLog.write("属性的势: ")
		for (i <- 0 until bn.NodeNum)
		outLog.write(bn.r(i) + ", ")
		outLog.write("\r\n")
		outLog.write("BIC罚项系数: " + reference.BIC + "\r\n")
		outLog.write("SEM迭代次数上限: " + reference.SEM_iteration + "\r\n")
		outLog.write("初始结构：\r\n")
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

		var outCandidate = new FileWriter(logFile.candidateStructure)                      //以非追加写入的方式创建FileWriter对象
		outCandidate.write("BN各节点的候选模型\r\n")
		outCandidate.close()

		var outBIC_score = new FileWriter(logFile.BIC_score)                      //以非追加写入的方式创建FileWriter对象
		outBIC_score.write("每次结构学习中 EM迭代后的 BIC分数:\r\n")
		outBIC_score.close()

		var outTime_S = new FileWriter(logFile.structureLearningTime)
		outTime_S.write("结构学习 时间 ： " + "\r\n")
		outTime_S.close()

		println("******************结构学习开始 4 *********************")
//		output.CPT_FormatToConsole(bn)
		ParameterLearning.learningParameter_no_latent(bn) // 基于完整数据计算参数
		output.CPT_FormatToConsole(bn)

		Q_BIC(bn)
		StructureLearning.old_score_BIC = bn.Q_BIC.sum
//		println(StructureLearning.old_score_BIC)
//		System.exit(0)

		var flag = 1
		var structureLearning_iteration_count = 1
		val iteration_SEM = reference.SEM_iteration

		while (flag == 1) {

			var ThetaNum = 0                                        //期望统计量或参数的总数量
			for (i <- 0 until bn.NodeNum)
				ThetaNum += bn.q(i) * bn.r(i)

			outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
			outLog.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************\r\n")
			outLog.write("CPT参数个数 ThetaNum: " + ThetaNum + "\r\n\r\n")
			outLog.close()

			outLog = new FileWriter(logFile.structureLearning, true)                         //以追加写入的方式创建FileWriter对象
			outLog.write("当前模型的BIC评分: " + StructureLearning.old_score_BIC + "\r\n")
			outLog.close()

			//存放BN学习过程中，每个节点的候选模型
			var outCandidate = new FileWriter(logFile.candidateStructure, true)
			outCandidate.write("******************结构学习 第" + structureLearning_iteration_count + "次迭代*********************\r\n")
			outCandidate.close()

			val t_start_S = new Date()
			flag = searchProcess_no_latent(bn, EdgeConstraint: Array[Array[Int]], EdgeConstraint_violate)
			val t_end_S = new Date()                                      //getTime获取得到的数值单位：毫秒数
			val runtime_S = (t_end_S.getTime - t_start_S.getTime)/1000.0
			S_time += runtime_S
			outLog = new FileWriter(logFile.structureLearning, true)    //以追加写入的方式创建FileWriter对象
			outLog.write("BN结构学习时间: " + runtime_S + " 秒\r\n\r\n")
			outLog.close()

			val outTime_S = new FileWriter(logFile.structureLearningTime, true)
			outTime_S.write("第 " + structureLearning_iteration_count + " 次迭代 ： " + runtime_S + "\r\n")
			outTime_S.close()

			if (structureLearning_iteration_count == iteration_SEM){
				println("结构学习迭代次数达到上限 "+ iteration_SEM + " 次，结束迭代")
				flag = 0
			}
			structureLearning_iteration_count += 1
		}
		reference.Data.unpersist()

		/**************输出最优模型及其参数*****************/
		output.structureToFile(bn, outputFile.optimal_structure)     //输出BN结构到outputFile.optimal_structure中
		output.CPT_FormatToFile(bn, outputFile.optimal_theta)        //输出BN参数到theta_file中

		outTime_S = new FileWriter(logFile.structureLearningTime, true)
		outTime_S.write(S_time + "\r\n")
		outTime_S.close()

		println("结构学习完成!")
	}



  /** 对所有节点的依次搜索过程
    * 局部最优模型先根据家族BIC评分与oldScore比较，若大于oldScore，则作为新的oldScore.
    */
def searchProcess_no_latent(bn: BayesianNetwork, EdgeConstraint: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]]):Int = {
	val output = new BN_Output
	val n = bn.NodeNum

	var candidate_model = new Array[BayesianNetwork](3 * (n - 1)) //每种算子操作最多产生n-1个候选模型,因此每次爬山过程，每个节点最多有3*(n-1)个候选模型

	var optimal_model_F = new BayesianNetwork(n) //局部最优模型
	var optimal_model = new BayesianNetwork(n) //局部最优模型

	val current_model = Array.ofDim[Int](n, n) //当前模型——int current_model[n][n]
	Search.copy_structure(current_model, bn.structure, n) //令bn为当前模型


	var optimal_Theta = new ArrayBuffer[Double] //局部最优模型的参数顺序表，因候选模型的ThetaNum一直在变化，所以使用变长数组

	println("当前模型的BIC评分: " + StructureLearning.old_score_BIC)
	println()
	var temp_Q_score = 0.0
	var new_Q_score = Double.NegativeInfinity //初始分值为负无穷


	for (current_node <- 0 until bn.NodeNum) { //爬山搜索,模型优化当前执行节点current_node
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

//		System.exit(0)
		/** ******************计算每个候选模型变量的势r与其父节点的组合情况数q ************************/
		for (v <- 0 until candidate.num) {
			for (i <- 0 until n)
				candidate_model(v).r(i) = bn.r(i) //设置BN中每个变量的势ri
			candidate_model(v).qi_computation() //根据模型结构与变量的势计算每个节点的父节点组合情况数q[]
		}

		var outLog = new FileWriter(logFile.structureLearning, true) //以追加写入的方式创建FileWriter对象
		outLog.write("【第" + (current_node + 1) + "个节点的候选模型评分计算过程】\r\n")
		outLog.close()

		for (i <- 0 until candidate.num) {

			if(reference.Family_type == "0"){  // 不使用家族BIC评分
				candidate_model(i).create_CPT()
				candidate_model(i).create_M_ijk()
				SEM.Maxlikelihood(candidate_model(i))
			}
			else{  // 使用家族BIC评分
				candidate_model(i).create_CPT_F()
				candidate_model(i).create_M_ijk_F()
				SEM_F.Maxlikelihood_F_no_latent(candidate_model(i))
			}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			var temp_model = new BayesianNetwork(n) //局部最优模型

			if(reference.Family_type == "0"){
				temp_model = candidate_model(i)
			}
			else{
				var temp_model_F = candidate_model(i)
				Search.copy_model(temp_model, temp_model_F)
				for (i <- 0 until n)
					temp_model.r(i) = bn.r(i) //设置BN中每个变量的势ri
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
//				println("===============================================================")
				temp_model.change_node_num = temp_model_F.change_node_num
//				println(temp_model.change_node_num)
				for(i <- 0 until temp_model_F.change_node_num){
					temp_model.change_node(i) = temp_model_F.change_node(i)
//					println(temp_model.change_node(i))
				}
			}

//			output.CPT_FormatToConsole(bn)
//			output.CPT_FormatToConsole(temp_model)
//			output.Mijk_FormatToConsole(temp_model)
//			output.structureToConsole(bn)
//			output.structureToConsole(temp_model)
//			temp_model.q.foreach(println(_))
//			temp_model.r.foreach(println(_))
//			System.exit(0)

//			StructureLearning_alarm.Q_BIC_F(temp_model)
//			temp_Q_score = temp_model.Q_BIC.sum
//			bn.Q_BIC.foreach(println(_))
//			println()
//			temp_model.Q_BIC.foreach(println(_))
//			println()
			StructureLearning_alarm.Q_BIC(temp_model)

			temp_Q_score = temp_model.Q_BIC.sum
			for(i <- 0 until n) {
				print(temp_model.Q_BIC(i)+"  ")
			}
			println()

			output.CPT_FormatToConsole(temp_model)
			temp_model.Q_BIC.foreach(println(_))
//			System.exit(0)
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			println("第" + (current_node + 1) + "个节点   "+"第" + (i + 1) + "个候选模型的期望BIC评分: " + temp_Q_score)
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			println()
//			System.exit(0)
			var outLog = new FileWriter(logFile.structureLearning, true) //以追加写入的方式创建FileWriter对象
			outLog.write("第" + (i + 1) + "个候选模型的期望BIC评分: " + temp_Q_score + "\r\n\r\n")
			output.structureToConsole(temp_model)
			output.Mijk_FormatToConsole(temp_model)
			output.CPT_FormatToConsole(temp_model)
			outLog.close()
			if (temp_Q_score > new_Q_score) {
//				Search.copy_model(optimal_model, candidate_model(i)) //optimal_model←candidate_model[i]
//				optimal_Theta.clear()
//				optimal_Theta ++= Theta_ijk //optimal_Theta←Theta_ijk
				optimal_model_F = candidate_model(i)
				new_Q_score = temp_Q_score
			}
		}
	}
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

	Q_BIC(optimal_model)
	val new_score_BIC = optimal_model.Q_BIC.sum

	output.CPT_FormatToConsole(bn)
	output.CPT_FormatToConsole(optimal_model)
	output.structureToConsole(bn)
	output.structureToConsole(optimal_model)
//	System.exit(0)

	//////////////////////////////////////////////////////////////////////////////////////////////
	val similarity1 = new_score_BIC - StructureLearning.old_score_BIC                   //模型相似度
	/////////////////////////////////////////////////////////////////////////////////////////////////
	println("候选模型与当前模型的BIC评分之差: " + similarity1)


	var outLog = new FileWriter(logFile.structureLearning, true) //以追加写入的方式创建FileWriter对象
	outLog.write("当前候选模型中的最优结构, BIC评分: " + new_score_BIC + "\r\n")
	outLog.write("候选模型与当前模型的BIC评分之差: " + similarity1 + "\r\n")
	outLog.write("当前候选模型中的最优结构: " + "\r\n")
    outLog.close()
    output.structureAppendFile(optimal_model, logFile.structureLearning)

	//如果新的最优候选模型的BIC评分高，则将bn设置为新的最优候选模型
	reference.SEM_threshold = 0
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
		outBIC_score.write("最优候选模型       BIC分数：" + StructureLearning.old_score_BIC + "\r\n")
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
}

	/*********************BIC评分函数****************************/
	def BIC_PD(bn: BayesianNetwork, PD_log: Double): Double = {

		val n = bn.NodeNum
		var model_dimension = 0
		var score = 0.0

		for (i <- 0 until n){
			println(i + " : "  + bn.q(i) * (bn.r(i) - 1))
			model_dimension += bn.q(i) * (bn.r(i) - 1)  //独立参数个数
		}

		val score1 = (model_dimension / 2.0).toDouble * log(reference.SampleNum)
/////////////////////////////////////////////////////////////////////////////////////////////////
//		score = PD_log  - score1        //BIC分值
/////////////////////////////////////////////////////////////////////////////////////////////////
		score = PD_log - reference.BIC * score1
/////////////////////////////////////////////////////////////////////////////////////////////////
//		score = PD_log
/////////////////////////////////////////////////////////////////////////////////////////////////
//		println("model_dimension / 2.0 : " + model_dimension / 2.0 + " log(reference.SampleNum): "+log(reference.SampleNum) + " reference.SampleNum: "+reference.SampleNum)
		println("独立参数个数 : " + model_dimension + " PD_log - score1: "+PD_log + " - "+ score1)
//		println(score + " = " + PD_log + " - " + score1 )
		score
	}

	/*********************期望BIC评分函数****************************/
	def Q_BIC(bn: BayesianNetwork) = {

		val n = bn.NodeNum
		var model_dimension = 0
		var likelihood = 0.0
		var score = 0.0
		var score1 = 0.0

//		var outLog_BIC = new FileWriter(outputFile.temporary_BIC, true) //以追加写入的方式创建FileWriter对象

		for(i <- 0 until bn.NodeNum){
			likelihood = 0.0
			for(j <- 0 until bn.q(i))
				for(k <- 0 until bn.r(i)) {
					likelihood += bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))                                    //log似然度
//					outLog_BIC.write(bn.M_ijk(i)(j)(k) + "*" + log(bn.theta(i)(j)(k)) + "=" + bn.M_ijk(i)(j)(k) + "*" + "log("+bn.theta(i)(j)(k)+")" + "=" + (bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))) + "\r\n")
				}
			model_dimension = bn.q(i) * (bn.r(i) - 1)  //独立参数个数
			score1 = (model_dimension / 2.0).toDouble * log(reference.SampleNum.toDouble)
// ///////////////////////////////////////////////////////////////////////////////////////////////
//			score = likelihood  - score1
/////////////////////////////////////////////////////////////////////////////////////////////////
			score = likelihood  - reference.BIC * score1
/////////////////////////////////////////////////////////////////////////////////////////////////
//			score = likelihood
/////////////////////////////////////////////////////////////////////////////////////////////////
//			println(likelihood - score1 + " " + likelihood + " " +score1)
			bn.Q_BIC(i) = score
		}

//		outLog_BIC.close()
	}

	def Q_BIC_F(bn: BayesianNetwork) = {

		val n = bn.NodeNum
		var model_dimension = 0
		var likelihood = 0.0
		var score = 0.0
		var score1 = 0.0

//		var outLog_BIC = new FileWriter(outputFile.temporary_BIC, true) //以追加写入的方式创建FileWriter对象
//		println("change_node_num : " + bn.change_node_num)
		for(i <- 0 until bn.change_node_num){
			likelihood = 0.0
//			println()
//			println("i : "+i)
			for(j <- 0 until bn.q(bn.change_node(i))){
//				println("j : "+j)
//				println(bn.q(bn.change_node(i)))
				for(k <- 0 until bn.r(bn.change_node(i))) {
//					println("k : "+k)
//					println(bn.r(bn.change_node(i)))
//					println(bn.M_ijk(bn.change_node(i))(j)(k) + " * " + log(bn.theta(bn.change_node(i))(j)(k)))
					likelihood += bn.M_ijk(bn.change_node(i))(j)(k) * log(bn.theta(bn.change_node(i))(j)(k))                                    //log似然度
//					outLog_BIC.write(bn.M_ijk(i)(j)(k) + "*" + log(bn.theta(i)(j)(k)) + "=" + bn.M_ijk(i)(j)(k) + "*" + "log("+bn.theta(i)(j)(k)+")" + "=" + (bn.M_ijk(i)(j)(k) * log(bn.theta(i)(j)(k))) + "\r\n")
				}
			}
			model_dimension = bn.q(bn.change_node(i)) * (bn.r(bn.change_node(i)) - 1)  //独立参数个数
			score1 = (model_dimension / 2.0).toDouble * log(reference.SampleNum.toDouble)
			// ///////////////////////////////////////////////////////////////////////////////////////////////
			//			score = likelihood  - score1
			/////////////////////////////////////////////////////////////////////////////////////////////////
			score = likelihood  - reference.BIC * score1
			/////////////////////////////////////////////////////////////////////////////////////////////////
//			score = likelihood
			/////////////////////////////////////////////////////////////////////////////////////////////////
//			println(likelihood - score1 + " " + likelihood + " " +score1)
			bn.Q_BIC(bn.change_node(i)) = score
//			System.exit(0)
		}

//		outLog_BIC.close()
	}



	def generate_Chest_Clinic_Structure(structure: Array[Array[Int]]): Unit ={
		structure(1-1)(2-1) = 1
		structure(1-1)(3-1) = 1
		structure(2-1)(7-1) = 1
		structure(3-1)(6-1) = 1
		structure(4-1)(5-1) = 1
		structure(5-1)(6-1) = 1
		structure(6-1)(7-1) = 1
		structure(6-1)(8-1) = 1
	}
}
