/**
  * Created by Gao on 2016/6/12.
  */
package BNLV_learning

import java.io.FileWriter
import java.util.Date

import BNLV_learning.Global._
import BNLV_learning.Inference._
import BNLV_learning.Learning.{OtherFunction, ParameterLearning, StructureLearning, StructureLearning2}
import BNLV_learning.Incremental._
import BNLV_learning.Input.BN_Input
import BNLV_learning.Output.BN_Output


object Main {
  def main(args: Array[String]) {
    val t_start = new Date()                       //获取起始时间对象
    println("BNLV Learning Based on Spark")


	  if(reference.cluster=="local") {
//		  inputFile.primitiveData = "data/alarm/alarm-10000.txt"
//		  inputFile.primitiveData = "Incremental/L2/data/1.txt"
//		  inputFile.primitiveData = "Incremental/L2/data/1M_L1_40W_1.0_learning.txt"
//		  inputFile.primitiveData = "Incremental/L2/data/1M_U1U2_40w_learning.txt"
//		  inputFile.primitiveData = "data/ml-1m/L2-120w-learning.txt"

//		  inputFile.primitiveData = "data/ml-1m/train_80per_1_MLBN.txt"
//		  inputFile.primitiveData = "data/ml-1m/train_80per_25W_MLBN.txt"
//		  inputFile.primitiveData = "data/ml-1m/train_80per_50W_MLBN.txt"
//		  inputFile.primitiveData = "data/ml-1m/train_80per_75W_MLBN.txt"
//		  inputFile.primitiveData = "data/ml-1m/train_80per_100W_MLBN.txt"

//		  inputFile.primitiveData = "data/Clothing-Fit-Data/train_80per_1_MLBN_CFD.txt"
//		  inputFile.primitiveData = "data/Clothing-Fit-Data/train_3.5W_CFD.txt"
//		  inputFile.primitiveData = "data/Clothing-Fit-Data/train_7W_CFD.txt"
		  inputFile.primitiveData = "data/Clothing-Fit-Data/train_10.5W_CFD.txt"
//		  inputFile.primitiveData = "data/Clothing-Fit-Data/train_14W_CFD.txt"


//		  inputFile.primitiveData = "data/Clothing-Fit-Data/1.txt"
	  }else if(reference.cluster=="spark") {
//		  inputFile.primitiveData = "hdfs://S4-B-Master:9000/BN-B/data.txt"
		  inputFile.primitiveData = "file:////mnt/shared/Datasets/MPBN/BN-B/data-incremental.txt"
	  }
	  reference.inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD

	  if(reference.data_type == "ml-1m"){
		  reference.userNum = 3
		  reference.LatentNum_Total = 3                       //设置BN隐变量数
		  reference.NodeNum_Total = reference.userNum + 3 *	reference.LatentNum_Total              //设置BN变量数
	  }else if(reference.data_type == "Clothing-Fit-Data") {
		  reference.userNum = 6
		  reference.LatentNum_Total = 4                       //设置BN隐变量数
		  reference.NodeNum_Total = reference.userNum + 3 *	reference.LatentNum_Total              //设置BN变量数
	  }



	  var bn_Array = new Array[BayesianNetwork](reference.LatentNum_Total)

	  if(reference.data_type == "ml-1m"){
		  //	  reference.Latent_r = Array(18,9)
		  reference.Latent_r = Array(9, 18, 53)
	  }else if(reference.data_type == "Clothing-Fit-Data") {
		  reference.Latent_r = Array(9, 68, 12, 3)
	  }

	  println("隐变量的势：")
	  reference.Latent_r.foreach(println(_))
	  if(reference.data_type == "ml-1m"){
		  reference.NodeNum = 6                          //设置BN节点数
	  }else if(reference.data_type == "Clothing-Fit-Data") {
		  reference.NodeNum = 9                          //设置BN节点数
	  }

	  reference.LatentNum = 1                        //设置BN隐变量数

      var bn = new BayesianNetwork(reference.NodeNum)
	  if(reference.data_type == "ml-1m"){
		  for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
			  bn_Array(i) = new BayesianNetwork(reference.NodeNum)
			  bn_Array(i).r(0) = 2; bn_Array(i).r(1) = 7; bn_Array(i).r(2) = 21;
			  bn_Array(i).r(3) = 5;
			  bn_Array(i).r(4) = reference.Latent_r(i);
			  bn_Array(i).r(5) = reference.Latent_r(i);

			  bn_Array(i).l(0) = "U"; bn_Array(i).l(1) = "U"; bn_Array(i).l(2) = "U";
			  bn_Array(i).l(3) = "R";
			  bn_Array(i).l(4) = "I";
			  bn_Array(i).l(5) = "L";
			  bn_Array(i).latent(5) = 1;
			  bn_Array(i).r.foreach(print(_))
			  bn_Array(i).l.foreach(print(_))
		  }
		  bn = new BayesianNetwork(reference.NodeNum_Total)
		  bn.r(0) = 2;
		  bn.r(1) = 7;
		  bn.r(2) = 21;
		  bn.l(0) = "U";
		  bn.l(1) = "U";
		  bn.l(2) = "U";
		  for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
			  bn.r(3+i* 3) = 5;
			  bn.r(4+i* 3) = reference.Latent_r(i);
			  bn.r(5+i* 3) = reference.Latent_r(i);
			  bn.l(3+i* 3) = "R";
			  bn.l(4+i* 3) = "I";
			  bn.l(5+i* 3) = "L";
			  bn.latent(5+i* 3) = 1;
		  }
	  }else if(reference.data_type == "Clothing-Fit-Data") {
		  for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
			  bn_Array(i) = new BayesianNetwork(reference.NodeNum)
			  bn_Array(i).r(0) = 7; bn_Array(i).r(1) = 7; bn_Array(i).r(2) = 7;bn_Array(i).r(3) = 7; bn_Array(i).r(4) = 7; bn_Array(i).r(5) = 7;
			  bn_Array(i).r(6) = 5;
			  bn_Array(i).r(7) = reference.Latent_r(i);
			  bn_Array(i).r(8) = reference.Latent_r(i);

			  bn_Array(i).l(0) = "U"; bn_Array(i).l(1) = "U"; bn_Array(i).l(2) = "U";bn_Array(i).l(3) = "U"; bn_Array(i).l(4) = "U"; bn_Array(i).l(5) = "U";
			  bn_Array(i).l(6) = "R";
			  bn_Array(i).l(7) = "I";
			  bn_Array(i).l(8) = "L";
			  bn_Array(i).latent(8) = 1;
			  bn_Array(i).r.foreach(print(_))
			  bn_Array(i).l.foreach(print(_))
		  }
		  bn = new BayesianNetwork(reference.NodeNum_Total)
		  bn.r(0) = 7;
		  bn.r(1) = 7;
		  bn.r(2) = 7;
		  bn.r(3) = 7;
		  bn.r(4) = 7;
		  bn.r(5) = 7;
		  bn.l(0) = "U";
		  bn.l(1) = "U";
		  bn.l(2) = "U";
		  bn.l(3) = "U";
		  bn.l(4) = "U";
		  bn.l(5) = "U";
		  for (i <- 0 until reference.LatentNum_Total) { //创建数组中每个对象
			  bn.r(6+i* 3) = 5;
			  bn.r(7+i* 3) = reference.Latent_r(i);
			  bn.r(8+i* 3) = reference.Latent_r(i);
			  bn.l(6+i* 3) = "R";
			  bn.l(7+i* 3) = "I";
			  bn.l(8+i* 3) = "L";
			  bn.latent(8+i* 3) = 1;
		  }
	  }


//	  System.exit(0)


//		reference.NodeNum = 3                          //设置BN节点数
//			reference.LatentNum = 1                        //设置BN隐变量数
//			bn = new BayesianNetwork(reference.NodeNum)
//			bn.r(0) = 2;   bn.r(1) = 2;    bn.r(2) = 2;
//			bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U";


//    reference.NodeNum = 6                          //设置BN节点数
//    reference.LatentNum = 1                        //设置BN隐变量数
//    bn = new BayesianNetwork(reference.NodeNum)
//    bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 18;   bn.r(5) = 18;
//    bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "L";


//		reference.NodeNum = 6                          //设置BN节点数
//		reference.LatentNum = 1                        //设置BN隐变量数
//		bn = new BayesianNetwork(reference.NodeNum)
//		bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 5;   bn.r(5) = 5;
//		bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "L";

//    reference.NodeNum = 8                          //设置BN节点数
//    reference.LatentNum = 2                        //设置BN隐变量数
//    bn = new BayesianNetwork(reference.NodeNum)
//    bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 5;   bn.r(5) = 5;  bn.r(6) = 5;    bn.r(7) = 5;
//    bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "I"; bn.l(6) = "L"; bn.l(7) = "L";


//				reference.NodeNum = 10                          //设置BN节点数
//		    reference.LatentNum = 3                        //设置BN隐变量数
//		    bn = new BayesianNetwork(reference.NodeNum)
//		    bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 5;   bn.r(5) = 5;  bn.r(6) = 5;    bn.r(7) = 5;	  bn.r(8) = 5;    bn.r(9) = 5;
//		    bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "I"; bn.l(6) = "I"; bn.l(7) = "L";   bn.l(8) = "L"; bn.l(9) = "L";

//		reference.NodeNum = 12                          //设置BN节点数
//		reference.LatentNum = 4                        //设置BN隐变量数
//		bn = new BayesianNetwork(reference.NodeNum)
//		bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 5;   bn.r(5) = 5;  bn.r(6) = 5;    bn.r(7) = 5;	  bn.r(8) = 5;    bn.r(9) = 5;	  bn.r(10) = 5;    bn.r(11) = 5;
//		bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "I"; bn.l(6) = "I"; bn.l(7) = "I";   bn.l(8) = "L"; bn.l(9) = "L";   bn.l(10) = "L"; bn.l(11) = "L";

//		reference.NodeNum = 14                          //设置BN节点数
//				reference.LatentNum = 5                        //设置BN隐变量数
//				bn = new BayesianNetwork(reference.NodeNum)
//				bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 10;   bn.r(5) = 10;  bn.r(6) = 10;    bn.r(7) = 10;	  bn.r(8) = 10;    bn.r(9) = 10;	  bn.r(10) = 10;    bn.r(11) = 10; bn.r(12) = 10;    bn.r(13) = 10;
//				bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "I"; bn.l(6) = "I"; bn.l(7) = "I";   bn.l(8) = "I"; bn.l(9) = "L";   bn.l(10) = "L"; bn.l(11) = "L";   bn.l(12) = "L"; bn.l(13) = "L";

//
//    reference.NodeNum = 9                          //设置BN节点数
//    reference.LatentNum = 2                        //设置BN隐变量数
//	  bn = new BayesianNetwork(reference.NodeNum)
//    bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 18;   bn.r(5) = 18;  bn.r(6) = 5;    bn.r(7) = 9;   bn.r(8) = 9;
//    bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "L"; bn.l(6) = "R"; bn.l(7) = "I";  bn.l(8) = "L";


    /********************更换数据集一定要记得修改*****************/
    //	  gender  age occupation  rating  genre		yearis_		child
    //	  2       7   21          5       17		10			2





//    	bn.r(0) = 2;   bn.r(1) = 7;    bn.r(2) = 21;  bn.r(3) = 5;    bn.r(4) = 17;   bn.r(5) = 2;  bn.r(6) = 10;    bn.r(7) = 17;      bn.r(8) = 2;    bn.r(9) = 10;
//    	bn.l(0) = "U"; bn.l(1) = "U"; bn.l(2) = "U"; bn.l(3) = "R"; bn.l(4) = "I"; bn.l(5) = "I"; bn.l(6) = "I";     bn.l(7) = "L";     bn.l(8) = "L";  bn.l(9) = "L";

    //bn.r(0) = 2; bn.r(1) = 3; bn.r(2) = 3; bn.r(3) = 5; bn.r(4) = 3; bn.r(bn.NodeNum-1) = bn.r(2)  //设置BN各变量的势——模拟数据5个节点
    //bn.r(0) = 2; bn.r(1) = 3; bn.r(2) = 3; bn.r(3) = 5; bn.r(bn.NodeNum-1) = bn.r(2)  //设置BN各变量的势
    /********************更换数据集一定要记得修改*****************
      * 数据集各变量说明
      * r(0)——性别，r(1)——年龄段，r(2)——电影类型，r(3)——评分值，r(4)——职业，r(5)——用户对电影类型的偏好
      */
    //bn.r(0) = 2; bn.r(1) = 7; bn.r(2) = 18; bn.r(3) = 5; bn.r(4) = 21; bn.r(bn.NodeNum-1) = bn.r(2)
    /*
    bn.ri_computation(inputFile.primitiveData, separator.data)
    bn.r(bn.NodeNum-1) = bn.r(2)
    */

    /** ThetaGeneratedKind == 0 —— "Random"方式随机生成初始参数
      * ThetaGeneratedKind == 1 ——"WeakConstraint3"方式随机生成初始参数——约束随机生成p(I|L)和约束随机生成p(R|I,L)
      * ThetaGeneratedKind == 2 ——"WeakConstraint1"方式随机生成初始参数——约束随机生成p(I|L)
      */
//    if (args.length > 0){
		//初始模型确定——终端输入参数——数据集、初始结构、初始参数生成方式、EM收敛阈值、数据集分区数目
        //inputFile.primitiveData = args(0)   //数据集
		//inputFile.inferenceData = args(0)
		//inputFile.testData = args(1)
		//inputFile.initial_structure = args(1)   //初始结构
		//inputFile.bn_structure = args(2)
		//reference.ThetaGeneratedKind = args(2).toInt   //初始参数生成方式
		//intermediate.initial_BN_object = args(3)
		//intermediate.BN_object = args(3)
		//reference.EM_threshold = args(3).toDouble   //EM收敛阈值
		//SparkConf.partitionNum = args(4).toInt   //数据集分区数目
//    }

    for (i <- 0 until bn.NodeNum)
      print(bn.r(i) + "\t")
    println()

    var operation = 333                                         //提交到Spark集群运算时，需要去除while循环
      println("请输入需要进行的操作: ")
      print("1——参数学习\n")
      print("2——结构学习\n")
      print("3/4——BN推理\n")
	  print("333——BN偏好推理\n")
	  print("44——BN评分推理\n")
      print("6——增量学习\n")
	  print("7——各自图学习\n")
	  print("8——子图合并\n")
	  print("9——MLBN增量学习\n")
      println("0——退出")

      //operation = readInt()                                      //从终端输入操作命令

      println("当前操作为 ： " + operation)
      operation match {
        case 1 =>  // 参数学习
			ParameterLearning.learningProcess(bn)
          val t_end = new Date()                                      //getTime获取得到的数值单位：数
          val runtime = (t_end.getTime - t_start.getTime)/1000.0
          val outTime = new FileWriter(logFile.totalTime)
          outTime.write(runtime + "\r\n")
          outTime.close()
        case 2 => // 结构学习
          reference.inFile_Data = reference.inFile
          StructureLearning.learningProcess(bn)
          val t_end = new Date()                                      //getTime获取得到的数值单位：数
          val runtime = (t_end.getTime - t_start.getTime)/1000.0
          val outTime = new FileWriter(logFile.totalTime)
          outTime.write(runtime + "\r\n")
          outTime.close()

        case 3 =>  // 偏好估计
			BN_Inference.PerferenceInferenceProcess(bn)
          val t_end = new Date()                                      //getTime获取得到的数值单位：数
			val runtime = (t_end.getTime - t_start.getTime)/1000.0
          val outTime = new FileWriter(logFile.totalTime)
          outTime.write(runtime + "\r\n")
          outTime.close()
		case 33 =>  // 偏好估计
			Inference_other.PerferenceInferenceProcess()
			val t_end = new Date()                                      //getTime获取得到的数值单位：数
			val runtime = (t_end.getTime - t_start.getTime)/1000.0
			val outTime = new FileWriter(logFile.totalTime)
			outTime.write(runtime + "\r\n")
			outTime.close()

		case 333 =>  // 偏好估计
			BN_Inference.preferenceInferenceProcess_strategy()
			val t_end = new Date()                                      //getTime获取得到的数值单位：数
			val runtime = (t_end.getTime - t_start.getTime)/1000.0
			val outTime = new FileWriter(logFile.totalTime)
			outTime.write(runtime + "\r\n")
			outTime.close()


        case 4 =>  // 评分估计
			BN_Inference.ratingInferenceProcess(bn)
			val t_end = new Date()                                      //getTime获取得到的数值单位：数
			val runtime = (t_end.getTime - t_start.getTime)/1000.0
			val outTime = new FileWriter(logFile.totalTime)
			outTime.write(runtime + "\r\n")
			outTime.close()
		case 44 =>  // 评分估计
			BN_Inference.ratingInferenceProcess_strategy(bn)
			val t_end = new Date()                                      //getTime获取得到的数值单位：数
			val runtime = (t_end.getTime - t_start.getTime)/1000.0
			val outTime = new FileWriter(logFile.totalTime)
			outTime.write(runtime + "\r\n")
			outTime.close()


        case 5 =>  // 概率推理
			BN_Inference.InferenceProcess(bn)
          val t_end = new Date()                                      //getTime获取得到的数值单位：数
          val runtime = (t_end.getTime - t_start.getTime)/1000.0
          val outTime = new FileWriter(logFile.totalTime)
          outTime.write(runtime + "\r\n")
          outTime.close()


        case 6 =>  // 增量学习
		  		IncrementalLearning.learningProcess(bn)
          val t_end = new Date()                                      //getTime获取得到的数值单位：数
          val runtime = (t_end.getTime - t_start.getTime)/1000.0
          val outTime = new FileWriter(logFile.totalTime)
          outTime.write(runtime + "\r\n")
          outTime.close()


		case 7 => // 各子图学习
			reference.R_type = "1"
        	for(i <- 0 until reference.LatentNum_Total){ //reference.LatentNum_Total

            //修改文件名
					var temp_string = (i+1).toString + ".txt"
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				if (reference.cluster == "spark") {
					outputFile.initial_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/initial_structure" + temp_string
					outputFile.initial_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/initial_theta" + temp_string
					outputFile.optimal_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_structure" + temp_string
					outputFile.optimal_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_theta" + temp_string
					logFile.candidateStructure = "/mnt/shared/Datasets/MPBN/SEM_logB/candidateStructure_file" + temp_string
					logFile.parameterLearning = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLearning_log" + temp_string
					logFile.structureLearning = "/mnt/shared/Datasets/MPBN/SEM_logB/structureLeaning_log" + temp_string
					logFile.BIC_score = "/mnt/shared/Datasets/MPBN/SEM_logB/BIC_score" + temp_string
					logFile.structureLearningTime = "/mnt/shared/Datasets/MPBN/SEM_logB/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLeaning_time" + temp_string
			  }
//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			  else if (reference.cluster == "local") {
				  outputFile.initial_structure = "out/Log/initial_structure" + temp_string
				  outputFile.initial_theta = "out/Log/initial_theta" + temp_string
				  outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
				  outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
				  logFile.candidateStructure = "out/Log/candidateStructure_file" + temp_string
				  logFile.parameterLearning = "out/Log/parameterLearning_log" + temp_string
				  logFile.structureLearning = "out/Log/structureLeaning_log" + temp_string
				  logFile.BIC_score = "out/Log/BIC_score" + temp_string
				  logFile.structureLearningTime = "out/Log/structureLeaning_time" + temp_string
				  logFile.parameterLearningTime = "out/Log/parameterLeaning_time" + temp_string
			  }
//////////////////////////////////////////////////////////////////////////////////////////////////////////

            //截取与子图相关的数据
            reference.inFile_Data = reference.inFile.map {
              line =>
                val data_Array = line.split(separator.data)
                var lines = ""
                for (j <- 0 to reference.userNum) {
                    lines += data_Array(j) + separator.data
                }
                lines += data_Array(reference.userNum+1+i)
                lines
            }
            // 结构学习
            StructureLearning.learningProcess(bn_Array(i))
          }
          val t_end = new Date()                                      //getTime获取得到的数值单位：数
          val runtime = (t_end.getTime - t_start.getTime)/1000.0
          val outTime = new FileWriter(logFile.totalTime)
          outTime.write(runtime + "\r\n")
          outTime.close()


		case 8 => // 结构合并
			reference.R_type = "n"
			for(i <- 0 until reference.LatentNum_Total){

				//修改文件名
				var temp_string = (i+1).toString + ".txt"
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				if (reference.cluster == "spark") {
					outputFile.optimal_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_structure" + temp_string
					outputFile.optimal_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_theta" + temp_string
				}
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				else if (reference.cluster == "local") {
					outputFile.optimal_structure = "out/Log/optimal_structure" + temp_string
					outputFile.optimal_theta = "out/Log/optimal_theta" + temp_string
				}
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
			if (reference.cluster == "spark"){
				outputFile.initial_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/initial_structure" + temp_string
				outputFile.initial_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/initial_theta" + temp_string
				outputFile.optimal_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_structure" + temp_string
				outputFile.optimal_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_theta" + temp_string
				logFile.candidateStructure  = "/mnt/shared/Datasets/MPBN/SEM_logB/candidateStructure_file" + temp_string
				logFile.parameterLearning  = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLearning_log" + temp_string
				logFile.structureLearning  = "/mnt/shared/Datasets/MPBN/SEM_logB/structureLeaning_log" + temp_string
				logFile.BIC_score  = "/mnt/shared/Datasets/MPBN/SEM_logB/BIC_score" + temp_string
				logFile.structureLearningTime  = "/mnt/shared/Datasets/MPBN/SEM_logB/structureLeaning_time" + temp_string
				logFile.parameterLearningTime = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLeaning_time" + temp_string
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			else if (reference.cluster == "local") {
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
			}
			//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
			var incremental_change_mark = new Array[Array[Int]](reference.LatentNum_Total)
			// 结构合并
			StructureLearning2.learningProcess1(bn, bn_Array, 0, incremental_change_mark)
			val t_end = new Date()                                      //getTime获取得到的数值单位：数
			val runtime = (t_end.getTime - t_start.getTime)/1000.0
			val outTime = new FileWriter(logFile.totalTime)
			outTime.write(runtime + "\r\n")
			outTime.close()

		case 9 => // 各子图增量学习
			reference.R_type = "1"
			var incremental_change_flag = 0
			var incremental_change_mark = new Array[Array[Int]](reference.LatentNum_Total)
			for(i <- 0 until reference.LatentNum_Total) {
//			for(i <- 1 until 2) {
				//修改文件名
				var temp_string = (i + 1).toString + ".txt"
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				if (reference.cluster == "spark") {
					inputFile.incremental_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_structure" + temp_string
					inputFile.incremental_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_theta" + temp_string

					outputFile.incremental_optimal_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_optimal_structure" + temp_string
					outputFile.incremental_optimal_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_optimal_theta" + temp_string
					outputFile.incremental_optimal_structure_S = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_optimal_structure_S" + temp_string
					outputFile.incremental_optimal_theta_S = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_optimal_theta_S" + temp_string

					logFile.candidateStructure = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_candidateStructure" + temp_string
					logFile.parameterLearning = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLearning_log" + temp_string
					logFile.incremental_Learning = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_log" + temp_string
					logFile.incremental_BIC_score = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_BIC_score" + temp_string
//					logFile.incremental_LearningTime  = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_LearningTime" + temp_string
					logFile.parameterLearningTime = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLeaning_time" + temp_string
					logFile.structureLearningTime = "/mnt/shared/Datasets/MPBN/SEM_logB/structureLeaning_time" + temp_string
				}
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				else if (reference.cluster == "local") {
					inputFile.incremental_structure = "out/Log/incremental/optimal_structure" + temp_string
					inputFile.incremental_theta = "out/Log/incremental/optimal_theta" + temp_string

					outputFile.incremental_optimal_structure = "out/Log/incremental/incremental_optimal_structure" + temp_string
					outputFile.incremental_optimal_theta = "out/Log/incremental/incremental_optimal_theta" + temp_string
					outputFile.incremental_optimal_structure_S = "out/Log/incremental/incremental_optimal_structure_S" + temp_string
					outputFile.incremental_optimal_theta_S = "out/Log/incremental/incremental_optimal_theta_S" + temp_string

					logFile.candidateStructure = "out/Log/incremental/incremental_candidateStructure" + temp_string
					logFile.parameterLearning = "out/Log/parameterLearning_log" + temp_string
					logFile.incremental_Learning = "out/Log/incremental/incremental_log" + temp_string
					logFile.incremental_BIC_score = "out/Log/incremental/incremental_BIC_score" + temp_string
//					logFile.incremental_LearningTime = "Incremental/L2/log/incremental_LearningTime" + temp_string
					logFile.parameterLearningTime = "out/Log/incremental/parameterLeaning_time" + temp_string
					logFile.structureLearningTime = "out/Log/incremental/structureLeaning_time" + temp_string
				}
				//////////////////////////////////////////////////////////////////////////////////////////////////////////

				//截取与子图相关的数据
				reference.inFile_Data = reference.inFile.map {
					line =>
						val data_Array = line.split(separator.data)
						var lines = ""
						for (j <- 0 to 3) {
							lines += data_Array(j) + separator.data
						}
						///////////// 100%改电影属性 ////////////////
//						var I = data_Array(4 + i).toInt + 4
//						if(I > 9){
//							I = I - 9
//						}
//						lines += I.toString
						/////////////////////////////
						lines += data_Array(4 + i)
						lines
				}
				// 增量学习
				incremental_change_mark(i) = new Array[Int](reference.NodeNum)
				incremental_change_mark(i) = IncrementalLearning.learningProcess(bn_Array(i))
				if (incremental_change_mark(i).sum > 0) {
					incremental_change_flag = 1
				}
				println("incremental_change_flag : " + incremental_change_flag)
				for (j <- 0 until reference.NodeNum) {
					print(incremental_change_mark(i)(j) + " ")
				}
				println()
			}

//			val t_end = new Date()                                      //getTime获取得到的数值单位：数
//			val runtime = (t_end.getTime - t_start.getTime)/1000.0
//			val outTime = new FileWriter(logFile.totalTime)
//			outTime.write(runtime + "\r\n")
//			outTime.close()
//			System.exit(0)

			if(incremental_change_flag == 1 && reference.LatentNum_Total > 1){
				for(i <- 0 until reference.LatentNum_Total){
					//修改文件名
					var temp_string = (i+1).toString + ".txt"
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					if (reference.cluster == "spark") {
						outputFile.optimal_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_optimal_structure" + temp_string
						outputFile.optimal_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/Incremental/incremental_optimal_theta" + temp_string
					}
					//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
					else if (reference.cluster == "local") {
						outputFile.optimal_structure = "out/Log/incremental/incremental_optimal_structure" + temp_string
						outputFile.optimal_theta = "out/Log/incremental/incremental_optimal_theta" + temp_string
					}
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
				if (reference.cluster == "spark") {
					outputFile.initial_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/initial_structure" + temp_string
					outputFile.initial_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/initial_theta" + temp_string
					outputFile.optimal_structure = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_structure" + temp_string
					outputFile.optimal_theta = "/mnt/shared/Datasets/MPBN/SEM_logB/optimal_theta" + temp_string
					logFile.candidateStructure = "/mnt/shared/Datasets/MPBN/SEM_logB/candidateStructure_file" + temp_string
					logFile.parameterLearning = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLearning_log" + temp_string
					logFile.structureLearning = "/mnt/shared/Datasets/MPBN/SEM_logB/structureLeaning_log" + temp_string
					logFile.BIC_score = "/mnt/shared/Datasets/MPBN/SEM_logB/BIC_score" + temp_string
					logFile.structureLearningTime = "/mnt/shared/Datasets/MPBN/SEM_logB/structureLeaning_time" + temp_string
					logFile.parameterLearningTime = "/mnt/shared/Datasets/MPBN/SEM_logB/parameterLeaning_time" + temp_string
				}
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				else if (reference.cluster == "local") {
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
				}
				//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
				// 结构合并
				print("incremental_change_mark : ")
				for(i <- 0 until reference.LatentNum_Total){
					print(incremental_change_mark(i) + " ")
				}
				println()
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

//        case 9 =>
//          OtherFunction.Read_BN_objectToFile(outputFile.learned_BN_object, outputFile.BN_objectStructure, outputFile.BN_objectTheta)
        case 0 =>
          println("程序执行结束!")
          SparkConf.sc.stop()                                    //关闭SparkContext
          return
        case _ =>  println("输入的操作码不对!")
      }


    //}                                                      //end_while
    SparkConf.sc.stop()                                    //关闭SparkContext
  }
}