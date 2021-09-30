//package BNLV_learning.Global
//
//object separator {
//  val data = ","              //原始数据文档中的横向分隔符
//  val structure = " "         //结构文件中的横向分隔符
//  val theta = "\t"            //参数文件中的横向分隔符
//  val tab = "\t"              //制表分隔符
//  val space = " "             //空格分隔符
//}
//
////Linux平台路径版
//object inputFile {
//  var primitiveData = "hdfs://S4-B-Master:9000/BN-B/data.txt"                                  //BN模型学习数据
//  //val primitiveData = "file:///mnt/shared/Datasets/wuxinran/dataset/L3-17-2-10-70A.txt"  //读取原始数据为RDD
//
//
////  var initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure.txt"                        //BN的初始结构
////  val initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta.txt"       //BN的初始参数——结构学习版
//  //val initial_theta_structureLearning = "in/test/initial_theta.txt"       //BN的初始参数——结构学习版
//
//
//
//  //var incremental_Data = "hdfs://S4-C-Master:9000/wuxinran/dataset.txt"                                     //BN的增量学习数据
//  val incremental_Data = "file:///mnt/shared/Datasets/wuxinran/dataset/dataset.txt"  //读取原始数据为RDD
//  //var incremental_Data_filled = "in/1M-1W/L2-1M-1_filled.txt"                                  //BN的增量学习数据
//  var incremental_structure = "/mnt/shared/Datasets/wuxinran/incremental_log/structure.txt"                            //BN的增量学习初始结构
//  var incremental_theta = "/mnt/shared/Datasets/wuxinran/incremental_log/theta.txt"                                    //BN的增量学习初始参数
//
//    var inference_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure.txt"                            //BN的初始结构
//    val inference_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta.txt"                                                     //进行BIC评分或模型推理的BN结构
//
//    var inference_learning_User_strategy = "Inference/L2/dataset/user-294-strategy.txt"
//  	var inference_learning_Data1 = "Inference/L2/dataset/L2-merge-120w-learning-inference.txt"
//  var inference_learning_Data = "file:///mnt/shared/Datasets/BN-B/user-6040.txt"
//    var inference_test_Data = "file:///mnt/shared/Datasets/BN-B/inference.txt"
//	var result = "Inference/L2/other/LDA-doc-word-user-genre-sorted.txt"
//    var InferenceProcess_Data = "Inference/L2/dataset/2.txt"
//    var initial_theta_parameterLearning = ""
//
//
//  //val EdgeConstraint_structure = "in/EdgeConstraint_structure.txt"                     //BN的边方向约束
//  //val primitive_structure = "in/primitive_structure.txt"		                           //原始BN结构或理想BN结构
//}
//
//object intermediate {
//  //val mendedData = "out/MedianResult/mended_data.txt"           //补后样本的期望数据文档
//  //var initial_BN_object = "out/Log/initial_BN_object.dat"       //以文件形式存储的初始BN对象
//  //var BN_object = "out/Log/BN_object.dat"                        //以文件形式存储的BN模型对象
//}
//
///**************最优模型及其参数的输出文件*****************/
//object outputFile {
//	var optimal_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_structure.txt"
//	var optimal_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/optimal_theta.txt"
//	var initial_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_structure.txt"
//	var initial_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/initial_theta.txt"
//	var mendeddata_structure = "/mnt/shared/Datasets/wuxinran/SEM_logB/mendeddata_structure.txt"
//	var mendeddata_theta = "/mnt/shared/Datasets/wuxinran/SEM_logB/mendeddata_theta.txt"
//
//	val temporary_structure1 = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary_structure1.txt"
//	val temporary_theta1 = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary_theta1.txt"
//    val temporary_mijk1 = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary_mijk1.txt"
//	val temporary_structure2 = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary_structure2.txt"
//	val temporary_theta2 = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary_theta2.txt"
//    val temporary_mijk2 = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary_mijk2.txt"
//
////    val temporary_I_theta00 = "Incremental/L2/temporary_I_theta00.txt"
////    val temporary_I_theta0 = "Incremental/L2/temporary_I_theta0.txt"
////    val temporary_I_mijk0 = "Incremental/L2/temporary_I_mijk0.txt"
////    val temporary_I_theta1 = "Incremental/L2/temporary_I_theta1.txt"
////    val temporary_I_mijk1 = "Incremental/L2/temporary_I_mijk1.txt"
////    val temporary_I_theta2 = "Incremental/L2/temporary_I_theta2.txt"
////    val temporary_I_mijk2 = "Incremental/L2/temporary_I_mijk2.txt"
//
////  val theta_structureLearning = "/mnt/shared/Datasets/wuxinran/SEM_logB/theta.txt"
//  //val learned_BN_object = "out/Log/learned_BN_object.dat"          //以文件形式存储学习所得的BN对象t"
//	//  val temporary_BIC = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary_BIC.txt"
//	//  val temporary = "/mnt/shared/Datasets/wuxinran/SEM_logB/temporary.txt"
//
//  //  val optimal_structure = "out/Log/optimal_structure.txt"
//  //  val optimal_theta = "out/Log/optimal_theta.txt"
//  //  val BN_objectStructure = "out/Result/BN_object_structure.txt"                      //展示文件对象BN的结构
//  //  val BN_objectTheta = "out/Result/BN_object_theta.txt"                              //展示文件对象BN的参数
//
//
//  var incremental_optimal_structure = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_optimal_structure.txt"
//  var incremental_optimal_theta = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_optimal_theta.txt"
//  var incremental_optimal_structure_S = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_optimal_structure_S.txt"
//  var incremental_optimal_theta_S = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_optimal_theta_S.txt"
//
////  val SampleInferenceResult = "/mnt/shared/Datasets/wuxinran/inference_log/SampleInferenceResult.csv"                    //根据样本导出的推理结果
////  val SampleInferenceResult_Format = "/mnt/shared/Datasets/wuxinran/inference_log/SampleInferenceResult_Format.csv"     //根据样本导出的推理结果——格式化小数位数
//  //val SamplePreferenceResult = "/mnt/shared/Datasets/wuxinran/inference_log/SamplePreferenceResult.csv"                  //根据样本导出的用户偏好结果
////  val SamplePreferenceResult = "Inference/L2/log/SamplePreferenceResult.txt"                  //根据样本导出的用户偏好结果
//  //val SamplePreferenceResult_Format = "/mnt/shared/Datasets/wuxinran/inference_log/SamplePreferenceResult_Format.csv"   //根据样本导出的用户偏好结果——格式化小数
//  val PreferenceResult_Learning = "/mnt/shared/Datasets/wuxinran/SEM_logB/PreferenceResult_Learning.txt"   //根据样本导出的用户偏好结果——格式化小数
//  val PreferenceResult_Test = "/mnt/shared/Datasets/wuxinran/SEM_logB/PreferenceResult_Test.txt"   //根据样本导出的用户偏好结果——格式化小数
//}
//
//object logFile {
//  val totalTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/total_time.txt"         //包含Spark程序启动时间以及学习执行时间
//
//  var candidateStructure = "/mnt/shared/Datasets/wuxinran/SEM_logB/candidateStructure_file.txt";
//  var parameterLearning = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLearning_log.txt"
//  var structureLearning = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_log.txt"
//  var BIC_score = "/mnt/shared/Datasets/wuxinran/SEM_logB/BIC_score.txt"
//  var structureLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/structureLeaning_time.txt"
//  var parameterLearningTime = "/mnt/shared/Datasets/wuxinran/SEM_logB/parameterLeaning_time.txt"
//
//  //  val testFile = "out/Log/test_log.txt"
//
//  var incremental_Learning = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_log.txt"
////  var incremental_candidateStructure = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_candidateStructure.txt"
////  var incremental_LearningTime = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_LearningTime.txt"
//  var incremental_BIC_score = "/mnt/shared/Datasets/wuxinran/incremental_log/incremental_BIC_score.txt"
//
////  val inference = "/mnt/shared/Datasets/wuxinran/inference_log/inference_log.txt"
//    val inference_log = "/mnt/shared/Datasets/wuxinran/SEM_logB/inference_log.txt"
//    val inference_temporary = "/mnt/shared/Datasets/wuxinran/SEM_logB/inference_temporary.txt"
//}




package BNLV_learning.Global

object separator {
	val data = ","              //原始数据文档中的横向分隔符
	val structure = " "         //结构文件中的横向分隔符
	val theta = "\t"            //参数文件中的横向分隔符
	val tab = "\t"              //制表分隔符
	val space = " "             //空格分隔符
}

//Linux平台路径版
object inputFile {
//	var primitiveData = "data/alarm/alarm-10000.txt"                                  //BN模型学习数据
//	var primitiveData = "Incremental/L2/data/1.txt"
	var primitiveData = "Incremental/L2/data/1M_L1_40W_0.25_learning.txt"
//	var primitiveData = "Incremental/L2/data/1M_U1U2_40w_learning.txt"
//	var primitiveData = "data/ml-1m/L2-120w-learning.txt"

	var initial_structure = "out/Log/initial_structure.txt"                        //BN的初始结构
	val initial_theta_structureLearning = "out/Log/initial_theta.txt"       //BN的初始参数——结构学习版
	val initial_theta_parameterLearning = "in/test/initial_theta.txt"       //BN的初始参数——参数学习版


//	var incremental_Data = "Incremental/L2/data/1M_L1_40W_1.0_learning.txt"                                  //BN的增量学习数据
	//  var incremental_Data_filled = "Incremental/L2/L2-1M-1_filled.txt"                                  //BN的增量学习数据
	var incremental_structure = "Incremental/L2/optimal_structure.txt"                            //BN的增量学习初始结构
	var incremental_theta = "Incremental/L2/optimal_theta.txt"                            //BN的增量学习初始参数

	var inference_structure = "Inference/L2/optimal_structure.txt"                            //BN的初始结构
	val inference_theta = "Inference/L2/optimal_theta.txt"                                                     //进行BIC评分或模型推理的BN结构


//	var inference_learning_Data = "Inference/L2/dataset/L2-merge-120w-learning-inference.txt"
//	var inference_learning_Data = "Inference/L2/dataset/1.txt"

	var inference_learning_Data = "Inference/L2/dataset/train_80per_1_MLBN_for_infer.txt"
	var inference_test_Data = "Inference/L2/dataset/test_20per_1_MLBN_for_infer.txt"
	var inference_learning_User = "Inference/L2/dataset/user-6040.txt"
	var inference_learning_User_strategy = "Inference/L2/dataset/user-294-strategy.txt"

//	var inference_learning_Data = "Inference/L2/dataset/train_80per_1_MLBN_for_infer_CFD.txt"
//	var inference_test_Data = "Inference/L2/dataset/test_20per_1_MLBN_for_infer_CFD.txt"
//	var inference_learning_User = "Inference/L2/dataset/user_CFD.txt"
//	var inference_learning_User_strategy = "Inference/L2/dataset/user-117649-strategy_CFD.txt"


//

//	var inference_test_Data = "Inference/L2/dataset/1M_L1_30W_0.0_inference.txt"

//    var inference_introduction_Data = "Inference/L2/dataset/1.txt"

//



	var InferenceProcess_Data = "Inference/L2/dataset/2.txt"

	var result = "Inference/L2/other/RSVD_userleibie_genre.txt"

	val EdgeConstraint_structure = "in/EdgeConstraint_structure.txt"                     //BN的边方向约束
	val primitive_structure = "in/primitive_structure.txt"		                           //原始BN结构或理想BN结构
}

object intermediate {
	val mendedData = "out/MedianResult/mended_data.txt"           //补后样本的期望数据文档
	var initial_BN_object = "out/Log/initial_BN_object.dat"       //以文件形式存储的初始BN对象
	var BN_object = "out/Log/BN_object.dat"                        //以文件形式存储的BN模型对象
}

/**************最优模型及其参数的输出文件*****************/
object outputFile {
	val theta_structureLearning = "out/Log/theta.txt"       //BN的初始参数——结构学习版
	val temporary_structure1 = "out/Log/temporary_structure1.txt"
	val temporary_theta1 = "out/Log/temporary_theta1.txt"
	val temporary_mijk1 = "out/Log/temporary_mijk1.txt"
	val temporary_structure2 = "out/Log/temporary_structure2.txt"
	val temporary_theta2 = "out/Log/temporary_theta2.txt"
	val temporary_mijk2 = "out/Log/temporary_mijk2.txt"



	val temporary_BIC = "out/Log/temporary_BIC.txt"

	var initial_structure = "out/Log/initial_structure.txt"
	var initial_theta = "out/Log/initial_theta.txt"

	val learned_BN_object = "out/Log/learned_BN_object.dat"          //以文件形式存储学习所得的BN对象

	var optimal_structure = "out/Log/optimal_structure.txt"
	var optimal_theta = "out/Log/optimal_theta.txt"

	var mendeddata_structure = "out/Log/mendeddata_structure.txt"
	var mendeddata_theta = "out/Log/mendeddata_theta.txt"

	val BN_objectStructure = "out/Result/BN_object_structure.txt"                      //展示文件对象BN的结构
	val BN_objectTheta = "out/Result/BN_object_theta.txt"                              //展示文件对象BN的参数

	//  val SampleInferenceResult = "out/Log/SampleInferenceResult.csv"                    //根据样本导出的推理结果
	//  val SampleInferenceResult_Format = "out/Log/SampleInferenceResult_Format.csv"     //根据样本导出的推理结果——格式化小数位数
	//  val SamplePreferenceResult = "out/Log/SamplePreferenceResult.csv"                  //根据样本导出的用户偏好结果
	val SamplePreferenceResult_Format = "Inference/L2/SamplePreferenceResult_Format.csv"   //根据样本导出的用户偏好结果——格式化小数
	val PreferenceResult_Learning = "Inference/L2/log/PreferenceResult_Learning.txt"   //根据样本导出的用户偏好结果——格式化小数
	var PreferenceResult_Test = "Inference/L2/log/PreferenceResult_Test.txt"   //根据样本导出的用户偏好结果——格式化小数

	var incremental_optimal_structure = "Incremental/L2/incremental_optimal_structure.txt"
	var incremental_optimal_theta = "Incremental/L2/incremental_optimal_theta.txt"
	var incremental_optimal_structure_S = "Incremental/L2/incremental_optimal_structure_S.txt"
	var incremental_optimal_theta_S = "Incremental/L2/incremental_optimal_theta_S.txt"

	val temporary_I_theta00 = "Incremental/L2/temporary_I_theta00.txt"
	val temporary_I_theta0 = "Incremental/L2/temporary_I_theta0.txt"
	val temporary_I_mijk0 = "Incremental/L2/temporary_I_mijk0.txt"
	val temporary_I_theta1 = "Incremental/L2/temporary_I_theta1.txt"
	val temporary_I_mijk1 = "Incremental/L2/temporary_I_mijk1.txt"
	val temporary_I_theta2 = "Incremental/L2/temporary_I_theta2.txt"
	val temporary_I_mijk2 = "Incremental/L2/temporary_I_mijk2.txt"

}

object logFile {
	var candidateStructure = "out/Log/candidateStructure_file.txt";

	var parameterLearning = "out/Log/parameterLearning_log.txt"
	var structureLearning = "out/Log/structureLeaning_log.txt"
	val inference = "out/Log/inference_log.txt"
	var BIC_score = "out/Log/BIC_score.txt"
	var structureLearningTime = "out/Log/structureLeaning_time.txt"
	var parameterLearningTime = "out/Log/parameterLeaning_time.txt"
	val totalTime = "out/Log/total_time.txt"         //包含Spark程序启动时间以及学习执行时间
	val testFile = "out/Log/test_log.txt"

	val inference_log = "Inference/L2/log/inference_log.txt"
	var inference_preference = "Inference/L2/log/inference_prerence.txt"
	val inference_temporary = "Inference/L2/log/inference_temporary.txt"

	var incremental_Learning = "Incremental/L2/log/incremental_log.txt"
	var incremental_LearningTime = "Incremental/L2/log/incremental_LearningTime.txt"
	var incremental_BIC_score = "Incremental/L2/log/incremental_BIC_score.txt"
}

