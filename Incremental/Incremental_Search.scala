//package BNLV_learning.Incremental
//
//import BNLV_learning.BayesianNetwork
//import BNLV_learning.Global.Candidate
//import BNLV_learning.Learning.Search._
//
//object Incremental_Search {
//
//	/**************确定加边范围****************/
//	def add_range(structure: Array[Array[Int]], NodeNum: Int, current_node: Int, add_node: Array[Int], S_node_parents_Array_No_Related:Array[Int]) {
//		/**
//		  * 在structure中,选出当前节点可对其进行加边操作的节点
//		  * current_node为当前操作节点
//		  * add_node[]存储当前节点可与之加边的节点,初始时add_node[NUM]={0},add_node[]=1——当前节点能够与第1个节点加边
//		  * 不能对与当前节点有连接的节点以及当前节点自己进行加边
//		  */
//		var j = 0
//		for(i <- 0 until NodeNum)
//			if(S_node_parents_Array_No_Related(current_node) != 1 && S_node_parents_Array_No_Related(i) != 1)
//				if(i != current_node && 0 == structure(i)(current_node) && 0 == structure(current_node)(i) ) {
//					add_node(j) = i+1
//					j += 1
//				}
//	}
//
//	/**************确定减边范围****************/
//	def delete_range(structure: Array[Array[Int]], NodeNum: Int, current_node: Int, delete_node: Array[Int], S_node_parents_Array_No_Related:Array[Int]) {
//		/**
//		  * 在structure中,选出当前节点可对其进行减边操作的节点
//		  * current_node为当前操作节点
//		  * delete_node[]存储当前节点可与之转边的节点,初始时delete_node[NUM]={0},delete_node[]=1——当前节点能够与第1个节点减边
//		  * 只能对与当前节点有连接的节点进行减边
//		  */
//		var j = 0
//		for (i <- 0 until NodeNum)
//		//if (1 == structure(i)(current_node) || 1 == structure(current_node)(i) ){
//			if(S_node_parents_Array_No_Related(current_node) != 1 && S_node_parents_Array_No_Related(i) != 1)
//				if (0 == structure(i)(current_node) && 1 == structure(current_node)(i) ){  //当前节点 指向 目标节点
//					delete_node(j) = i+1
//					j += 1
//				}
//	}
//
//	/**************确定转边范围****************/
//	def reverse_range(structure: Array[Array[Int]], NodeNum: Int, current_node: Int, reverse_node: Array[Int], S_node_parents_Array_No_Related:Array[Int]) {
//		/**
//		  * 在structure中,选出当前节点可对其进行转边操作的节点
//		  * current_node为当前操作节点
//		  * reverse_node[]存储当前节点可与之转边的节点,初始时reverse_node[NUM]={0},reverse_node[]=1——当前节点能够与第1个节点转边
//		  * 只能对与当前节点有连接的节点进行转边
//		  */
//		var j = 0
//		for (i <- 0 until NodeNum)
//		//if (1 == structure(i)(current_node) || 1 == structure(current_node)(i) ){
//			if(S_node_parents_Array_No_Related(current_node) != 1 && S_node_parents_Array_No_Related(i) != 1)
//				if (0 == structure(i)(current_node) && 1 == structure(current_node)(i) ){   //当前节点 指向 目标节点
//					reverse_node(j) = i+1
//					j += 1
//				}
//	}
//
//	/*******************加边操作*******************/
//	def add_edge(current_model: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]], NodeNum: Int, candidate_model: Array[BayesianNetwork], candidate: Candidate, current_node: Int, S_node_parents_Array_No_Related:Array[Int]) {
//
//		val n = NodeNum
//		val add_node = Array.fill[Int](n)(0)                  //当前节点可对其进行加边操作的节点,数组元素初始值为0
//		val temp = Array.ofDim[Int](n, n)
//		add_range(current_model, n, current_node, add_node, S_node_parents_Array_No_Related)      //确定加边范围
//
//		for (i <- 0 until array_length(add_node) ) {
//			copy_structure(temp, current_model, n)                 //当前模型结构拷贝到temp中
//			//加边操作,add_node[i]中存储的为节点号，因此使用时需要将值减1
//			temp(current_node)(add_node(i) - 1) = 1
//			if (!prune_check(temp, EdgeConstraint_violate, NodeNum, current_node, add_node(i) - 1)) {   //模型不满足剪枝条件
//				copy_structure(candidate_model(candidate.num).structure, temp, n)   //将候选模型结构存入candidate_model中
//				candidate_model(candidate.num).change_node_num = 1  // 当前模型父节点变化的节点个数为1个
//				candidate_model(candidate.num).change_node(0) = add_node(i) - 1    // 当前模型父节点变化的节点为add_node(i) - 1
//				candidate.num += 1
//			}
//		}
//	}
//
//	/*******************减边操作*******************/
//	def delete_edge(current_model: Array[Array[Int]], EdgeConstraint: Array[Array[Int]], NodeNum: Int, candidate_model: Array[BayesianNetwork], candidate: Candidate, current_node: Int, S_node_parents_Array_No_Related:Array[Int]) {
//
//		val n = NodeNum
//		val delete_node = Array.fill[Int](n)(0) //当前节点可对其进行减边操作的节点
//		val temp = Array.ofDim[Int](n, n)
//		delete_range(current_model, n, current_node, delete_node, S_node_parents_Array_No_Related) //确定减边范围
////		println("current_node : " + current_node)
////		delete_node.foreach(println(_))
////		System.exit(0)
//
//		for (i <- 0 until array_length(delete_node)) {
//			copy_structure(temp, current_model, n); //当前模型结构拷贝到temp中
//			//减边操作
//			temp(current_node)(delete_node(i) - 1) = 0
//			//		if (1 == temp(current_node)(delete_node(i) - 1))
//			//		temp(current_node)(delete_node(i) - 1) = 0
//			//		else
//			//		temp(delete_node(i) - 1)(current_node) = 0
//			//if (quality_check(temp, NodeNum)) {    //模型满足隐变量模型性质
//			if (!prune_check_deleteEdge(temp, EdgeConstraint, NodeNum, current_node, delete_node(i) - 1)) {   //模型不满足剪枝条件
//				copy_structure(candidate_model(candidate.num).structure, temp, n) //将候选模型结构存入candidate_model中
//				candidate_model(candidate.num).change_node_num = 1  // 当前模型父节点变化的节点个数为1个
//				candidate_model(candidate.num).change_node(0) = delete_node(i) - 1    // 当前模型父节点变化的节点为delete_node(i) - 1
//				candidate.num += 1
//			}
//		}
//	}
//
//	/*******************转边操作*******************/
//	def reverse_edge(current_model: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]], NodeNum: Int, candidate_model: Array[BayesianNetwork], candidate: Candidate, current_node: Int, S_node_parents_Array_No_Related:Array[Int]) {
//
//		val n = NodeNum
//		val reverse_node= Array.fill[Int](n)(0)                                      //当前节点可对其进行转边操作的节点
//		val temp = Array.ofDim[Int](n, n)
//		var exchange = 0                                                             //临时交换变量
//		reverse_range(current_model, n, current_node, reverse_node, S_node_parents_Array_No_Related)            //确定转边范围
//
//		for (i <- 0 until array_length(reverse_node) ) {
//			copy_structure(temp, current_model, n)                                     //当前模型结构拷贝到temp中
//			//转边操作——swap(temp(current_node)(reverse_node(i) - 1), temp(reverse_node(i) - 1)(current_node) )
//			//      exchange = temp(current_node)(reverse_node(i) - 1)
//			//      temp(current_node)(reverse_node(i) - 1) = temp(reverse_node(i) - 1)(current_node)
//			//      temp(reverse_node(i) - 1)(current_node) = exchange
//			temp(current_node)(reverse_node(i) - 1) = 0
//			temp(reverse_node(i) - 1)(current_node) = 1
//			if (!prune_check(temp, EdgeConstraint_violate, NodeNum, current_node, reverse_node(i) - 1)) {  //模型不满足剪枝条件
//				copy_structure(candidate_model(candidate.num).structure, temp, n)           //将候选模型结构存入candidate_model中
//				candidate_model(candidate.num).change_node_num = 2  // 当前模型父节点变化的节点个数为1个
//				candidate_model(candidate.num).change_node(0) = current_node    // 当前模型父节点变化的节点为current_node
//				candidate_model(candidate.num).change_node(1) = reverse_node(i) - 1    // 当前模型父节点变化的节点为reverse_node(i) - 1)
//				candidate.num += 1
//			}
//		}
//	}
//
//}
