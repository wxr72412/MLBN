package BNLV_learning.Learning

import BNLV_learning.BayesianNetwork
import BNLV_learning.Global.{Candidate, reference}

/**
  * Created by Gao on 2016/8/30.
  */
object Search {

/**************确定加边范围****************/
def add_range(structure: Array[Array[Int]], NodeNum: Int, current_node: Int, add_node: Array[Int]) {
	/**
	* 在structure中,选出当前节点可对其进行加边操作的节点
	* current_node为当前操作节点
	* add_node[]存储当前节点可与之加边的节点,初始时add_node[NUM]={0},add_node[]=1——当前节点能够与第1个节点加边
	* 不能对与当前节点有连接的节点以及当前节点自己进行加边
	*/
	var j = 0
	for(i <- 0 until NodeNum)
	if(i != current_node && 0 == structure(i)(current_node) && 0 == structure(current_node)(i) ) {
		add_node(j) = i+1
		j += 1
	}
}

/**************确定减边范围****************/
def delete_range(structure: Array[Array[Int]], NodeNum: Int, current_node: Int, delete_node: Array[Int]) {
	/**
	* 在structure中,选出当前节点可对其进行减边操作的节点
	* current_node为当前操作节点
	* delete_node[]存储当前节点可与之转边的节点,初始时delete_node[NUM]={0},delete_node[]=1——当前节点能够与第1个节点减边
	* 只能对与当前节点有连接的节点进行减边
	*/
	var j = 0
	for (i <- 0 until NodeNum)
		//if (1 == structure(i)(current_node) || 1 == structure(current_node)(i) ){
		if (0 == structure(i)(current_node) && 1 == structure(current_node)(i) ){  //当前节点 指向 目标节点
			delete_node(j) = i+1
			j += 1
		}
}

/**************确定转边范围****************/
def reverse_range(structure: Array[Array[Int]], NodeNum: Int, current_node: Int, reverse_node: Array[Int]) {
	/**
	* 在structure中,选出当前节点可对其进行转边操作的节点
	* current_node为当前操作节点
	* reverse_node[]存储当前节点可与之转边的节点,初始时reverse_node[NUM]={0},reverse_node[]=1——当前节点能够与第1个节点转边
	* 只能对与当前节点有连接的节点进行转边
	*/
	var j = 0
	for (i <- 0 until NodeNum)
		//if (1 == structure(i)(current_node) || 1 == structure(current_node)(i) ){
		if (0 == structure(i)(current_node) && 1 == structure(current_node)(i) ){   //当前节点 指向 目标节点
			reverse_node(j) = i+1
			j += 1
		}
}

/***************检查当前结构是否满足隐变量模型的性质,即第n行不能全为0*****************/
def quality_check(structure: Array[Array[Int]], NodeNum: Int): Boolean = {

	for(i <- 1 to reference.LatentNum){    //保证每个隐变量都要指向一个元素
		for (j <- 0 until NodeNum)
			if (structure(NodeNum - i)(j) == 1)    //第n行存在非0元素，满足隐变量模型的性质
				return true
	}
	return false
//	for (i <- 0 until NodeNum - 1)
//		if (structure(NodeNum - 1)(i) != 0)    //第n行存在非0元素，满足隐变量模型的性质
//			return true
//	false
}

/***************检查当前结构是否满足给定的边约束*****************/
//def EdgeConstraint_check(structure: Array[Array[Int]], EdgeConstraint: Array[Array[Int]], NodeNum: Int, current_node: Int, operator_node: Int): Boolean = {
//    /**
//      * 搜索过程中约束边的方向,在加边、转边操作中需要进行此约束判定
//      * 所操作的边——structure[current_node][operator_node]或structure[operator_node][current_node]
//      * operator_node——当前节点与之操作的节点
//      */
//	if (1 == structure(current_node)(operator_node)) {  //加边操作会出现此种情况
//		if (EdgeConstraint(operator_node)(current_node) != 1)
//			return true                   //通过边约束检查
//		else                             //所操作的边与约束边的方向相反
//			return false
//	}
//	else {    //0 == structure[current_node][operator_node]
//		// 1==structure[operator_node][current_node])——转边操作会出现此种情况
//		if(EdgeConstraint(current_node)(operator_node) != 1)
//			return true                   //通过边约束检查
//		else                             //所操作的边与约束边的方向相反
//			return false
//	}
//}
def EdgeConstraint_check(structure: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]], NodeNum: Int, current_node: Int, operator_node: Int): Boolean = {
	/**
	  * 搜索过程中约束边的方向,在加边、转边操作中需要进行此约束判定
	  * 所操作的边——structure[current_node][operator_node]或structure[operator_node][current_node]
	  * operator_node——当前节点与之操作的节点
	  */
	if (1 == structure(current_node)(operator_node)) {  //加边操作会出现此种情况
		if (EdgeConstraint_violate(current_node)(operator_node) == 1)
			return true                   //通过边约束检查
		else                             //所操作的边与约束边的方向相反
			return false
	}
	else {    //0 == structure[current_node][operator_node]
		// 1==structure[operator_node][current_node])——转边操作会出现此种情况
		if(EdgeConstraint_violate(operator_node)(current_node) == 1)
			return true                   //通过边约束检查
		else                             //所操作的边与约束边的方向相反
			return false
	}
}

/****************判断结构是否会被剪枝****************/
def prune_check(structure: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]], NodeNum: Int, current_node: Int, operator_node: Int): Boolean = {

//	if (!quality_check(structure, NodeNum))           //不满足隐变量模型的性质，剪枝  true 满足性质
//		return true
	if(CircleCheck.process(structure, NodeNum))  //结构中存在环，剪枝   false不存在环
		return true
	//else if(!EdgeConstraint_check(structure, EdgeConstraint, NodeNum, current_node, operator_node))        //不满足边约束，剪枝   true 满足约束
	else if(!EdgeConstraint_check(structure, EdgeConstraint_violate, NodeNum, current_node, operator_node))
		return true
	else
		return false   //满足条件，不被剪枝
}

def EdgeConstraint_check_deleteEdge(structure: Array[Array[Int]], EdgeConstraint: Array[Array[Int]], NodeNum: Int, current_node: Int, operator_node: Int): Boolean = {
	if (EdgeConstraint(current_node)(operator_node) == 0)
		return true                   //通过边约束检查
	else                             //所操作的边与约束边的方向相反
		return false

}

/****************判断结构是否会被剪枝****************/
def prune_check_deleteEdge(structure: Array[Array[Int]], EdgeConstraint: Array[Array[Int]], NodeNum: Int, current_node: Int, operator_node: Int): Boolean = {

//	if (!quality_check(structure, NodeNum))           //不满足隐变量模型的性质，剪枝  true 满足性质
//		return true
//	else if(CircleCheck.process(structure, NodeNum))  //结构中存在环，剪枝   false不存在环
//		return true
	//else if(!EdgeConstraint_check(structure, EdgeConstraint, NodeNum, current_node, operator_node))        //不满足边约束，剪枝   true 满足约束
	if(!EdgeConstraint_check_deleteEdge(structure, EdgeConstraint, NodeNum, current_node, operator_node))
		return true
	else
		return false   //满足条件，不被剪枝
}


/*******************加边操作*******************/
def add_edge(current_model: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]], NodeNum: Int, candidate_model: Array[BayesianNetwork], candidate: Candidate, current_node: Int) {

	val n = NodeNum
	val add_node = Array.fill[Int](n)(0)                  //当前节点可对其进行加边操作的节点,数组元素初始值为0
	val temp = Array.ofDim[Int](n, n)
	add_range(current_model, n, current_node, add_node)      //确定加边范围

	for (i <- 0 until array_length(add_node) ) {
		copy_structure(temp, current_model, n)                 //当前模型结构拷贝到temp中
		//加边操作,add_node[i]中存储的为节点号，因此使用时需要将值减1
		temp(current_node)(add_node(i) - 1) = 1
		if (!prune_check(temp, EdgeConstraint_violate, NodeNum, current_node, add_node(i) - 1)) {   //模型不满足剪枝条件
			copy_structure(candidate_model(candidate.num).structure, temp, n)   //将候选模型结构存入candidate_model中
			candidate_model(candidate.num).change_node_num = 1  // 当前模型父节点变化的节点个数为1个
			candidate_model(candidate.num).change_node(0) = add_node(i) - 1    // 当前模型父节点变化的节点为add_node(i) - 1
			candidate.num += 1
		}
	}
}

/*******************减边操作*******************/
def delete_edge(current_model: Array[Array[Int]], EdgeConstraint: Array[Array[Int]], NodeNum: Int, candidate_model: Array[BayesianNetwork], candidate: Candidate, current_node: Int) {

	val n = NodeNum
	val delete_node = Array.fill[Int](n)(0) //当前节点可对其进行减边操作的节点
	val temp = Array.ofDim[Int](n, n)
	delete_range(current_model, n, current_node, delete_node) //确定减边范围

	for (i <- 0 until array_length(delete_node)) {
		copy_structure(temp, current_model, n); //当前模型结构拷贝到temp中
		//减边操作
		temp(current_node)(delete_node(i) - 1) = 0
//		if (1 == temp(current_node)(delete_node(i) - 1))
//		temp(current_node)(delete_node(i) - 1) = 0
//		else
//		temp(delete_node(i) - 1)(current_node) = 0
		//if (quality_check(temp, NodeNum)) {    //模型满足隐变量模型性质

		if (!prune_check_deleteEdge(temp, EdgeConstraint, NodeNum, current_node, delete_node(i) - 1)) {   //模型不满足剪枝条件
			copy_structure(candidate_model(candidate.num).structure, temp, n) //将候选模型结构存入candidate_model中
			candidate_model(candidate.num).change_node_num = 1  // 当前模型父节点变化的节点个数为1个
			candidate_model(candidate.num).change_node(0) = delete_node(i) - 1    // 当前模型父节点变化的节点为delete_node(i) - 1
			candidate.num += 1
		}
	}
}

/*******************转边操作*******************/
def reverse_edge(current_model: Array[Array[Int]], EdgeConstraint: Array[Array[Int]], EdgeConstraint_violate: Array[Array[Int]], NodeNum: Int, candidate_model: Array[BayesianNetwork], candidate: Candidate, current_node: Int) {

	val n = NodeNum
	val reverse_node= Array.fill[Int](n)(0)                                      //当前节点可对其进行转边操作的节点
	val temp = Array.ofDim[Int](n, n)
	var exchange = 0                                                             //临时交换变量
	reverse_range(current_model, n, current_node, reverse_node)            //确定转边范围

	for (i <- 0 until array_length(reverse_node) ) {
		copy_structure(temp, current_model, n)                                     //当前模型结构拷贝到temp中
		//转边操作——swap(temp(current_node)(reverse_node(i) - 1), temp(reverse_node(i) - 1)(current_node) )
		//      exchange = temp(current_node)(reverse_node(i) - 1)
		//      temp(current_node)(reverse_node(i) - 1) = temp(reverse_node(i) - 1)(current_node)
		//      temp(reverse_node(i) - 1)(current_node) = exchange




		temp(current_node)(reverse_node(i) - 1) = 0
		temp(reverse_node(i) - 1)(current_node) = 1
		if (!prune_check(temp, EdgeConstraint_violate, NodeNum, reverse_node(i) - 1, current_node)
			//		temp(current_node)(add_node(i) - 1) = 1
			//		if (!prune_check(temp, EdgeConstraint_violate, NodeNum, current_node, add_node(i) - 1)) {   //模型不满足剪枝条件
			&& !prune_check_deleteEdge(temp, EdgeConstraint, NodeNum, current_node, reverse_node(i) - 1)) {  //模型不满足剪枝条件
			//		temp(current_node)(delete_node(i) - 1) = 0
			//		if (!prune_check_deleteEdge(temp, EdgeConstraint, NodeNum, current_node, delete_node(i) - 1)) {   //模型不满足剪枝条件
			copy_structure(candidate_model(candidate.num).structure, temp, n)           //将候选模型结构存入candidate_model中
			candidate_model(candidate.num).change_node_num = 2  // 当前模型父节点变化的节点个数为1个
			candidate_model(candidate.num).change_node(0) = current_node    // 当前模型父节点变化的节点为current_node
			candidate_model(candidate.num).change_node(1) = reverse_node(i) - 1    // 当前模型父节点变化的节点为reverse_node(i) - 1)
			candidate.num += 1
		}
	}
}

  /**************模型拷贝***************/
	def copy_model(bn1: BayesianNetwork, bn2: BayesianNetwork) {
		//将模型bn2的结构以及q[]复制给bn1
		val n = bn1.NodeNum
		for (i <- 0 until n) {
			for (j <- 0 until n){
				bn1.structure(i)(j) = bn2.structure(i)(j)   //结构
			}
			bn1.q(i) = bn2.q(i)  //父节点取值个数
			bn1.r(i) = bn2.r(i)  //节点的势
			bn1.l(i) = bn2.l(i)  //节点的势
			bn1.latent(i) = bn2.latent(i)  //节点的势
		}
	}

	def copy_bn(bn1: BayesianNetwork, bn2: BayesianNetwork) {
		//将模型bn2的结构以及q[]复制给bn1
		val n = bn1.NodeNum
		for (i <- 0 until n) {
			for (j <- 0 until n)
				bn1.structure(i)(j) = bn2.structure(i)(j) //结构
			bn1.q(i) = bn2.q(i) //父节点取值个数
			bn1.r(i) = bn2.r(i) //节点的势
			bn1.l(i) = bn2.l(i)  //节点的势
		}
		bn1.create_CPT()
		for (i <- 0 until bn1.NodeNum){
			for (j <- 0 until bn1.q(i)) {
				for (k <- 0 until bn1.r(i)) {
					bn1.theta(i)(j)(k) = bn2.theta(i)(j)(k)
				}
			}
		}
	}

	/**************结构拷贝***************/  //可利用数组自带函数copyToArray实现,但会带来语义理解的困难
	def copy_structure(s1: Array[Array[Int]], s2: Array[Array[Int]], NodeNum: Int) {
		//将结构s2的内容复制给s1
		for (i <- 0 until NodeNum)
			for (j <- 0 until NodeNum)
				s1(i)(j) = s2(i)(j)
	}

  /***************参数顺序表拷贝***************/
  def copy_Theta_ijk(theta1: Array[Double], theta2: Array[Double], theta_num: Int) {
    //将参数顺序表theta2的内容复制给参数顺序表theta1
    for (i <- 0 until theta_num)
      theta1(i) = theta2(i)
  }

  /*
  /***************参数拷贝到vector***************/
  void theta_to_vector(vector<double> &theta1,vector<double> theta2,int theta_num)
  {//将参数theta2的内容复制给theta1
    int i;
    theta1.clear();
    for(i=0;i<theta_num;i++)
    theta1.push_back(theta2[i]);
  }
  */

  /**
    * 计算数组中依次存储的非0元素个数
    * node[0]=1,node[1]=4,node[2]=0...——array_length(node)返回2
    */
  def array_length(node: Array[Int]): Int = {
    node.count({x: Int => x > 0})
  }

  /*
  /***************交换a、b****************/
  def swap(int &a,int &b) {
    int temp;
    temp=a,a=b,b=temp;
  }
  */
}
