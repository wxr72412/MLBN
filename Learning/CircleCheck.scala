package BNLV_learning.Learning

/**
  * Created by Gao on 2016/8/30.
  */

class tempReference {
  var isExist: Boolean = _                   //判断环是否存在
}

object CircleCheck {

  /**
    * 思想：用回溯法,遍历时，如果遇到了之前访问过的结点，则图中存在环。
    * 引入visited数组的原因：
    * 1）在一次深度遍历时，检测路径上是否结点是否已经被检测到，如果被重复检测，则表示有环。
    * 2）注意，在深度递归返回时，总是要把visited置为false。
    * 引入Isvisited数组的原因：
    * 1）用于记录目前为止深度遍历过程中遇到的顶点。
    * 2）因为，我们不一定以所有结点为起始点都进行一次深度遍历。
    * 3）举例，在结点A为起点，进行深度遍历时，遇到了结点B，此时Isvisited在A和B两个位置都为true。
    * 那么没遇到环，那么我们就不用再以B为起始点继续进行一次深度遍历了，
    * 因为A为起点的深度遍历已经验证不会有环了。
  */

def DFS(structure: Array[Array[Int]], NodeNum: Int, i: Int, reference: tempReference, visited: Array[Boolean], Isvisited: Array[Boolean]) {
	if (!reference.isExist) {
		visited(i) = true
		Isvisited(i) = true
		for (j <- 0 until NodeNum) {
			if (1 == structure(i)(j)) {
				if (false == visited(j)) {
					DFS(structure, NodeNum, j, reference, visited, Isvisited)
				}
				else {
					reference.isExist = true
				}
			}
		}
		visited(i) = false //回溯，如果不写就变成一半的深度遍历，不能进行判断是否有边存在
	}
	else if(reference.isExist){
		reference.isExist
	}
}

  /***************检查图中是否有环*****************/
  def process(structure: Array[Array[Int]], NodeNum: Int): Boolean = {

    val n = NodeNum                                     //BN的节点数
    val reference = new tempReference
    reference.isExist = false                          //判断环是否存在
    val Isvisited = Array.fill[Boolean](n)(false)     //创建大小为n的Boolean数组，并初始化每个元素为false
    val visited = Array.fill[Boolean](n)(false)
    for (i <- 0 until n) {
      if (false == Isvisited(i)) {
        DFS(structure, NodeNum, i, reference, visited, Isvisited)
        if (reference.isExist) {
          return true   //有环
        }
      }
    }
    reference.isExist  //无环
  }
}
