package BNLV_learning.Learning

import BNLV_learning.Global.intermediate
import BNLV_learning.Input.BN_Input
import BNLV_learning.Output.BN_Output

/**
  * Created by Gao on 2016/10/9.
  */
object OtherFunction {

  /** 判断两个图结构是否相等 */
  def isEqualGraph(s1: Array[Array[Int]], s2: Array[Array[Int]], NodeNum: Int): Boolean = {
    for (i <- 0 until NodeNum)
      for (j <- 0 until NodeNum) {
        if ( s1(i)(j) != s2(i)(j) )
          return false
      }
    true
  }

  /** 读取文件对象BN到文本文档中 */
  def Read_BN_objectToFile(BN_ObjFile: String, structureFile: String, thetaFile: String) {

    val input = new BN_Input
    val output = new BN_Output
    val bn = input.ObjectFromFile(BN_ObjFile)
    output.structureToFile(bn, structureFile)
    output.CPT_FormatToFile(bn, thetaFile)
  }

}
