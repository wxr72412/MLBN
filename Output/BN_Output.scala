/**
  * Created by Gao on 2016/6/16.
  */
package BNLV_learning.Output

import java.io.{FileOutputStream, FileWriter, ObjectOutputStream, PrintWriter}
import java.text.NumberFormat

import BNLV_learning.BayesianNetwork
import BNLV_learning.Global.logFile

class BN_Output {

  val nf = NumberFormat.getNumberInstance()
  nf.setMaximumFractionDigits(8)                       //设置输出格式保留小数位数

	/** 按文件追加的方式，输出BN的结构到文件 */
	def structureAppendFile(bn: BayesianNetwork, fileName: String) {
		val outFile = new FileWriter(fileName, true)  //以追加写入的方式创建FileWriter对象

		for (i <- 0 until bn.NodeNum) {
			for (j <- 0 until bn.NodeNum - 1)
				outFile.write(bn.structure(i)(j) + " ")
			outFile.write(bn.structure(i)(bn.NodeNum - 1) + "\r\n")  //文件换行符\r\n
		}
		outFile.write("\r\n")
		outFile.close()
	}

	/** 按文件追加的方式，输出图结构到文件 */
	def structureAppendFile(structure: Array[Array[Int]], fileName: String, NodeNum: Int) {
		val outFile = new FileWriter(fileName, true)  //以追加写入的方式创建FileWriter对象

		for (i <- 0 until NodeNum) {
			for (j <- 0 until NodeNum - 1)
				outFile.write(structure(i)(j) + " ")
			outFile.write(structure(i)(NodeNum - 1) + "\r\n")  //文件换行符\r\n
		}
		outFile.write("\r\n")
		outFile.close()
	}

  /** 以格式化方式，按文件追加的方式，输出BN的参数到文件*/
  def CPT_FormatAppendFile(bn: BayesianNetwork, fileName: String) {
    val outFile = new FileWriter(fileName, true)  //以追加写入的方式创建FileWriter对象

    for (i <- 0 until bn.NodeNum - 1) {
      for (j <- 0 until bn.q(i)) {
        for (k<- 0 until bn.r(i) - 1)
          outFile.write(nf.format(bn.theta(i)(j)(k) ) + "\t")
        outFile.write(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ) + "\r\n")
      }
      outFile.write("#\r\n")
    }
    val i = bn.NodeNum - 1
    for (j <- 0 until bn.q(i)) {
      for (k<- 0 until bn.r(i) - 1)
        outFile.write(nf.format(bn.theta(i)(j)(k) ) + "\t")
      outFile.write(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ) + "\r\n")
    }
    outFile.write("\r\n")
    outFile.close()
  }

	/** 以格式化方式，按文件追加的方式，输出BN的参数到文件*/
	def CPT_AppendFile(bn: BayesianNetwork, fileName: String) {
		val outFile = new FileWriter(fileName, true)  //以追加写入的方式创建FileWriter对象

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
					outFile.write(bn.theta(i)(j)(k)  + "\t")
				outFile.write(bn.theta(i)(j)(bn.r(i) - 1)  + "\r\n")
			}
			outFile.write("#\r\n")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i)) {
			for (k<- 0 until bn.r(i) - 1)
				outFile.write(bn.theta(i)(j)(k)  + "\t")
			outFile.write(bn.theta(i)(j)(bn.r(i) - 1)  + "\r\n")
		}
		outFile.write("\r\n")
		outFile.close()
	}

	/** 以格式化方式，输出BN的参数到文件*/
	def CPT_FormatToFile(bn: BayesianNetwork, fileName: String) {
		val outFile = new PrintWriter(fileName)

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
					outFile.print(nf.format(bn.theta(i)(j)(k) ) + "\t")
				outFile.println(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))
			}
			outFile.println("#")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i) - 1) {
			for (k<- 0 until bn.r(i) - 1)
				outFile.print(nf.format(bn.theta(i)(j)(k) ) + "\t")
			outFile.println(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))
		}

		//输入文件的最后一行内容
		val j = bn.q(i) - 1
		for (k<- 0 until bn.r(i) - 1)
			outFile.print(nf.format(bn.theta(i)(j)(k) ) + "\t")
		outFile.print(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))

		outFile.close()
	}

	/** 以格式化方式，输出BN的参数到文件*/
	def CPT_FormatToFile_ijk(bn: BayesianNetwork, fileName: String) {
		val outFile = new PrintWriter(fileName)

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
					outFile.print(nf.format(bn.theta(i)(j)(k) ) + "\t")
				outFile.println(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))
			}
			outFile.println("#")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i) - 1) {
			for (k<- 0 until bn.r(i) - 1)
				outFile.print(nf.format(bn.theta(i)(j)(k) ) + "\t")
			outFile.println(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))
		}

		//输入文件的最后一行内容
		val j = bn.q(i) - 1
		for (k<- 0 until bn.r(i) - 1)
			outFile.print(nf.format(bn.theta(i)(j)(k) ) + "\t")
		outFile.print(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))

		outFile.close()
	}

	/** 以格式化方式，按文件追加的方式，输出BN的参数到文件*/
	def Mijk_FormatAppendFile(bn: BayesianNetwork, fileName: String) {
		val outFile = new FileWriter(fileName, true)  //以追加写入的方式创建FileWriter对象

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
					outFile.write(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
				outFile.write(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ) + "\r\n")
			}
			outFile.write("#\r\n")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i)) {
			for (k<- 0 until bn.r(i) - 1)
				outFile.write(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
			outFile.write(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ) + "\r\n")
		}
		outFile.write("\r\n")
		outFile.close()
	}
	def Mijk_AppendFile(bn: BayesianNetwork, fileName: String) {
		val outFile = new FileWriter(fileName, true)  //以追加写入的方式创建FileWriter对象

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
					outFile.write(bn.M_ijk(i)(j)(k)  + "\t")
				outFile.write(bn.M_ijk(i)(j)(bn.r(i) - 1)  + "\r\n")
			}
			outFile.write("#\r\n")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i)) {
			for (k<- 0 until bn.r(i) - 1)
				outFile.write(bn.M_ijk(i)(j)(k)  + "\t")
			outFile.write(bn.M_ijk(i)(j)(bn.r(i) - 1)  + "\r\n")
		}
		outFile.write("\r\n")
		outFile.close()
	}

	/** 以格式化方式，输出BN的参数到文件*/
	def Mijk_FormatToFile(bn: BayesianNetwork, fileName: String) {
		val outFile = new PrintWriter(fileName)

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
					outFile.print(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
				outFile.println(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ))
			}
			outFile.println("#")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i) - 1) {
			for (k<- 0 until bn.r(i) - 1)
				outFile.print(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
			outFile.println(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ))
		}

		//输入文件的最后一行内容
		val j = bn.q(i) - 1
		for (k<- 0 until bn.r(i) - 1)
			outFile.print(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
		outFile.print(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ))

		outFile.close()
	}

	/** 以格式化方式，输出BN的参数到控制台*/
	def CPT_FormatToConsole(bn: BayesianNetwork) {
		println("参数CPT: ")

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
//					print(nf.format(bn.theta(i)(j)(k) ) + "\t")
					print(i.toString+"-"+j.toString+"-"+k.toString+" "+nf.format(bn.theta(i)(j)(k)).toString  + "\t")
//				println(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))
				println(i.toString+"-"+j.toString+"-"+(bn.r(i) - 1).toString+" "+nf.format(bn.theta(i)(j)(bn.r(i) - 1)).toString)
			}
			println("#")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i) - 1) {
			for (k<- 0 until bn.r(i) - 1)
//				print(nf.format(bn.theta(i)(j)(k) ) + "\t")
				print(i.toString+"-"+j.toString+"-"+k.toString+" "+nf.format(bn.theta(i)(j)(k)).toString  + "\t")
//			println(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))
			println(i.toString+"-"+j.toString+"-"+(bn.r(i) - 1).toString+" "+nf.format(bn.theta(i)(j)(bn.r(i) - 1)).toString)
		}

		//输入文件的最后一行内容
		val j = bn.q(i) - 1
		for (k<- 0 until bn.r(i) - 1)
//			print(nf.format(bn.theta(i)(j)(k) ) + "\t")
			print(i.toString+"-"+j.toString+"-"+k.toString+" "+nf.format(bn.theta(i)(j)(k)).toString  + "\t")
//		println(nf.format(bn.theta(i)(j)(bn.r(i) - 1) ))
		println(i.toString+"-"+j.toString+"-"+(bn.r(i) - 1).toString+" "+nf.format(bn.theta(i)(j)(bn.r(i) - 1)).toString)

		println("****************")
	}

	/** 以格式化方式，输出BN的统计量到控制台*/
	def Mijk_FormatToConsole(bn: BayesianNetwork) {
		println("参数CPT: ")

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.q(i)) {
				for (k<- 0 until bn.r(i) - 1)
					print(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
				println(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ))
			}
			println("#")
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.q(i) - 1) {
			for (k<- 0 until bn.r(i) - 1)
				print(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
			println(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ))
		}

		//输入文件的最后一行内容
		val j = bn.q(i) - 1
		for (k<- 0 until bn.r(i) - 1)
			print(nf.format(bn.M_ijk(i)(j)(k) ) + "\t")
		println(nf.format(bn.M_ijk(i)(j)(bn.r(i) - 1) ))

		println("****************")
	}

	/** 输出BN的结构到文件 */
	def structureToFile(bn: BayesianNetwork, fileName: String) {
		val outFile = new PrintWriter(fileName)

		for (i <- 0 until bn.NodeNum - 1) {
			for (j <- 0 until bn.NodeNum - 1)
				outFile.print(bn.structure(i)(j) + " ")
			outFile.println(bn.structure(i)(bn.NodeNum - 1))
		}
		val i = bn.NodeNum - 1
		for (j <- 0 until bn.NodeNum - 1)                                //BN结构的邻接矩阵的最后一行
			outFile.print(bn.structure(i)(j) + " ")
		outFile.print(bn.structure(i)(bn.NodeNum - 1))

		outFile.close()
	}

	/** 输出结构到控制台 */
	def structureToConsole(bn: BayesianNetwork) {
		//NodeNum——BN的节点数
		for (i <- 0 until bn.NodeNum) {
			for (j <- 0 until bn.NodeNum)
				print(bn.structure(i)(j) + " ")
			println()
		}
		println()
	}

	def structureToConsole_matrix(structure: Array[Array[Int]]) {
		for (i <- 0 until structure.size) {
			for (j <- 0 until structure.size)
				print(structure(i)(j) + " ")
			println()
		}
		println()
	}
  /////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////
  /////////////////////////////////////////////////////////////////////









  /*******************输出候选模型到candidate_file*****************/
	def candidateStructureAppendFile(candidate_model: Array[BayesianNetwork], existed_candidate_num: Int, candidate_num: Int, candidate_file: String, current_node: Int, NodeNum: Int) {
		/*
		existed_candidate_num——已生成的候选模型数量
		candidate_model——候选模型存储的数组
		candidate_num——当前候选模型的总数量
		current_node——爬山过程的当前节点
		*/
		val  n = NodeNum
		val outCandidate = new FileWriter(candidate_file, true)         //以追加写入的方式创建FileWriter对象

		for (i <- existed_candidate_num until candidate_num) {
			outCandidate.write("第" + (i + 1) + "个候选模型\r\n")
			for (j <- 0 until n - 1) {
				for (k <- 0 until n - 1)
					outCandidate.write(candidate_model(i).structure(j)(k) + " ")
					outCandidate.write(candidate_model(i).structure(j)(n - 1) + "\r\n")
			}
			for (k <- 0 until n - 1)                                 //一个候选模型结构的邻接矩阵的最后一行,即j == n-1时
				outCandidate.write(candidate_model(i).structure(n - 1)(k) + " ")
			outCandidate.write(candidate_model(i).structure(n - 1)(n - 1) + "\r\n")

			outCandidate.write("父节点变化的节点：")
			for (k <- 0 until candidate_model(i).change_node_num)
				outCandidate.write(" "+candidate_model(i).change_node(k))
			outCandidate.write("\r\n")
			outCandidate.write("\r\n")
		}
		outCandidate.close()
	}







  /** 存储BN对象到文件 */
  def ObjectToFile(outObject: Object, fileName: String) {

    val outFile = new ObjectOutputStream(new FileOutputStream(fileName))
    outFile.writeObject(outObject)
    outFile.flush()
    outFile.close()
  }

}

/*  未进行输出格式化版本
 /** 输出BN的参数到控制台*/
 def CPT_ToConsole(bn: BayesianNetwork) {
   println("参数CPT: ")

   for (i <- 0 until bn.NodeNum - 1) {
     for (j <- 0 until bn.q(i)) {
       for (k<- 0 until bn.r(i) - 1)
         print(bn.theta(i)(j)(k) + "\t")
       println(bn.theta(i)(j)(bn.r(i) - 1))
     }
     println("#")
   }
   val i = bn.NodeNum - 1
   for (j <- 0 until bn.q(i) - 1) {
     for (k<- 0 until bn.r(i) - 1)
       print(bn.theta(i)(j)(k) + "\t")
     println(bn.theta(i)(j)(bn.r(i) - 1))
   }

   //输入文件的最后一行内容
   val j = bn.q(i) - 1
   for (k<- 0 until bn.r(i) - 1)
     print(bn.theta(i)(j)(k) + "\t")
   println(bn.theta(i)(j)(bn.r(i) - 1))

   println("****************")
 }

 /** 输出BN的参数到文件*/
 def CPT_ToFile(bn: BayesianNetwork, fileName: String) {
   val outFile = new PrintWriter(fileName)

   for (i <- 0 until bn.NodeNum - 1) {
     for (j <- 0 until bn.q(i)) {
       for (k<- 0 until bn.r(i) - 1)
         outFile.print(bn.theta(i)(j)(k) + "\t")
       outFile.println(bn.theta(i)(j)(bn.r(i) - 1))
     }
     outFile.println("#")
   }
   val i = bn.NodeNum - 1
   for (j <- 0 until bn.q(i) - 1) {
     for (k<- 0 until bn.r(i) - 1)
       outFile.print(bn.theta(i)(j)(k) + "\t")
     outFile.println(bn.theta(i)(j)(bn.r(i) - 1))
   }

   //输入文件的最后一行内容
   val j = bn.q(i) - 1
   for (k<- 0 until bn.r(i) - 1)
     outFile.print(bn.theta(i)(j)(k) + "\t")
   outFile.print(bn.theta(i)(j)(bn.r(i) - 1))

   outFile.close()
 }

   /** 按文件追加的方式，输出BN的参数到文件*/
  def CPT_AppendFile(bn: BayesianNetwork, fileName: String) {
    val outFile = new FileWriter(fileName, true)  //以追加写入的方式创建FileWriter对象

    for (i <- 0 until bn.NodeNum - 1) {
      for (j <- 0 until bn.q(i)) {
        for (k<- 0 until bn.r(i) - 1)
          outFile.write(bn.theta(i)(j)(k) + "\t")
        outFile.write(bn.theta(i)(j)(bn.r(i) - 1) + "\r\n")
      }
      outFile.write("#\r\n")
    }
    val i = bn.NodeNum - 1
    for (j <- 0 until bn.q(i)) {
      for (k<- 0 until bn.r(i) - 1)
        outFile.write(bn.theta(i)(j)(k) + "\t")
      outFile.write(bn.theta(i)(j)(bn.r(i) - 1) + "\r\n")
    }
    outFile.write("\r\n")
    outFile.close()
  }
 */