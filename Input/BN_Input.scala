/**
  * Created by Gao on 2016/6/17.
  */
package BNLV_learning.Input

import java.io.{FileInputStream, ObjectInputStream}

import scala.io.Source
import BNLV_learning.BayesianNetwork

class BN_Input {

  /** 从文件中读取表示BN结构的邻接矩阵到bn.structure中 */
  def structureFromFile(fileName: String, separator: String, bn: BayesianNetwork) {
    val inFile = Source.fromFile(fileName)     //创建文件读取对象
    val lineIterator = inFile.getLines         //按行将文件读入集合lineIterator中

    var i = 0                                  //i从0遍历到NodeNum-1
    for (line <- lineIterator) {  //sstr2value(s,bn.structure[i],n,separator);——C++
      val tokens = line.split(separator)       //将一行字符串按分隔符separator拆分读入字符串数组tokens中
      //val numbers = tokens.map.(_.toInt)
      bn.structure(i) = tokens.map(_.toInt)  //将tokens中的内容读取到bn.structure[i][0]...bn.structure[i][n-1]中
      i += 1
    }
    inFile.close()
  }

  /** 从文件中读取表示BN结构的邻接矩阵到structure中 */
  def structureFromFile(fileName: String, separator: String, structure: Array[Array[Int]]) {
    val inFile = Source.fromFile(fileName)     //创建文件读取对象
    val lineIterator = inFile.getLines         //按行将文件读入集合lineIterator中

    var i = 0                                  //i从0遍历到NodeNum-1
    for (line <- lineIterator) {  //sstr2value(s,bn.structure[i],n,separator);——C++
    val tokens = line.split(separator)       //将一行字符串按分隔符separator拆分读入字符串数组tokens中
      //val numbers = tokens.map.(_.toInt)
      structure(i) = tokens map { _.toInt}  //将tokens中的内容读取到bn.structure[i][0]...bn.structure[i][n-1]中
      i += 1
    }
    inFile.close()
  }

  /** 从文件中读取参数θ到动态三维数组theta中 */
  def CPT_FromFile(fileName: String, separator: String, bn: BayesianNetwork) {
    val inFile = Source.fromFile(fileName)
    val lineIterator = inFile.getLines         //按行将文件读入集合lineIterator中

    var i = 0
    var j = 0
    for (line <- lineIterator) {   //将第每个节点的CPT值从文件中取到内存——theta[i][j][k]
      if ("#" == line) {  //getline读取到纵向分隔符#
        if (bn.q(i) != j)
          Console.err.println("方法\"CPT_FromFile\"中存在错误！Error：bn.q(i) - 1 != j")
        i += 1 //i <- 0 until bn.NodeNum
        j = 0
      }
      else {  //sstr2value(s,bn.theta[i][j],bn.r[i],separator);——C++
        val tokens = line.split(separator)         //将一行字符串按分隔符separator拆分读入字符串数组tokens中
        //for(t <- tokens){print(t+",")}
        //println()
        bn.theta(i)(j) = tokens.map(_.toDouble)  //将tokens中的内容读取到 bn.theta[i][j][0]...bn.theta[i][j][bn.r[i]-1]中
        j += 1  //j <- 0 until bn.q(i)
      }
    }  //end_for

    if (bn.NodeNum - 1 != i)
      Console.err.println("方法\"CPT_FromFile\"中存在错误！Error: bn.NodeNum - 1 != i")

    inFile.close()
  }









  /** 从文件读取BN对象 */
  def ObjectFromFile(fileName: String): BayesianNetwork = {

    val inFile = new ObjectInputStream(new FileInputStream(fileName))
    val inObject = inFile.readObject.asInstanceOf[BayesianNetwork]      //从文件读入对象存储于inObject中，bj.asInstanceOf[T]看成(T)obj
    inFile.close()
    inObject
  }

}