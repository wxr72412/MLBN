/**
  * Created by Gao on 2016/6/12.
  */
package BNLV_learning

import BNLV_learning.Global.{SparkConf, inputFile, reference, separator}
import BNLV_learning.Output.BN_Output

import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._

class M_table {                                      //M_ijk表的格式
  var tag: String = _                                //M_ijk的标号
  var value: Double = _                              //M_ijk的权重
}

class BayesianNetwork(val NodeNum: Int) extends Serializable {

	val theta = new Array[Array[Array[Double]]](NodeNum)        //定义一个三维可变数组theta——其中的每个二维数组对应一个顶点的条件概率表CPT
	val M_ijk = new Array[Array[Array[Double]]](NodeNum)        //定义一个三维可变数组M_ijk——其中的每个二维数组对应一个顶点的充分统计量

	var mendedData_flag = 1
	var Q_BIC = new Array[Double](NodeNum)        //每个节点的期望BIC评分
	var likelihood:Double = _
	var penalty:Double = _
	var change_node_num:Int = _
	val change_node = new Array[Int](2)

	val structure = Array.ofDim[Int](NodeNum, NodeNum)          //邻接矩阵——表示BN的结构
	val r = new Array[Int](NodeNum)                             //存放BN中每个变量的势ri
	val q = new Array[Int](NodeNum)                             //存放BN中每个变量其父节点的组合情况数qi
	val l = new Array[String](NodeNum)                             //存放BN中每个变量的标记 U L I R
	val latent = new Array[Int](NodeNum)                             //存放BN中每个变量的标记 U L I R
  /*******************根据数据集计算每个显变量的势********************/
  /* 下述方法不可行，并行map中的普通累加变量在action操作执行完毕后即清空
  def ri_computation(primitiveData: String, separator: String){

    val ovariable_num = NodeNum - 1                             //显变量的个数
    val rn = new Array[ArrayBuffer[Int]](ovariable_num)        //数组元素为可变数组的数组rn
    for (i <- 0 until ovariable_num)
      rn(i) = new ArrayBuffer[Int]

    val inFile = SparkConf.sc.textFile(primitiveData) //读取原始数据为RDD
    val primitiveSample = inFile map {                //对原始数据文档primitiveData中的每一行进行处理
      line =>                                         //读取原始文档中的一行数据到line中
        val s_int = line.split(separator).map( _.toInt )     //存储s[i]中各变量的整数形式,如：s[i]="3 1 2"——s_int[0]=3, s_int[1]=1, s_int[2]=2
        for (i <- 0 until ovariable_num) {
          var repeat = false
          breakable {
            for(temp <- rn(i))
              if (s_int(i) == temp){
                repeat = true
                break()
              }
          }
          if (!repeat) {          //若第i个变量相应的向量中不存在与s_int相等的值,将该值存入rn(i)
            print(repeat + "\t" +  s_int(i))
            println()
            rn(i) += s_int(i)
            print("rn: " + i + "\t" +  rn(i))
            println()
          }
        }
    }
    primitiveSample.count()      //action操作——执行map中的所有操作，并统计行数
    for (i <- 0 until ovariable_num){
      rn(i).foreach(t => print(t + "\t"))
      println("*****************")
    }

    for (i <- 0 until ovariable_num)
      r(i) = rn(i).length
  }
  */

	/** 根据模型的结构与r[]计算每个节点的父节点组合情况数q[] */
	def qi_computation() {
		var temp = 1
		for (i <- 0 until NodeNum) {
			temp = 1
			for (j <- 0 until NodeNum)
				if (structure(j)(i) == 1)   //j是i的父节点
					temp *= r(j)
			if(temp == 1)                 //第i+1个节点无父节点
				q(i) = 1
			else
				q(i) = temp
		}
	}

	/** 创建CPT */
	def create_CPT() {
		for (i <- 0 until NodeNum) {       //开辟存放第i个变量CPT的二维数组空间
			theta(i) = new Array[Array[Double]](q(i))
			for (j <- 0 until q(i))
				theta(i)(j) = new Array[Double](r(i))     //theta[i][j][k]==θijk
		}
	}
	/** 创建CPT */
	def create_M_ijk() {
		for (i <- 0 until NodeNum) {       //开辟存放第i个变量CPT的二维数组空间
			M_ijk(i) = new Array[Array[Double]](q(i))
			for (j <- 0 until q(i))
				M_ijk(i)(j) = new Array[Double](r(i))     //theta[i][j][k]==θijk
		}
	}

	def create_CPT_F() {
		for (i <- 0 until change_node_num) {       //开辟存放第i个变量CPT的二维数组空间
			theta(i) = new Array[Array[Double]](q(change_node(i)))
			for (j <- 0 until q(change_node(i)))
				theta(i)(j) = new Array[Double](r(change_node(i)))     //theta[i][j][k]==θijk
		}
	}
	/** 创建CPT */
	def create_M_ijk_F() {
		for (i <- 0 until change_node_num) {       //开辟存放第i个变量CPT的二维数组空间
			M_ijk(i) = new Array[Array[Double]](q(change_node(i)))
			for (j <- 0 until q(change_node(i)))
				M_ijk(i)(j) = new Array[Double](r(change_node(i)))    //theta[i][j][k]==θijk
		}
	}

  /** 生成随机初始参数 */
  def RandomTheta() {
    val RAND_MAX = 32767
    val random = new Random()
    //random.setSeed(0)
    var randomValue = 0.0
    var temp = 0.0

    for (i <-0 until NodeNum)
      for (j <- 0 until q(i)) {
        temp = 0.0
        //random.nextInt(RAND_MAX)产生0~RAND_MAX之间的伪随机整数，random.nextDouble()产生0.0~1.0之间的伪随机浮点数
        randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) //0 < 伪随机数 < 1
        theta(i)(j)(0) = randomValue
        temp += theta(i)(j)(0)
        for (k <- 1 until r(i) - 1) {
          randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * (1 - temp); //0 < 伪随机数 < 1 - temp
          theta(i)(j)(k) = randomValue
          temp += theta(i)(j)(k)
        }
        theta(i)(j)(r(i) - 1) = 1 - temp
      }
  }

//  /** 生成弱约束一随机初始参数 */
//  //该方法隐变量的节点序号为3，当此序号有变化时，下述方法需要修改
//  def RandomTheta_WeakConstraint1() {                          //约束随机生成p(I|L)
//    val RAND_MAX = 32767
//    var temp = 0.0
//    val random = new Random()
//    //random.setSeed(0)
//    var randomValue = 0.0
//
//    for (i <- 0 until NodeNum) {
//      if (i != 2) {
//        for (j <- 0 until q(i)) {
//          temp = 0.0
//          randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2)   //0 < 伪随机数 < 1
//          theta(i)(j)(j) = randomValue
//          temp += theta(i)(j)(j)
//          for (k <- 1 until r(i) - 1) {  //rand()函数产生0~RAND_MAX之间的伪随机整数
//            randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * (1 - temp) //0 < 伪随机数 < 1 - temp
//            theta(i)(j)(k) = randomValue
//            temp += theta(i)(j)(k)
//          }
//          theta(i)(j)(r(i) - 1) = 1 - temp
//        }
//      }
//      else {
//        for (j <- 0 until q(i)) {
//          temp = 0.0
//          randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) //0 < 伪随机数 < 1
//          theta(i)(j)(j) = randomValue                                           //先随机生成p(I=j|L=j)
//          temp += theta(i)(j)(j)
//          for (k <- 0 until r(i)) {    //rand()函数产生0~RAND_MAX之间的伪随机整数
//            if (k != j) {
//              if ((1 - temp) > theta(i)(j)(j)) {  //(1-temp) > p(I=j|L=j)
//                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * theta(i)(j)(j) //0 < 伪随机数 < p(I=j|L=j)
//                theta(i)(j)(k) = randomValue
//                temp += theta(i)(j)(k)
//              }
//              else {    //(1-temp) <= p(I=j|L=j)
//                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * (1 - temp) //0 < 伪随机数 < 1 - temp
//                theta(i)(j)(k) = randomValue
//                temp += theta(i)(j)(k)
//              }
//            }
//          }
//          theta(i)(j)(j) += (1 - temp)   //将分配剩余的概率值补给p(I=j|L=j)
//        }
//      }
//    }
//  }

def RandomTheta(bn: BayesianNetwork) {
	val RAND_MAX = 128
	var temp = 0.0
	val random = new Random()
	var randomValue = 0.0
	var sum = 0.0
	for (i <- 0 until NodeNum) {
		for (j <- 0 until q(i)) {
			sum = 0.0
			for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
				randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
				sum += randomValue
				theta(i)(j)(k) = randomValue
			}
			for (k <- 0 until r(i)) {
				// 随机数
				theta(i)(j)(k) = theta(i)(j)(k) / sum
//				// 平均数
//				theta(i)(j)(k) = 1.0 / r(i)
				//temp += theta(i)(j)(k)
			}
		}
	}
}

	def AveregeTheta(bn: BayesianNetwork) {
		for (i <- 0 until NodeNum) {
			for (j <- 0 until q(i)) {
				for (k <- 0 until r(i)) {
					// 平均数
					theta(i)(j)(k) = 1.0 / r(i)
				}
			}
		}
	}

def RandomTheta_WeakConstraint1(bn: BayesianNetwork) {
	val RAND_MAX = 128
	var temp = 0.0
	val random = new Random()
	var randomValue = 0.0
	var sum = 0.0
	for (i <- 0 until NodeNum) {
		if (bn.l(i) == "L") {
			for (j <- 0 until q(i)) {
				sum = 0.0
				temp = 0.0
				var randomValue_ArrayBuffer = new ArrayBuffer[Double]
				randomValue_ArrayBuffer.clear()
				for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
					randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
					randomValue_ArrayBuffer += randomValue
					sum += randomValue
				}
				for (k <- 0 until r(i)) {
					if (j % 3 == ((k+1) % 3) ) { // n1 >= n2
						theta(i)(j)(k) = randomValue_ArrayBuffer.max / sum
						randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
					} else{// if (j % 3 != k % 3) {
						theta(i)(j)(k) = randomValue_ArrayBuffer.min / sum
						randomValue_ArrayBuffer -= randomValue_ArrayBuffer.min
					}
				}
			}
		}
		else{
			for (j <- 0 until q(i)) {
				sum = 0.0
				for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
					randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
					sum += randomValue
					theta(i)(j)(k) = randomValue
				}
				for (k <- 0 until r(i)) {
					// 随机数
					theta(i)(j)(k) = theta(i)(j)(k) / sum
					// 平均数
//					theta(i)(j)(k) = 1.0 / r(i)
					//temp += theta(i)(j)(k)
				}
			}
		}
	}
}

	def RandomTheta_WeakConstraint2(bn: BayesianNetwork) {
		val inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
		val user_Attribution_num = bn.r(0)*bn.r(1)*bn.r(2)
//		println(user_Attribution_num)

		val RAND_MAX = 128
		var temp = 0.0
		val random = new Random()
		var randomValue = 0.0
		var sum = 0.0
		for (i <- 0 until NodeNum) {
			if (bn.l(i) == "LL") {
				/////////////////////////统计 某一用户属性组合  对应某一电影类型   某一特定取值的平均分/////////////////////////////
				var I = i - reference.LatentNum
				// 统计每个用户组合  对应的18个电影类型的5种评分的数量
				var user_Rating_Count = new Array[Array[Array[Int]]](user_Attribution_num)
				for(ii <- 0 until user_Attribution_num){
					user_Rating_Count(ii) = new Array[Array[Int]](bn.r(I))   //某一用户属性组合  对应某一电影类型
					for(jj <- 0 until bn.r(I)){
						user_Rating_Count(ii)(jj) = new Array[Int](bn.r(3))   //某一用户属性组合  对应某一电影类型   某一特定取值的平均分
						for(kk <- 0 until bn.r(3)){
							user_Rating_Count(ii)(jj)(kk) = 0
						}
					}
				}
				val inFile_collect = inFile.collect()
				for(line <- inFile_collect){
					val lineStringArray = line.split(separator.data).map(_.toInt)
					val ii = (lineStringArray(0)-1)*bn.r(1)*bn.r(2) + (lineStringArray(1)-1)*bn.r(2) + (lineStringArray(2)-1)
					val jj = lineStringArray(I)-1
					val kk = lineStringArray(3)-1
					user_Rating_Count(ii)(jj)(kk) += 1
					//			println(ii + " " + jj + " " + kk + " " + user_Rating_Count(ii)(jj)(kk))
				}

				//某一用户属性组合  对应某一电影类型   某一特定取值的评分数量
				var user_Rating_Frequency = new Array[Array[Int]](user_Attribution_num)
				for(ii <- 0 until user_Attribution_num){
					user_Rating_Frequency(ii) = new Array[Int](bn.r(I))
					for(jj <- 0 until bn.r(I)){
						user_Rating_Frequency(ii)(jj) = 0
						for(kk <- 0 until bn.r(3)){
							user_Rating_Frequency(ii)(jj) += user_Rating_Count(ii)(jj)(kk)
						}
						//				println(i + " " + j + " " + user_Rating_Frequency(i)(j))
					}
				}
				// 统计 某一用户属性组合  对应某一电影类型   某一特定取值的平均分
				var user_Rating_Average = new Array[Array[Double]](user_Attribution_num)
				for(ii <- 0 until user_Attribution_num){
					user_Rating_Average(ii) = new Array[Double](bn.r(I))
					for(jj <- 0 until bn.r(I)){
						user_Rating_Average(ii)(jj) = 0.0
						for(kk <- 0 until bn.r(3)){
							user_Rating_Average(ii)(jj) += user_Rating_Count(ii)(jj)(kk)*(kk+1)
						}
						if(user_Rating_Frequency(ii)(jj) != 0)
							user_Rating_Average(ii)(jj) /= user_Rating_Frequency(ii)(jj)
						//				println(i + " " + j + " " + user_Rating_Average(i)(j))
					}
				}
				//		System.exit(0)
				/////////////////////////////////////////////////////////////////////////////////////
				for (j <- 0 until q(i)) {
					sum = 0.0
//					val genre:Int = j % bn.r(i)
					for (k <- 0 until r(i)) {
						if(user_Rating_Average(j)(k) > 3){  // 平均分大于3 喜欢
							bn.theta(i)(j)(k) = 1.1
//							bn.theta(i)(j)(k) = j + k + r(i)
						}
						else{  // 平均分小于3 不喜欢
							bn.theta(i)(j)(k) = 1.0
//							bn.theta(i)(j)(k) = j + k + 2
						}
						sum += bn.theta(i)(j)(k)
					}
					for (k <- 0 until r(i)) {
						bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
					}
					/////////////////////////////////////////////////////////////////////////////////////
//					sum = 0.0
//					var randomValue_ArrayBuffer = new ArrayBuffer[Double]
//					randomValue_ArrayBuffer.clear()
//					for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
//						randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
//						randomValue_ArrayBuffer += randomValue
//						sum += randomValue
//					}
//					for (k <- 0 until r(i)) {
//						if(user_Rating_Average(j)(k) > 3){  // 平均分大于3 喜欢
//							bn.theta(i)(j)(k) = randomValue_ArrayBuffer.max / sum
//							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
//						}
//						else{  // 平均分小于3 不喜欢
//							bn.theta(i)(j)(k) = randomValue_ArrayBuffer.min / sum
//							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.min
//						}
//					}
					/////////////////////////////////////////////////////////////////////////////////////
				}
			}
			else if(bn.l(i) == "II"){
				/////////////////////////////////////////////////////////////////////////////////////
				for (j <- 0 until q(i)) {
					sum = 0.0
					for (k <- 0 until r(i)) {
						if( j%18 == k)
							bn.theta(i)(j)(k) = 1.1
//							bn.theta(i)(j)(k) = j + k + r(i)
						else
							bn.theta(i)(j)(k) = 1.0
//							bn.theta(i)(j)(k) = j + k + 1
						sum += bn.theta(i)(j)(k)
					}
					for (k <- 0 until r(i)) {
						bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
					}
				}
				/////////////////////////////////////////////////////////////////////////////////////
				//				for (j <- 0 until q(i)) {
				//					sum = 0.0
				//					for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
				//						randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
				//						sum += randomValue
				//						bn.theta(i)(j)(k) = randomValue
				//					}
				//					for (k <- 0 until r(i)) {
				//						// 随机数
				//						bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
				//						// 平均数
				//						//					bn.theta(i)(j)(k) = 1.0 / r(i)
				//						//temp += bn.theta(i)(j)(k)
				//					}
				//				}
				/////////////////////////////////////////////////////////////////////////////////////
//				for (j <- 0 until q(i)) {
//					sum = 0.0
//					val user:Int = j / 18
//					val genre:Int = j % 18
//					if(user_Rating_Average(user)(genre) > 3){
//						bn.theta(i)(j)(0) = 1.0
//						bn.theta(i)(j)(1) = 1.0
//						bn.theta(i)(j)(2) = 1.0
//						bn.theta(i)(j)(3) = 2.0
//						bn.theta(i)(j)(4) = 2.0
//						for (k <- 0 until r(i)) {
//							sum += bn.theta(i)(j)(k)
//						}
//						for (k <- 0 until r(i)) {
//							bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
//						}
//					} else if(user_Rating_Average(user)(genre) > 0){
//						bn.theta(i)(j)(0) = 2.0
//						bn.theta(i)(j)(1) = 2.0
//						bn.theta(i)(j)(2) = 1.0
//						bn.theta(i)(j)(3) = 1.0
//						bn.theta(i)(j)(4) = 1.0
//						for (k <- 0 until r(i)) {
//							sum += bn.theta(i)(j)(k)
//						}
//						for (k <- 0 until r(i)) {
//							bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
//						}
//					}else if(user_Rating_Average(user)(genre) == 0){
//						bn.theta(i)(j)(0) = 1.0
//						bn.theta(i)(j)(1) = 1.0
//						bn.theta(i)(j)(2) = 1.0
//						bn.theta(i)(j)(3) = 1.0
//						bn.theta(i)(j)(4) = 1.0
//						for (k <- 0 until r(i)) {
//							sum += bn.theta(i)(j)(k)
//						}
//						for (k <- 0 until r(i)) {
//							bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
//						}
//					}
//				}
			/////////////////////////////////////////////////////////////////////////////////////
			}
			else if(bn.l(i) == "R"){
				/////////////////////////////////////////////////////////////////////////////////////
				println("初始化R！！！")
				val Ln = reference.LatentNum
				val Lr = new Array[Int](Ln)
				for(i <- 0 until Ln){
					Lr(Ln-1-i) = bn.r(reference.NodeNum-1-i)
				}
				var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
				for(i <- 0 until Ln){
					c *= Lr(i)
				}
				val t = new Array[String](c) //存储一条原始样本的c个补后样本
				var Lx = Array.ofDim[Int](Ln+1)
				var tag_num=0
				while(Lx(0) == 0){
					var paddingdata = ""
					for(i <- 1 until Ln){   //1,2,3,....,Ln
						paddingdata += (Lx(i)+1).toString + separator.data
					}
					t(tag_num) =  paddingdata + (Lx(Ln)+1).toString
					tag_num+=1
					Lx(Ln) += 1
					for(i <- 0 until Ln){   //0,1,2,....,Ln-1
						if( Lx(Ln-i)==Lr(Ln-i-1) ){
							Lx(Ln-i) = 0
							Lx(Ln-i-1) +=1
						}
					}
				}
				var temp = 0.0
				var j = 0
				for( t_I <- t ){
					for( t_L <- t ){
						//println(t_I + " " + t_L)
						//					1,1 1,1
						//					1,1 1,2
						//					1,1 1,3
						var equal_num = 0 //偏好与item属性维度相同的个数
						val t_I_Array = t_I.split(",")
						val t_L_Array = t_L.split(",")
						//t_I_Array.zip(t_L_Array).foreach(println(_))
						//					(1,1) (1,1)
						//					(1,1) (1,2)
						//					(1,1) (1,3)
						for (t_I_L <- t_I_Array.zip(t_L_Array)){
							if(t_I_L._1 == t_I_L._2)
								equal_num += 1
						}
						//println(equal_num)
						//					2
						//					1
						//					1
						var sum = 0.0
						temp = 0.0
						/////////////////////////////////////////////////////////////////////////////////////
//						if (equal_num == Ln) { // n1 >= n2
//							reference.miu = 6.0
//						}
//						else if (equal_num == 0) { // n1 >= n2
//							reference.miu = 0.0
//						}
//						for (k <- 0 until r(i)) {
//							var x = (scala.util.Random.nextInt(1000) - 500) / 1000.0 + k + 1
//							var y = (  1/((math.sqrt(2*Math.PI))*reference.sigma)  ) * math.exp( -1 * ((x-reference.miu)*(x-reference.miu))/(2*reference.sigma*reference.sigma) )
//							bn.theta(i)(j)(k) = y
//							sum += y
//						}
//						for (k <- 0 until r(i)) {
//							bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
//						}
						/////////////////////////////////////////////////////////////////////////////////////
						if (equal_num == Ln) { // n1 >= n2
							bn.theta(i)(j)(0) = 0.05
							bn.theta(i)(j)(1) = 0.14
							bn.theta(i)(j)(2) = 0.21
							bn.theta(i)(j)(3) = 0.25
							bn.theta(i)(j)(4) = 0.35
							for (k <- 0 until r(i)) {
								sum += bn.theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
							}
						}
						else if (equal_num == 0) { // n1 >= n2
							bn.theta(i)(j)(0) = 0.36
							bn.theta(i)(j)(1) = 0.26
							bn.theta(i)(j)(2) = 0.19
							bn.theta(i)(j)(3) = 0.14
							bn.theta(i)(j)(4) = 0.05
							for (k <- 0 until r(i)) {
								sum += bn.theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
							}
						}
						else{ // n1 < n2
							for (k <- 0 until r(i)) {
								bn.theta(i)(j)(k) = 1.0 / r(i)
								//								bn.theta(i)(j)(k) = 1.0+j+k / r(i)
							}
						}
						////////////////////////////////////////////////////////////////////////////////////
						/////////////////////////////////////////////////////////////////////////////////////
						j += 1
					}
				}
//				/////////////////////////////////////////////////////////////////////////////////////
//				for (j <- 0 until q(i)) {
//					sum = 0.0
//					val user:Int = j / 18
//					val genre:Int = j % 18
//					if(user_Rating_Average(user)(genre) > 3){  // 喜欢
//						bn.theta(i)(j)(0) = 1.0
//						bn.theta(i)(j)(1) = 1.0
//						bn.theta(i)(j)(2) = 1.0
//						bn.theta(i)(j)(3) = 1.1
//						bn.theta(i)(j)(4) = 1.1
//						for (k <- 0 until r(i)) {
//							sum += bn.theta(i)(j)(k)
//						}
//						for (k <- 0 until r(i)) {
//							bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
//						}
//					}else if(user_Rating_Average(user)(genre) > 0){// 不喜欢
//						bn.theta(i)(j)(0) = 1.1
//						bn.theta(i)(j)(1) = 1.1
//						bn.theta(i)(j)(2) = 1.0
//						bn.theta(i)(j)(3) = 1.0
//						bn.theta(i)(j)(4) = 1.0
//						for (k <- 0 until r(i)) {
//							sum += bn.theta(i)(j)(k)
//						}
//						for (k <- 0 until r(i)) {
//							bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
//						}
//					}else if(user_Rating_Average(user)(genre) == 0){
//						for (k <- 0 until r(i)) {
//							bn.theta(i)(j)(k) = 1.0 / r(i)
//						}
//					}
//				}
//				/////////////////////////////////////////////////////////////////////////////////////
////				for (j <- 0 until q(i)) {
////					sum = 0.0
////					temp = 0.0
////					var randomValue_ArrayBuffer = new ArrayBuffer[Double]
////					randomValue_ArrayBuffer.clear()
////					for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
////						randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
////						randomValue_ArrayBuffer += randomValue
////						sum += randomValue
////					}
////					val user:Int = j / 18
////					val genre:Int = j % 18
////					if(user_Rating_Average(user)(genre) > 3){  // 喜欢
////						val index = (random.nextInt(2) + 3) // 0,1  +3   随机生成3、4之中一个整数
////						if (index == 4) {
////							bn.theta(i)(j)(4) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(4)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////							bn.theta(i)(j)(3) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(3)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////						} else {
////							bn.theta(i)(j)(3) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(3)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////							bn.theta(i)(j)(4) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(4)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////						}
////						for (k <- 0 to 1) {
////							bn.theta(i)(j)(k) = randomValue_ArrayBuffer(k) / sum
////							temp += bn.theta(i)(j)(k)
////						}
////						bn.theta(i)(j)(2) = 1 - temp
////					}else if(user_Rating_Average(user)(genre) > 0){// 不喜欢
////						val index = (random.nextInt(2)) // 0,1  +0   随机生成0、1之中一个整数
////						if (index == 0) {
////							bn.theta(i)(j)(0) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(0)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////							bn.theta(i)(j)(1) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(1)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////						} else {
////							bn.theta(i)(j)(1) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(1)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////							bn.theta(i)(j)(0) = randomValue_ArrayBuffer.max / sum
////							temp += bn.theta(i)(j)(0)
////							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
////						}
////						for (k <- 3 to 4) {
////							bn.theta(i)(j)(k) = randomValue_ArrayBuffer(k-3) / sum
////							temp += bn.theta(i)(j)(k)
////						}
////						bn.theta(i)(j)(2) = 1 - temp
////					}else if(user_Rating_Average(user)(genre) == 0){
////						for (k <- 0 until r(i)) {
////							bn.theta(i)(j)(k) = 1.0 / r(i)
////						}
////					}
////				}
//				/////////////////////////////////////////////////////////////////////////////////////
			}
			else{
				for (j <- 0 until q(i)) {
					sum = 0.0
					for (k <- 0 until r(i)) {
						// 随机数
//						bn.theta(i)(j)(k) = bn.theta(i)(j)(k) / sum
						// 平均数
						bn.theta(i)(j)(k) = 1.0 / r(i)
						//temp += bn.theta(i)(j)(k)
					}
				}
			}
		}
	}


	def RandomTheta_WeakConstraint22(bn: BayesianNetwork) {
		val inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
		val user_Attribution_num = bn.r(0)*bn.r(1)*bn.r(2)
		//		println(user_Attribution_num)

		val RAND_MAX = 128
		var temp = 0.0
		val random = new Random()
		var randomValue = 0.0
		var sum = 0.0
		for (i <- 0 until NodeNum) {
			if (bn.l(i) == "LL") {
				/////////////////////////统计 某一用户属性组合  对应某一电影类型   某一特定取值的平均分/////////////////////////////
				var I = i - reference.LatentNum
				// 统计每个用户组合  对应的18个电影类型的5种评分的数量
				var user_Rating_Count = new Array[Array[Array[Int]]](user_Attribution_num)
				for(ii <- 0 until user_Attribution_num){
					user_Rating_Count(ii) = new Array[Array[Int]](bn.r(I))   //某一用户属性组合  对应某一电影类型
					for(jj <- 0 until bn.r(I)){
						user_Rating_Count(ii)(jj) = new Array[Int](bn.r(3))   //某一用户属性组合  对应某一电影类型   某一特定取值的平均分
						for(kk <- 0 until bn.r(3)){
							user_Rating_Count(ii)(jj)(kk) = 0
						}
					}
				}
				val inFile_collect = inFile.collect()
				for(line <- inFile_collect){
					val lineStringArray = line.split(separator.data).map(_.toInt)
					val ii = (lineStringArray(0)-1)*bn.r(1)*bn.r(2) + (lineStringArray(1)-1)*bn.r(2) + (lineStringArray(2)-1)
					val jj = lineStringArray(I)-1
					val kk = lineStringArray(3)-1
					user_Rating_Count(ii)(jj)(kk) += 1
					//			println(ii + " " + jj + " " + kk + " " + user_Rating_Count(ii)(jj)(kk))
				}

				//某一用户属性组合  对应某一电影类型   某一特定取值的评分数量
				var user_Rating_Frequency = new Array[Array[Int]](user_Attribution_num)
				for(ii <- 0 until user_Attribution_num){
					user_Rating_Frequency(ii) = new Array[Int](bn.r(I))
					for(jj <- 0 until bn.r(I)){
						user_Rating_Frequency(ii)(jj) = 0
						for(kk <- 0 until bn.r(3)){
							user_Rating_Frequency(ii)(jj) += user_Rating_Count(ii)(jj)(kk)
						}
						//				println(i + " " + j + " " + user_Rating_Frequency(i)(j))
					}
				}
				// 统计 某一用户属性组合  对应某一电影类型   某一特定取值的平均分
				var user_Rating_Average = new Array[Array[Double]](user_Attribution_num)
				for(ii <- 0 until user_Attribution_num){
					user_Rating_Average(ii) = new Array[Double](bn.r(I))
					for(jj <- 0 until bn.r(I)){
						user_Rating_Average(ii)(jj) = 0.0
						for(kk <- 0 until bn.r(3)){
							user_Rating_Average(ii)(jj) += user_Rating_Count(ii)(jj)(kk)*(kk+1)
						}
						if(user_Rating_Frequency(ii)(jj) != 0)
							user_Rating_Average(ii)(jj) /= user_Rating_Frequency(ii)(jj)
						//				println(i + " " + j + " " + user_Rating_Average(i)(j))
					}
				}
				for (j <- 0 until q(i)) {
					sum = 0.0
					//					val genre:Int = j % bn.r(i)
					for (k <- 0 until r(i)) {
						if(user_Rating_Average(j)(k) > 3){  // 平均分大于3 喜欢
							theta(i)(j)(k) = 1.1
							//							theta(i)(j)(k) = j + k + r(i)
						}
						else{  // 平均分小于3 不喜欢
							theta(i)(j)(k) = 1.0
							//							theta(i)(j)(k) = j + k + 2
						}
						sum += theta(i)(j)(k)
					}
					for (k <- 0 until r(i)) {
						theta(i)(j)(k) = theta(i)(j)(k) / sum
					}
				}
			}
			else if(bn.l(i) == "II"){
				/////////////////////////////////////////////////////////////////////////////////////
//				for (j <- 0 until q(i)) {
//					sum = 0.0
//					for (k <- 0 until r(i)) {
//						if( j == k)
//							theta(i)(j)(k) = 1.1
//						//							theta(i)(j)(k) = j + k + r(i)
//						else
//							theta(i)(j)(k) = 1.0
//						//							theta(i)(j)(k) = j + k + 1
//						sum += theta(i)(j)(k)
//					}
//					for (k <- 0 until r(i)) {
//						theta(i)(j)(k) = theta(i)(j)(k) / sum
//					}
//				}
				/////////////////////////////////////////////////////////////////////////////////////
				for (j <- 0 until q(i)) {
					sum = 0.0
					var max = 0.0
					for (k <- 0 until r(i)) {
						if(max < theta(i)(j)(k))
							max = theta(i)(j)(k)
					}
					if (theta(i)(j)(j%18) < max)
						theta(i)(j)(j%18) = max  + 1.0/r(i)
					for (k <- 0 until r(i)) {
						sum += theta(i)(j)(k)
					}
					for (k <- 0 until r(i)) {
						theta(i)(j)(k) = theta(i)(j)(k) / sum
					}
				}
				/////////////////////////////////////////////////////////////////////////////////////
			}
			else if(bn.l(i) == "R"){
				/////////////////////////////////////////////////////////////////////////////////////
				val Ln = reference.LatentNum
				val Lr = new Array[Int](Ln)
				for(i <- 0 until Ln){
					Lr(Ln-1-i) = bn.r(reference.NodeNum-1-i)
				}
				var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
				for(i <- 0 until Ln){
					c *= Lr(i)
				}
				val t = new Array[String](c) //存储一条原始样本的c个补后样本
				var Lx = Array.ofDim[Int](Ln+1)
				var tag_num=0
				while(Lx(0) == 0){
					var paddingdata = ""
					for(i <- 1 until Ln){   //1,2,3,....,Ln
						paddingdata += (Lx(i)+1).toString + separator.data
					}
					t(tag_num) =  paddingdata + (Lx(Ln)+1).toString
					tag_num+=1
					Lx(Ln) += 1
					for(i <- 0 until Ln){   //0,1,2,....,Ln-1
						if( Lx(Ln-i)==Lr(Ln-i-1) ){
							Lx(Ln-i) = 0
							Lx(Ln-i-1) +=1
						}
					}
				}
				var temp = 0.0
				var j = 0
				for( t_I <- t ){
					for( t_L <- t ){
						//println(t_I + " " + t_L)
						//					1,1 1,1
						//					1,1 1,2
						//					1,1 1,3
						var equal_num = 0 //偏好与item属性维度相同的个数
						val t_I_Array = t_I.split(",")
						val t_L_Array = t_L.split(",")
						//t_I_Array.zip(t_L_Array).foreach(println(_))
						//					(1,1) (1,1)
						//					(1,1) (1,2)
						//					(1,1) (1,3)
						for (t_I_L <- t_I_Array.zip(t_L_Array)){
							if(t_I_L._1 == t_I_L._2)
								equal_num += 1
						}
						//println(equal_num)
						//					2
						//					1
						//					1
						var sum = 0.0
						temp = 0.0
						/////////////////////////////////////////////////////////////////////////////////////
						if (equal_num == Ln) { // n1 >= n2
							theta(i)(j)(0) = 0.05
							theta(i)(j)(1) = 0.14
							theta(i)(j)(2) = 0.21
							theta(i)(j)(3) = 0.25
							theta(i)(j)(4) = 0.35
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else if (equal_num == 0) { // n1 >= n2
							theta(i)(j)(0) = 0.36
							theta(i)(j)(1) = 0.26
							theta(i)(j)(2) = 0.19
							theta(i)(j)(3) = 0.14
							theta(i)(j)(4) = 0.05
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else{ // n1 < n2
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = 1.0 / r(i)
								//								theta(i)(j)(k) = 1.0+j+k / r(i)
							}
						}
						j += 1
					}
				}
			}
		}
	}


	def RandomTheta_WeakConstraint222(bn: BayesianNetwork) {

		val RAND_MAX = 128
		var temp = 0.0
		val random = new Random()
		var randomValue = 0.0
		var sum = 0.0
		for (i <- 0 until NodeNum) {
			if (bn.l(i) == "L" || bn.l(i) == "I") {
				for (j <- 0 until q(i)) {
					sum = 0.0
					for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
						randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
						sum += randomValue
						theta(i)(j)(k) = randomValue
					}
					for (k <- 0 until r(i)) {
						// 随机数
						//						theta(i)(j)(k) = theta(i)(j)(k) / sum
						// 平均数
						theta(i)(j)(k) = 1.0 / r(i)
						//temp += theta(i)(j)(k)
					}
				}
			}
			else if(bn.l(i) == "R"){
				/////////////////////////////////////////////////////////////////////////////////////
				val Ln = reference.LatentNum
				val Lr = new Array[Int](Ln)
				for(i <- 0 until Ln){
					Lr(Ln-1-i) = bn.r(reference.NodeNum-1-i)
				}
				var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
				for(i <- 0 until Ln){
					c *= Lr(i)
				}
				val t = new Array[String](c) //存储一条原始样本的c个补后样本
				var Lx = Array.ofDim[Int](Ln+1)
				var tag_num=0
				while(Lx(0) == 0){
					var paddingdata = ""
					for(i <- 1 until Ln){   //1,2,3,....,Ln
						paddingdata += (Lx(i)+1).toString + separator.data
					}
					t(tag_num) =  paddingdata + (Lx(Ln)+1).toString
					tag_num+=1
					Lx(Ln) += 1
					for(i <- 0 until Ln){   //0,1,2,....,Ln-1
						if( Lx(Ln-i)==Lr(Ln-i-1) ){
							Lx(Ln-i) = 0
							Lx(Ln-i-1) +=1
						}
					}
				}
				var temp = 0.0
				var j = 0
				for( t_I <- t ){
					for( t_L <- t ){
						//println(t_I + " " + t_L)
						//					1,1 1,1
						//					1,1 1,2
						//					1,1 1,3
						var equal_num = 0 //偏好与item属性维度相同的个数
						val t_I_Array = t_I.split(",")
						val t_L_Array = t_L.split(",")
						//t_I_Array.zip(t_L_Array).foreach(println(_))
						//					(1,1) (1,1)
						//					(1,1) (1,2)
						//					(1,1) (1,3)
						for (t_I_L <- t_I_Array.zip(t_L_Array)){
							if(t_I_L._1 == t_I_L._2)
								equal_num += 1
						}
						//println(equal_num)
						//					2
						//					1
						//					1
						var sum = 0.0
						temp = 0.0
						/////////////////////////////////////////////////////////////////////////////////////
						if (equal_num == Ln) { // n1 >= n2
							theta(i)(j)(0) = 0.16
							theta(i)(j)(1) = 0.18
							theta(i)(j)(2) = 0.21
							theta(i)(j)(3) = 0.22
							theta(i)(j)(4) = 0.23
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else if (equal_num == 0) { // n1 >= n2
							theta(i)(j)(0) = 0.24
							theta(i)(j)(1) = 0.22
							theta(i)(j)(2) = 0.19
							theta(i)(j)(3) = 0.18
							theta(i)(j)(4) = 0.17
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else{ // n1 < n2
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = 1.0 / r(i)
								//								theta(i)(j)(k) = 1.0+j+k / r(i)
							}
						}
						j += 1
					}
				}
			}
		}
	}


	def RandomTheta_WeakConstraintR(bn: BayesianNetwork) {
		val inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
		val user_Attribution_num = bn.r(0)*bn.r(1)*bn.r(2)
		//		println(user_Attribution_num)

		val RAND_MAX = 128
		var temp = 0.0
		val random = new Random()
		var randomValue = 0.0
		var sum = 0.0
		for (i <- 0 until NodeNum) {
			if(bn.l(i) == "R"){
				/////////////////////////////////////////////////////////////////////////////////////
				val Ln = 1
				val Lr = new Array[Int](1)
				Lr(0) = bn.r(i + reference.LatentNum)
				var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
				for(i <- 0 until Ln){
					c *= Lr(i)
				}
				val t = new Array[String](c) //存储一条原始样本的c个补后样本
				var Lx = Array.ofDim[Int](Ln+1)
				var tag_num=0
				while(Lx(0) == 0){
					var paddingdata = ""
					for(i <- 1 until Ln){   //1,2,3,....,Ln
						paddingdata += (Lx(i)+1).toString + separator.data
					}
					t(tag_num) =  paddingdata + (Lx(Ln)+1).toString
					tag_num+=1
					Lx(Ln) += 1
					for(i <- 0 until Ln){   //0,1,2,....,Ln-1
						if( Lx(Ln-i)==Lr(Ln-i-1) ){
							Lx(Ln-i) = 0
							Lx(Ln-i-1) +=1
						}
					}
				}
				var temp = 0.0
				var j = 0
				for( t_I <- t ){
					for( t_L <- t ){
						var equal_num = 0 //偏好与item属性维度相同的个数
						val t_I_Array = t_I.split(",")
						val t_L_Array = t_L.split(",")
						for (t_I_L <- t_I_Array.zip(t_L_Array)){
							if(t_I_L._1 == t_I_L._2)
								equal_num += 1
						}
						var sum = 0.0
						temp = 0.0
						/////////////////////////////////////////////////////////////////////////////////////
						if (equal_num == Ln) { // n1 >= n2
							theta(i)(j)(0) = 0.16
							theta(i)(j)(1) = 0.18
							theta(i)(j)(2) = 0.21
							theta(i)(j)(3) = 0.22
							theta(i)(j)(4) = 0.23
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else if (equal_num == 0) { // n1 >= n2
							theta(i)(j)(0) = 0.24
							theta(i)(j)(1) = 0.22
							theta(i)(j)(2) = 0.19
							theta(i)(j)(3) = 0.18
							theta(i)(j)(4) = 0.17
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else{ // n1 < n2
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = 1.0 / r(i)
								//								theta(i)(j)(k) = 1.0+j+k / r(i)
							}
						}
						////////////////////////////////////////////////////////////////////////////////////
						/////////////////////////////////////////////////////////////////////////////////////
						j += 1
					}
				}
			}
			else{
				for (j <- 0 until q(i)) {
					sum = 0.0
					for (k <- 0 until r(i)) { //rand()函数产生0~RAND_MAX之间的伪随机整数
						randomValue = (random.nextInt(RAND_MAX) + 1).toDouble //1 <= 伪随机数 <= (RAND_MAX) = 32767
						sum += randomValue
						theta(i)(j)(k) = randomValue
					}
					for (k <- 0 until r(i)) {
						// 随机数
						//						theta(i)(j)(k) = theta(i)(j)(k) / sum
						// 平均数
						theta(i)(j)(k) = 1.0 / r(i)
						//temp += theta(i)(j)(k)
					}
				}
			}
		}
	}


	def RandomTheta_WeakConstraintRR(bn: BayesianNetwork) {
		val inFile = SparkConf.sc.textFile(inputFile.primitiveData, SparkConf.partitionNum)  //读取原始数据为RDD
		val user_Attribution_num = bn.r(0)*bn.r(1)*bn.r(2)
		//		println(user_Attribution_num)

		val RAND_MAX = 128
		var temp = 0.0
		val random = new Random()
		var randomValue = 0.0
		var sum = 0.0
		for (i <- 0 until NodeNum) {
			if(bn.l(i) == "R"){
				/////////////////////////////////////////////////////////////////////////////////////
				val Ln = 1
				val Lr = new Array[Int](Ln)
				for(i <- 0 until Ln){
					Lr(Ln-1-i) = bn.r(i + 1)
				}
				var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
				for(i <- 0 until Ln){
					c *= Lr(i)
				}
				val t = new Array[String](c) //存储一条原始样本的c个补后样本
				var Lx = Array.ofDim[Int](Ln+1)
				var tag_num=0
				while(Lx(0) == 0){
					var paddingdata = ""
					for(i <- 1 until Ln){   //1,2,3,....,Ln
						paddingdata += (Lx(i)+1).toString + separator.data
					}
					t(tag_num) =  paddingdata + (Lx(Ln)+1).toString
					tag_num+=1
					Lx(Ln) += 1
					for(i <- 0 until Ln){   //0,1,2,....,Ln-1
						if( Lx(Ln-i)==Lr(Ln-i-1) ){
							Lx(Ln-i) = 0
							Lx(Ln-i-1) +=1
						}
					}
				}
				var temp = 0.0
				var j = 0
				for( t_I <- t ){
					for( t_L <- t ){
						//println(t_I + " " + t_L)
						//					1,1 1,1
						//					1,1 1,2
						//					1,1 1,3
						var equal_num = 0 //偏好与item属性维度相同的个数
						val t_I_Array = t_I.split(",")
						val t_L_Array = t_L.split(",")
						//t_I_Array.zip(t_L_Array).foreach(println(_))
						//					(1,1) (1,1)
						//					(1,1) (1,2)
						//					(1,1) (1,3)
						for (t_I_L <- t_I_Array.zip(t_L_Array)){
							if(t_I_L._1 == t_I_L._2)
								equal_num += 1
						}
						//println(equal_num)
						//					2
						//					1
						//					1
						var sum = 0.0
						temp = 0.0
						/////////////////////////////////////////////////////////////////////////////////////
						if (equal_num == Ln) { // n1 >= n2
							theta(i)(j)(0) = 0.16
							theta(i)(j)(1) = 0.18
							theta(i)(j)(2) = 0.21
							theta(i)(j)(3) = 0.22
							theta(i)(j)(4) = 0.23
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else if (equal_num == 0) { // n1 >= n2
							theta(i)(j)(0) = 0.24
							theta(i)(j)(1) = 0.22
							theta(i)(j)(2) = 0.19
							theta(i)(j)(3) = 0.18
							theta(i)(j)(4) = 0.17
							for (k <- 0 until r(i)) {
								sum += theta(i)(j)(k)
							}
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = theta(i)(j)(k) / sum
							}
						}
						else{ // n1 < n2
							for (k <- 0 until r(i)) {
								theta(i)(j)(k) = 1.0 / r(i)
								//								theta(i)(j)(k) = 1.0+j+k / r(i)
							}
						}
						j += 1
					}
				}
			}
		}
	}





/** 生成弱约束三随机初始参数 */
//该方法隐变量的节点序号为3，当此序号有变化时，下述方法需要修改
def RandomTheta_WeakConstraint(bn: BayesianNetwork) {
	//val RAND_MAX = 32767
	val RAND_MAX = 128
	var temp = 0.0
	val random = new Random()
	//random.setSeed(0)
	var randomValue = 0.0

	for (i <- 0 until NodeNum) {
		//I节点
		if (bn.l(i) == "I") {  //i==2——约束随机生成p(I|L)
			for (j <- 0 until q(i)) {

				var sum = 0.0
				var randomValue_ArrayBuffer = new ArrayBuffer[Double]
				//var randomValue_ArrayBuffer1 = new ArrayBuffer[Double]
				randomValue_ArrayBuffer.clear()
				//randomValue_ArrayBuffer1.clear()
				for (k <- 0 until r(i)) {   //rand()函数产生0~RAND_MAX之间的伪随机整数
					randomValue = (random.nextInt(RAND_MAX) + 1).toDouble   //1 <= 伪随机数 <= (RAND_MAX) = 32767
					randomValue_ArrayBuffer += randomValue
					//randomValue_ArrayBuffer1 += randomValue
					sum += randomValue
				}

				theta(i)(j)(j) = randomValue_ArrayBuffer.max / sum
				randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max

				var l = 0
				for (k <- 0 until r(i)) {
					if(k != j) {
						theta(i)(j)(k) = randomValue_ArrayBuffer(l) / sum
						//temp += theta(i)(j)(k)
						l += 1
					}
				}
			}
		}
		//R节点
		//r[node_num-1]——隐变量节点L的势——隐变量个数改变时，需要修改
		else if (bn.l(i) == "R") {  //i==3——约束随机生成p(R|I,L)
//		else if (bn.l(i) == "XXX") {  //i==3——约束随机生成p(R|I,L)
			val Ln = reference.LatentNum
			val Lr = new Array[Int](Ln)
			for(i <- 0 until Ln){
				Lr(Ln-1-i) = bn.r(reference.NodeNum-1-i)
			}
			var c = 1  //隐变量取值组合数 = 隐变量的势的乘积
			for(i <- 0 until Ln){
				c *= Lr(i)
			}
			val t = new Array[String](c) //存储一条原始样本的c个补后样本
			var Lx = Array.ofDim[Int](Ln+1)
			var tag_num=0
			while(Lx(0) == 0){
				var paddingdata = ""
				for(i <- 1 until Ln){   //1,2,3,....,Ln
					paddingdata += (Lx(i)+1).toString + separator.data
				}
				t(tag_num) =  paddingdata + (Lx(Ln)+1).toString
				tag_num+=1
				Lx(Ln) += 1
				for(i <- 0 until Ln){   //0,1,2,....,Ln-1
					if( Lx(Ln-i)==Lr(Ln-i-1) ){
						Lx(Ln-i) = 0
						Lx(Ln-i-1) +=1
					}
				}
			}
			var temp = 0.0
			var j = 0
			for( t_I <- t ){
				for( t_L <- t ){
					//println(t_I + " " + t_L)
//					1,1 1,1
//					1,1 1,2
//					1,1 1,3
					var equal_num = 0 //偏好与item属性维度相同的个数
					val t_I_Array = t_I.split(",")
					val t_L_Array = t_L.split(",")
					//t_I_Array.zip(t_L_Array).foreach(println(_))
//					(1,1) (1,1)
//					(1,1) (1,2)
//					(1,1) (1,3)
					for (t_I_L <- t_I_Array.zip(t_L_Array)){
						if(t_I_L._1 == t_I_L._2)
							equal_num += 1
					}
					//println(equal_num)
//					2
//					1
//					1
					var sum = 0.0
					temp = 0.0
					var randomValue_ArrayBuffer = new ArrayBuffer[Double]
					//var randomValue_ArrayBuffer1 = new ArrayBuffer[Double]
					randomValue_ArrayBuffer.clear()
					//randomValue_ArrayBuffer1.clear()
					for (k <- 0 until r(i)) {   //rand()函数产生0~RAND_MAX之间的伪随机整数
						randomValue = (random.nextInt(RAND_MAX) + 1).toDouble   //1 <= 伪随机数 <= (RAND_MAX) = 32767
						randomValue_ArrayBuffer += randomValue
						//randomValue_ArrayBuffer1 += randomValue
						sum += randomValue
					}
					//randomValue_Array.foreach(println(_))
					if (equal_num >= Ln - equal_num) { // n1 >= n2
						//randomValue_ArrayBuffer = randomValue_ArrayBuffer.sorted
//						println(equal_num)
//						println(Ln - equal_num)

						val index = (random.nextInt(2) + 3) // 随机生成3、4之中一个整数
						if(index == 4) {
							theta(i)(j)(4) = randomValue_ArrayBuffer.max / sum
							temp += theta(i)(j)(4)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
							theta(i)(j)(3) = randomValue_ArrayBuffer.max / sum
							temp += theta(i)(j)(3)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
						}else{
							theta(i)(j)(3) = randomValue_ArrayBuffer.max / sum
							temp += theta(i)(j)(3)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
							theta(i)(j)(4) = randomValue_ArrayBuffer.max / sum
							temp += theta(i)(j)(4)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.max
						}
						for (k <- 0 to 1) {
							theta(i)(j)(k) = randomValue_ArrayBuffer(k) / sum
							temp += theta(i)(j)(k)
						}
						theta(i)(j)(2) = 1 - temp
					}else{ // n1 < n2
						val index = (random.nextInt(2) + 3) // 随机生成3、4之中一个整数
						if(index == 4) {
							theta(i)(j)(4) = randomValue_ArrayBuffer.min / sum
							temp += theta(i)(j)(4)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.min
							theta(i)(j)(3) = randomValue_ArrayBuffer.min / sum
							temp += theta(i)(j)(3)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.min
						}else{
							theta(i)(j)(3) = randomValue_ArrayBuffer.min / sum
							temp += theta(i)(j)(3)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.min
							theta(i)(j)(4) = randomValue_ArrayBuffer.min / sum
							temp += theta(i)(j)(4)
							randomValue_ArrayBuffer -= randomValue_ArrayBuffer.min
						}
						for (k <- 0 to 1) {
							theta(i)(j)(k) = randomValue_ArrayBuffer(k) / sum
							temp += theta(i)(j)(k)
						}
						theta(i)(j)(2) = 1 - temp
					}
//					var xxx = 0.0
//					for (k <- 0 to 4) {
//						xxx += theta(i)(j)(k)
//					}
//					println(xxx)
					//System.exit(0)
					j += 1
				}
			}
		}
		//U、L节点
		else {  //bn.l(i) == "U"  bn.l(i) == "L"
			for (j <- 0 until q(i)) {
				var sum = 0.0
				for (k <- 0 until r(i)) {   //rand()函数产生0~RAND_MAX之间的伪随机整数
					randomValue = (random.nextInt(RAND_MAX) + 1).toDouble   //1 <= 伪随机数 <= (RAND_MAX) = 32767
					sum += randomValue
					theta(i)(j)(k) = randomValue
				}
				for (k <- 0 until r(i)) {
					theta(i)(j)(k) = theta(i)(j)(k) / sum
					//temp += theta(i)(j)(k)
				}
			}
		}
	}
}


/** 生成弱约束三随机初始参数 */
//该方法隐变量的节点序号为3，当此序号有变化时，下述方法需要修改
def RandomTheta_WeakConstraint3() {
    val RAND_MAX = 32767
    var temp = 0.0
    val random = new Random()
    //random.setSeed(0)
    var randomValue = 0.0

    for (i <- 0 until NodeNum) {
      if (2 == i) {  //i==2——约束随机生成p(I|L)
        for (j <- 0 until q(i)) {
          temp = 0.0
          randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) //0 < 伪随机数 < 1
          theta(i)(j)(j) = randomValue                         //先随机生成p(I=j|L=j)
          temp += theta(i)(j)(j)
          for (k <- 0 until r(i)) {   //rand()函数产生0~RAND_MAX之间的伪随机整数
            if (k != j) {
              if ((1 - temp) > theta(i)(j)(j)) {  //(1-temp) > p(I=j|L=j)
                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * theta(i)(j)(j)  //0 < 伪随机数 < p(I=j|L=j)
                theta(i)(j)(k) = randomValue
                temp += theta(i)(j)(k)
              }
              else {  //(1-temp) <= p(I=j|L=j)
                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * (1-temp)  //0 < 伪随机数 < 1 - temp
                theta(i)(j)(k) = randomValue
                temp += theta(i)(j)(k)
              }
            }
          }
          theta(i)(j)(j) += (1 - temp)                     //将分配剩余的概率值补给p(I=j|L=j)
        }
      }

      //r[node_num-1]——隐变量节点L的势——隐变量个数改变时，需要修改
      else if (3 == i) {  //i==3——约束随机生成p(R|I,L)
        val PreferenceMatch = new Array[Int](r(NodeNum - 1))  //存储父节点集中的偏好匹配时的序号j值,此处j从0开始
        for (t <- 0 until r(NodeNum - 1))
          PreferenceMatch(t) = t * r(NodeNum - 1) + t

        for (j <- 0 until q(i)) {
          if (equation(j, PreferenceMatch, r(NodeNum - 1)) ) {  //偏好匹配——I=Ci,L=Ci
            temp = 0.0
            var k = 3
            randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2)  //0 < 伪随机数 < 1
            theta(i)(j)(k) = randomValue                                     //先随机生成p(R=4|I,L)
            temp += theta(i)(j)(k)
            k = 4
            randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2)  * (1 - temp)  //0 < 伪随机数 < 1 - temp
            theta(i)(j)(k) = randomValue                                     //先随机生成p(R=5|I,L)
            temp += theta(i)(j)(k)

            val min_temp: Double = min(theta(i)(j)(3), theta(i)(j)(4))
            for (k <- 0 to 2) {
              if ((1 - temp) > min_temp) {  //(1-temp) > min(theta[i][j][3],theta[i][j][4])
                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * min_temp;    //0 < 伪随机数 < min(theta[i][j][3],theta[i][j][4])
                theta(i)(j)(k) = randomValue
                temp += theta(i)(j)(k)
              }
              else {  //(1-temp) <= min(theta[i][j][3],theta[i][j][4])
                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2)  * (1 - temp)  //0 < 伪随机数 < 1 - temp
                theta(i)(j)(k) = randomValue
                temp += theta(i)(j)(k)
              }
            }
            theta(i)(j)(3) += (1 - temp) / 2                                   //将分配剩余的概率值补给p(R=4|x2,L)与p(R=5|x2,L)
            theta(i)(j)(4) += (1 - temp) / 2
          }

          else {  //偏好不匹配——I=Ci,L=Cj
            temp = 0.0
            randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2)  //0 < 伪随机数 < 1
            theta(i)(j)(0) = randomValue                                      //先随机生成p(R=1|I,L)
            temp += theta(i)(j)(0)
            for (k <-1 to 2) {
              randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2)  * (1 - temp)        //0 < 伪随机数 < 1 - temp
              theta(i)(j)(k) = randomValue                                    //随机生成p(R=2|I,L)与p(R=3|I,L)
              temp += theta(i)(j)(k)
            }
            val min_temp: Double = min(theta(i)(j)(0), theta(i)(j)(1), theta(i)(j)(2))
            for (k <- 3 to 4) {
              if((1-temp) > min_temp) {  //(1-temp) > min(theta[i][j][3],theta[i][j][4])
                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * min_temp    //0 < 伪随机数 < min(theta[i][j][3],theta[i][j][4])
                theta(i)(j)(k) = randomValue
                temp += theta(i)(j)(k)
              }
              else {   //(1-temp) <= min(theta[i][j][3],theta[i][j][4])
                randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * (1 - temp)    //0 < 伪随机数 < 1 - temp
                theta(i)(j)(k) = randomValue
                temp += theta(i)(j)(k)
              }
            }
            theta(i)(j)(1) += (1 - temp) / 2                 //因p(R=1|I,L)的概率较低，所以分配剩余的概率值仅补给p(R=2|I,L)与p(R=3|I,L)
            theta(i)(j)(2) += (1 - temp) / 2
          }
        }
      }

      else {  //i!=2 && i!=3
        for (j <- 0 until q(i)) {
          temp = 0.0
          randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2)  //0 < 伪随机数 < 1
          theta(i)(j)(0) = randomValue
          temp += theta(i)(j)(0)
          for (k <- 1 until r(i)-1) {   //rand()函数产生0~RAND_MAX之间的伪随机整数
            randomValue = (random.nextInt(RAND_MAX) + 1).toDouble / (RAND_MAX + 2) * (1 - temp)    //0 < 伪随机数 < 1 - temp
            theta(i)(j)(k) = randomValue
            temp += theta(i)(j)(k)
          }
          theta(i)(j)(r(i)-1) = 1 - temp
        }
      }
    }
  }

  /** 根据BN参数数目初始化M_ijk */
  def initialize_M_ijk(M_ijk: Array[M_table], ThetaNum: Int) {
    //M_ijk().tag==i-j-k，M_ijk().value==w
    var v = 0
    for (i <- 1 to NodeNum)
      for (j <- 1 to q(i - 1) )
        for (k <- 1 to r(i - 1) ) {
          M_ijk(v).tag = i.toString + "-" + j.toString + "-" + k.toString  //i-j-k
          M_ijk(v).value = 0.0
          v += 1
        }
    if (ThetaNum != v)
      Console.err.println("方法\"initialize_M_ijk\"中存在Bug！Error: ThetaNum != v")
  }

  /** 将M_ijk标号映射为M_ijk表中对应的顺序位置，位置从0开始 */
  def hash_ijk(m_i: Int, m_j: Int, m_k: Int) : Int = {
    //m_i,m_j,m_k从0开始
    var pos = 0
    var i = 0
    while (i < m_i) {  //跳过0到i-1个节点的参数个数
      pos += q(i) * r(i)
      i += 1
    }
    pos = pos + m_j * r(i) + m_k          //i=m_i  //跳过该节点的前j-1个父节点取值对应的参数个数 //跳过该节点的前k-1个取值对应的参数个数
    pos
  }

  /** 将参数CPT形式转换为顺序表形式 */
  def CPT_to_Theta_ijk(Theta_ijk: Array[Double]) {
    var v = 0
    for (i <-0 until NodeNum)
      for (j <- 0 until q(i) )
        for (k <- 0 until r(i)) {
          v = hash_ijk(i, j, k)

          Theta_ijk(v) = theta(i)(j)(k)
        }
  }

  /** 将参数顺序表形式转换为n个CPT的存储形式 */
  def Theta_ijk_to_CPT(Theta_ijk: Array[Double]) {
    var v = 0
    for (i <- 0 until NodeNum)
      for (j <- 0 until q(i) )
        for (k <- 0 until r(i) ) {
          v = hash_ijk(i, j, k)
          theta(i)(j)(k) = Theta_ijk(v)
    }
  }

  /** 将最优参数顺序表形式转换为n个CPT的存储形式 */
  def optimal_Theta_to_CPT(Theta_ijk: ArrayBuffer[Double]) {
    var v = 0
    for (i <- 0 until NodeNum)
      for (j <- 0 until q(i) )
        for (k <- 0 until r(i) ) {
          v = hash_ijk(i, j, k)
          theta(i)(j)(k) = Theta_ijk(v)
        }
  }

  //以下方法为本类自用方法
  /** 判定j是否与PreferenceMatch中的某个元素匹配 */
  def equation(j: Int, PreferenceMatch: Array[Int], r: Int): Boolean = {
   //r——数组PreferenceMatch的大小
    for (i <- 0 until r) {
      if (j == PreferenceMatch(i))
        return true
    }
    false
  }

  /** 取a与b中的较小者 */
  def min(a: Double, b: Double): Double = {
    if (a > b) b
    else a
  }

  /** 取a,b,c中的较小者 */
  def min(a: Double, b: Double, c: Double): Double = {
    if (a > b) {
      if (b > c) c
      else b
    }
    else {  //a <= b
      if (a > c) c
      else a
    }
  }

}