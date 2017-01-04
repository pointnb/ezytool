import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.apache.spark.SparkConf

import scala.io.Source
import java.io._
import java.sql.Connection
import java.sql.DriverManager

object wordCount{
		// args(0): start_year	
		// args(1): start_month
		// args(2): end_year
		// args(3): end_month
		// args(4): top_count
		// args(5): news [0:baseball|1:daily|2:all]
		// args(6): category [0:NER|1:!NER|2:!all]
		// args(7~20): ner category [#:14]
		// args(21): keyword[optional]
				
	def main(args: Array[String]){

		val conf = new SparkConf().setAppName("wordCount")
		var es_query: String = null
		var rk = 0
				
		if(args.length == 22 && args(21) == "-rk"){
			rk = 1
		}
		
		if(args.length  == 21 || rk == 1){
			es_query = """{"filter":{"range":{"date":{"gte": """" + args(0) + """-""" + args(1) + """", "lte": """" + args(2) + """-""" + args(3) + """"}}}}"""
		}else{
			es_query = """{"filter":{"range":{"date":{"gte": """" + args(0) + """-""" + args(1) + """", "lte": """" + args(2) + """-""" + args(3) + """"}}}, "query": {"query_string":{"query": "body: """ + args(21) + """"} } }"""
		}

		conf.set("es.index,auto.create", "true")
		conf.set("es.nodes.discovery", "true")
		conf.set("es.nodes", "127.0.0.1:9200")		
		conf.set("es.query", es_query)		
		
		if(args(5).toInt == 0){
			conf.set("es.resource", "baseball/naver")
		}else if(args(5).toInt == 1){
			conf.set("es.resource", "baseball/daily")
		}else{
			conf.set("es.resource", "baseball")
		}
		
		var sc = new SparkContext(conf)
		var dRDD: org.apache.spark.rdd.RDD[String] = null
		var data: org.apache.spark.sql.DataFrame = null

		var elasticRDD = sc.esRDD()
		var bodyRDD = elasticRDD.values.map( s=>s.get("body").toString)
		var wordCountRDD: org.apache.spark.rdd.RDD[(Int, String)] = null
		var wordFilter: org.apache.spark.rdd.RDD[String] = null
		
		
		var ner_category = Array("PER", "FLD", "AFW", "ORG", "LOC", "CVL", "DAT", "TIM", "NUM", "EVT", "ANM", "PLT", "MAT", "TRM")
		var ner_filter: String = null
		
		/*
		PER 인명
		FLD 학문분야
		AFW 인공물
		ORG 기관명
		LOC 지역명
		CVL 문화
		DAT 날짜
		TIM 시간
		NUM 숫자
		EVT 사건
		ANM 신체
		PLT 식물명
		MAT 화학물질명
		TRM 용어		
		*/
		
		var flag = 0
		for(num <- 7 to 20){
			if(args(num).toInt == 1){
				if(ner_filter == null){
					ner_filter = ner_category(num-7)	
				}else{
					ner_filter = ner_filter + " " + ner_category(num-7)	
				}
			}
		}		
	
		if(rk == 1){
			try{				
				for(line <- Source.fromFile("/apache_tmp/rank.txt").getLines()){					
					var keyword_and_count = line.split(",") // (0): keyword, (1): count
					var keyword = keyword_and_count(0).split("-")(0)
					
					sc.stop()
					
					es_query = """{"filter":{"range":{"date":{"gte": """" + args(0) + """-""" + args(1) + """", "lte": """" + args(2) + """-""" + args(3) + """"}}}, "query": {"query_string":{"query": "body: """ + keyword + """"} } }"""
					conf.set("es.query", es_query)		
					
					sc =new SparkContext(conf)
					
					var elasticRDD = sc.esRDD()
					var bodyRDD = elasticRDD.values.map( s=>s.get("body").toString)
					
					var wordFlat = bodyRDD.flatMap(s=>s.replace("Some(", "").replace(")", "").split(" "))
					
					if(args(6).toInt == 0){ // NER
						wordFilter = wordFlat.filter( s=> s.contains("-") && !s.contains(keyword) )
						wordFilter = wordFilter.filter({ s=> var tmp = s.split("-");  var tmp2 = tmp(1).split("_"); tmp(0).length() > 1 && tmp2(1) != "I" && ner_filter.contains(tmp2(0))})
					}else if(args(6).toInt == 1){ // not NER
						wordFilter = wordFlat.filter( s=> (!s.contains("-")) && !s.contains(keyword) && s.length() > 1)
					}else{
						wordFilter = wordFlat.filter( s=> (!s.contains(keyword)) )
						wordFilter = wordFilter.filter({ s=> if(s.contains("-")){ var tmp = s.split("-");  var tmp2 = tmp(1).split("_"); tmp(0).length() > 1 && tmp2(1) != "I" && ner_filter.contains(tmp2(0))}else{ s.length() > 1} })
					}
					println("{\"word\": \"new_word\", \"count\": " + 0 + "}")
					println("{\"word\": \"" + keyword_and_count(0) + "\", \"count\": " + keyword_and_count(1).toInt + "}")
					wordCountRDD = wordFilter.map(word => (word, 1)).reduceByKey(_ + _).map(s=>s.swap).sortByKey(false, 1)
					if(args(4).toInt == 0){
						wordCountRDD.collect.foreach({ case(k, v) => println("{\"word\": \"" + v + "\", \"count\": " + k.toInt + "}") })
					}
					else{
						wordCountRDD.take(args(4).toInt).foreach({ case(k, v) => println("{\"word\": \"" + v + "\", \"count\": " + k.toInt + "}") })
					}
				}
			} catch{
				case ex: Exception => println(ex)
			}
		}else if(args.length  == 22){ // keyword search
			var keyword = args(21)
			println(keyword)
			var wordFlat = bodyRDD.flatMap(s=>s.replace("Some(", "").replace(")", "").split(" "))
				
			if(args(6).toInt == 0){ // NER
				wordFilter = wordFlat.filter( s=> s.contains("-") && !s.contains(keyword) )
				wordFilter = wordFilter.filter({ s=> var tmp = s.split("-");  var tmp2 = tmp(1).split("_"); tmp(0).length() > 1 && tmp2(1) != "I" && ner_filter.contains(tmp2(0));})
			}else if(args(6).toInt == 1){ // not NER
				wordFilter = wordFlat.filter( s=> (!s.contains("-")) && !s.contains(keyword) && s.length() > 1)
			}else{
				wordFilter = wordFlat.filter( s=> (!s.contains(keyword)) )
				wordFilter = wordFilter.filter({ s=> if(s.contains("-")){ var tmp = s.split("-");  var tmp2 = tmp(1).split("_"); tmp(0).length() > 1 && tmp2(1) != "I" && ner_filter.contains(tmp2(0));}else{ s.length() > 1} })
			}
			wordCountRDD = wordFilter.map(word => (word, 1)).reduceByKey(_ + _).map(s=>s.swap).sortByKey(false, 1)
			if(args(4).toInt == 0){
				wordCountRDD.collect.foreach({ case(k, v) => println("{\"word\": \"" + v + "\", \"count\": " + k.toInt + "}") })
			}
			else{
				wordCountRDD.take(args(4).toInt).foreach({ case(k, v) => println("{\"word\": \"" + v + "\", \"count\": " + k.toInt + "}") })
			}
		}else{ // range search
			var wordFlat = bodyRDD.flatMap(s=>s.replace("Some(", "").replace(")", "").split(" "))
			
			if(args(6).toInt == 0){ // NER
				wordFilter = wordFlat.filter( s=> s.contains("-"))
				wordFilter = wordFilter.filter({ s=> var tmp = s.split("-");  var tmp2 = tmp(1).split("_");  tmp(0).length() > 1 && tmp2(1) != "I" && ner_filter.contains(tmp2(0));})
			}else if(args(6).toInt == 1){ // Not NER
				wordFilter = wordFlat.filter( s=> (!s.contains("-")) && s.length() > 1)
			}else{
				wordFilter = wordFlat.filter({ s=> if(s.contains("-")){ var tmp = s.split("-");  var tmp2 = tmp(1).split("_"); tmp(0).length() > 1 && tmp2(1) != "I" && ner_filter.contains(tmp2(0));}else{ s.length() > 1} })
			}

			wordCountRDD = wordFilter.map(word => (word, 1)).reduceByKey(_ + _).map(s=>s.swap).sortByKey(false, 1)
			val writer = new PrintWriter(new File("/apache_tmp/rank.txt"))
			if(args(4).toInt == 0){
				wordCountRDD.collect.foreach({ case(k, v) => println("{\"word\": \"" + v + "\", \"count\": " + k.toInt + "}"); writer.write(v + "," + k + "\n") })
			}
			else{
				wordCountRDD.take(args(4).toInt).foreach({ case(k, v) => println("{\"word\": \"" + v + "\", \"count\": " + k.toInt + "}"); writer.write(v + "," + k + "\n") })
			}
			
			writer.close()
		}
	}        
}
