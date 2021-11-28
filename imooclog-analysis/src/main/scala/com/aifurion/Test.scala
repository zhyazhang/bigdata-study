package com.aifurion

import scala.util.matching.Regex
import java.util.regex.{Matcher, Pattern}

object Test {


    def main(args: Array[String]): Unit = {


        //"http://www.imooc.com/video/6512"

        //val string = "\"http://www.imooc.com/video/2147?_t_t_t=0.6748756573069841\"".replace("\"","")

        val string = "http://www.imooc.com/video/2147#t_t_t=0.6748756573069841".replace("\"", "")
        val domain = "http://www.imooc.com/"
        val domainIndex: Int = string.indexOf(domain)
        val cms: String = string.substring(domainIndex + domain.length)

        val cmsTypeId: Array[String] = cms.split("/")
        
        val pattern = """(^[0-9]+)""".r

        println(pattern.findFirstIn(cmsTypeId(1)).get)



    }
}
