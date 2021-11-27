package com.aifurion.utils

import java.io.InputStream
import java.util.Properties

object PropertiesUtil {

    lazy val getProperties: Properties = {
        val properties = new Properties()
        val inputStream: InputStream = this.getClass.
                getClassLoader.
                getResourceAsStream("application.properties")
        properties.load(inputStream)
        properties
    }
}
