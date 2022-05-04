package com.atguigu.util

import java.io.InputStream
import java.util.Properties

object PropertiesUtil {
  def load(file: String): Properties = {
    val properties = new Properties()
    val inputStream: InputStream = PropertiesUtil.getClass.getClassLoader.getResourceAsStream(file)
    properties.load(inputStream)
    properties
  }
}
