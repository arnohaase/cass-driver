package com.ajjpj.cassdriver.connection

import java.net.InetAddress


case class CassandraClusterConfig()
case class CassandraConnectionConfig(clusterConfig: CassandraClusterConfig, address: InetAddress, port: Int)