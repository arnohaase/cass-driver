package com.ajjpj.cassdriver.connection.metadata


//TODO tuning ensure hardware friendly representation for meta data --> changes very rarely, shared widely between threads and actors
case class CassTableMetadata(columns: Vector[CassColumnMetadata]) {

}

case class CassColumnMetadata(keyspace: String, tableName: String, colName: String, tpe: CassType) {

}