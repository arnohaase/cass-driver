package com.ajjpj.cassdriver.connection.api


//TODO tuning ensure hardware friendly representation for meta data --> changes very rarely, shared widely between threads and actors
case class CassTableMetadata(columns: Vector[CassColumnMetadata]) {

}

case class CassColumnMetadata(name: String, tpe: CassType) {

}