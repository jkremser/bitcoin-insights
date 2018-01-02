package io.radanalytics.bitcoin

class Input(val value: Long, val address: String) {
}

class Output(val value: Long, val address: String) {
}

class Transaction(val id: String, val time: Int, val block: String, val inputs: Array[Input], val outputs: Array[Output]) {

}
