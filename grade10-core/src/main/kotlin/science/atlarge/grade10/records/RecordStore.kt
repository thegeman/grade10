package science.atlarge.grade10.records

/**
 * TODO: Document
 */
class RecordStore : Iterable<Record> {

    private val records: MutableList<Record> = arrayListOf()

    fun add(newRecord: Record) {
        records.add(newRecord)
    }

    fun addAll(newRecords: Iterable<Record>) {
        records.addAll(newRecords)
    }

    fun removeIf(predicate: (Record) -> Boolean) {
        records.removeIf(predicate)
    }

    override fun iterator() = records.iterator()

}
