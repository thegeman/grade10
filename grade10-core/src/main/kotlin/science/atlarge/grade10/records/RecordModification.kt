package science.atlarge.grade10.records

/**
 * TODO: Document
 */
interface RecordModificationRule {

    fun modify(recordStore: RecordStore)

}

/**
 * TODO: Document
 */
interface RecordDerivationRule : RecordModificationRule {

    fun deriveFromRecords(records: Iterable<Record>): Iterable<Record>

    override fun modify(recordStore: RecordStore) {
        val newRecords = deriveFromRecords(recordStore)
        recordStore.addAll(newRecords)
    }

}

/**
 * TODO: Document
 */
interface FilteredRecordDerivationRule : RecordDerivationRule {

    fun matches(record: Record): Boolean

    override fun modify(recordStore: RecordStore) {
        val matchedRecords = recordStore.filter(this::matches)
        val newRecords = deriveFromRecords(matchedRecords)
        recordStore.addAll(newRecords)
    }

}

/**
 * TODO: Document
 */
abstract class TypeFilteredRecordDerivationRule<in T : Record>(
        private val klass: Class<T>
) : RecordModificationRule {

    protected open fun matches(record: T): Boolean {
        return true
    }

    protected abstract fun deriveFromMatchedRecords(records: Iterable<T>): Iterable<Record>

    override fun modify(recordStore: RecordStore) {
        val matchedRecords = recordStore.filterIsInstance(klass).filter(this::matches)
        val newRecords = deriveFromMatchedRecords(matchedRecords)
        recordStore.addAll(newRecords)
    }

}
