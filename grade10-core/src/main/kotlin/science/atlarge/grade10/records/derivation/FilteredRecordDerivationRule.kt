package science.atlarge.grade10.records.derivation

import science.atlarge.grade10.records.Record

interface FilteredRecordDerivationRule : RecordDerivationRule {

    fun matches(record: Record): Boolean

    fun deriveNewRecordsFromMatched(records: Iterable<Record>): Iterable<Record>

    override fun deriveNewRecordsFrom(records: Iterable<Record>): Iterable<Record> =
            deriveNewRecordsFromMatched(records.filter { matches(it) })

}