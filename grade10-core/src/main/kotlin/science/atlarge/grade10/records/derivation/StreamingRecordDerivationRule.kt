package science.atlarge.grade10.records.derivation

import science.atlarge.grade10.records.Record

interface StreamingRecordDerivationRule : FilteredRecordDerivationRule {

    fun deriveNewRecordsFromSingle(record: Record): Iterable<Record>

    override fun deriveNewRecordsFromMatched(records: Iterable<Record>): Iterable<Record> =
            records.flatMap { deriveNewRecordsFromSingle(it) }

}