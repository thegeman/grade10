package science.atlarge.grade10.records.derivation

import science.atlarge.grade10.records.Record

interface RecordDerivationRule {

    fun deriveNewRecordsFrom(records: Iterable<Record>): Iterable<Record>

}
