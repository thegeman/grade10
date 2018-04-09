package science.atlarge.grade10.records.extraction

import science.atlarge.grade10.records.Record

interface RecordExtractionRule<in T> {

    fun extractAll(input: T, recordCreationCallback: (Record) -> Unit)

}
