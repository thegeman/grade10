package science.atlarge.grade10.analysis.bottlenecks

import science.atlarge.grade10.model.execution.Phase
import science.atlarge.grade10.model.execution.PhaseType

interface PerPhaseBottleneckPredicate {

    fun combineSubPhaseBottlenecks(subPhaseBottlenecks: Iterator<PhaseAndBottleneckStatus>): BottleneckStatus

}

interface PerPhaseTypeBottleneckPredicate : PerPhaseBottleneckPredicate {

    override fun combineSubPhaseBottlenecks(subPhaseBottlenecks: Iterator<PhaseAndBottleneckStatus>): BottleneckStatus {
        return combineSubPhaseTypeBottlenecks(object : Iterator<PhaseTypeAndBottleneckStatus> {

            private val cache = PhaseTypeAndBottleneckStatus()

            override fun hasNext(): Boolean = subPhaseBottlenecks.hasNext()

            override fun next(): PhaseTypeAndBottleneckStatus = cache.also {
                val nextVal = subPhaseBottlenecks.next()
                it.bottleneckStatus = nextVal.bottleneckStatus
                it.phaseType = nextVal.phase.type
            }

        })
    }

    fun combineSubPhaseTypeBottlenecks(subPhaseBottlenecks: Iterator<PhaseTypeAndBottleneckStatus>): BottleneckStatus

}

interface BottleneckPredicate : PerPhaseTypeBottleneckPredicate {

    override fun combineSubPhaseBottlenecks(
            subPhaseBottlenecks: Iterator<PhaseAndBottleneckStatus>
    ): BottleneckStatus {
        return combineBottlenecks(object : BottleneckStatusIterator {

            override fun hasNext(): Boolean = subPhaseBottlenecks.hasNext()

            override fun nextBottleneckStatus(): BottleneckStatus = subPhaseBottlenecks.next().bottleneckStatus

        })
    }

    override fun combineSubPhaseTypeBottlenecks(
            subPhaseBottlenecks: Iterator<PhaseTypeAndBottleneckStatus>
    ): BottleneckStatus {
        return combineBottlenecks(object : BottleneckStatusIterator {

            override fun hasNext(): Boolean = subPhaseBottlenecks.hasNext()

            override fun nextBottleneckStatus(): BottleneckStatus = subPhaseBottlenecks.next().bottleneckStatus

        })
    }

    fun combineBottlenecks(bottlenecks: BottleneckStatusIterator): BottleneckStatus

}


object AnyBottleneckPredicate : BottleneckPredicate {

    override fun combineBottlenecks(bottlenecks: BottleneckStatusIterator): BottleneckStatus {
        while (bottlenecks.hasNext()) {
            val status = bottlenecks.next()
            if (status != BottleneckStatusConstants.NONE) {
                return status
            }
        }
        return BottleneckStatusConstants.NONE
    }

}

object AllBottleneckPredicate : BottleneckPredicate {

    override fun combineBottlenecks(bottlenecks: BottleneckStatusIterator): BottleneckStatus {
        var status = BottleneckStatusConstants.NONE
        while (bottlenecks.hasNext()) {
            status = bottlenecks.next()
            if (status == BottleneckStatusConstants.NONE) {
                return status
            }
        }
        return status
    }

}

object MajorityBottleneckPredicate : BottleneckPredicate {

    override fun combineBottlenecks(bottlenecks: BottleneckStatusIterator): BottleneckStatus {
        var count = 0
        var bottleneckedCount = 0
        var localOrGlobal = BottleneckStatusConstants.LOCAL
        while (bottlenecks.hasNext()) {
            val status = bottlenecks.next()
            count++
            if (status != BottleneckStatusConstants.NONE) {
                bottleneckedCount++
                localOrGlobal = status
            }
        }
        return if (bottleneckedCount > 0 && bottleneckedCount * 2 > count) {
            localOrGlobal
        } else {
            BottleneckStatusConstants.NONE
        }
    }

}

class GroupByPhaseTypeBottleneckPredicate(
        private val sameTypePredicate: PerPhaseBottleneckPredicate,
        private val differentTypePredicate: PerPhaseTypeBottleneckPredicate
) : PerPhaseBottleneckPredicate {

    private val encounteredPhaseTypes: MutableSet<PhaseType> = hashSetOf()
    private val bottlenecksByPhaseTypes: MutableMap<PhaseType, MutableList<PhaseAndBottleneckStatus>> = mutableMapOf()

    private var pairPool: Array<PhaseAndBottleneckStatus> = Array(8) { PhaseAndBottleneckStatus() }
    private var pairsUsed: Int = 0

    private val phaseTypeAndBottleneckStatusIterator = object : Iterator<PhaseTypeAndBottleneckStatus> {

        private lateinit var phaseTypeIterator: Iterator<PhaseType>
        private val phaseTypeAndBottleneckStatus = PhaseTypeAndBottleneckStatus()

        fun reset() {
            phaseTypeIterator = encounteredPhaseTypes.iterator()
        }

        override fun hasNext(): Boolean = phaseTypeIterator.hasNext()

        override fun next(): PhaseTypeAndBottleneckStatus = phaseTypeAndBottleneckStatus.also {
            val nextPhaseType = phaseTypeIterator.next()
            it.phaseType = nextPhaseType
            it.bottleneckStatus = sameTypePredicate.combineSubPhaseBottlenecks(
                    bottlenecksByPhaseTypes[nextPhaseType]!!.iterator())
        }

    }

    override fun combineSubPhaseBottlenecks(subPhaseBottlenecks: Iterator<PhaseAndBottleneckStatus>): BottleneckStatus {
        while (subPhaseBottlenecks.hasNext()) {
            add(subPhaseBottlenecks.next())
        }

        phaseTypeAndBottleneckStatusIterator.reset()
        val result = differentTypePredicate.combineSubPhaseTypeBottlenecks(phaseTypeAndBottleneckStatusIterator)

        encounteredPhaseTypes.forEach { bottlenecksByPhaseTypes[it]!!.clear() }
        encounteredPhaseTypes.clear()

        return result
    }

    private fun add(phaseAndBottleneckStatus: PhaseAndBottleneckStatus) {
        if (pairsUsed == pairPool.size) {
            pairPool = Array(pairPool.size + pairPool.size / 2) { i ->
                if (i < pairPool.size) pairPool[i] else PhaseAndBottleneckStatus()
            }
        }
        val copy = pairPool[pairsUsed++].also {
            it.phase = phaseAndBottleneckStatus.phase
            it.bottleneckStatus = phaseAndBottleneckStatus.bottleneckStatus
        }
        encounteredPhaseTypes.add(copy.phase.type)
        bottlenecksByPhaseTypes.getOrPut(copy.phase.type) { arrayListOf() }
                .add(copy)
    }

}

fun selectPredicatePerParentPhase(
        predicateForParentPhase: (Phase) -> PerPhaseBottleneckPredicate
): PerPhaseBottleneckPredicate {
    return object : PerPhaseBottleneckPredicate {

        private val cache: MutableMap<Phase, PerPhaseBottleneckPredicate> = hashMapOf()
        private val peekingIterator = object : Iterator<PhaseAndBottleneckStatus> {

            private lateinit var iterator: Iterator<PhaseAndBottleneckStatus>
            private var nextVal: PhaseAndBottleneckStatus? = null

            fun setIterator(iterator: Iterator<PhaseAndBottleneckStatus>) {
                this.iterator = iterator
                nextVal = if (iterator.hasNext()) iterator.next() else null
            }

            fun peek(): PhaseAndBottleneckStatus? = nextVal

            override fun hasNext(): Boolean = nextVal != null

            override fun next(): PhaseAndBottleneckStatus {
                val result = nextVal!!
                nextVal = if (iterator.hasNext()) iterator.next() else null
                return result
            }

        }

        override fun combineSubPhaseBottlenecks(subPhaseBottlenecks: Iterator<PhaseAndBottleneckStatus>): BottleneckStatus {
            peekingIterator.setIterator(subPhaseBottlenecks)
            val peeked = peekingIterator.peek()
            if (peeked != null) {
                val parent = peeked.phase.parent
                if (parent != null) {
                    val predicate = cache.getOrPut(parent) { predicateForParentPhase(parent) }
                    return predicate.combineSubPhaseBottlenecks(peekingIterator)
                }
            }
            return BottleneckStatusConstants.NONE
        }

    }
}
