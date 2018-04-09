package science.atlarge.grade10.model.execution

import science.atlarge.grade10.model.HierarchyComponent
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.PathComponent
import science.atlarge.grade10.util.TimeSliceCount
import science.atlarge.grade10.util.TimeSliceId
import science.atlarge.grade10.util.TimeSliceRange

/**
 * A hierarchical model of [Phases][Phase] describing the execution of a single job on the modeled system.
 *
 * @property[specification] a specification of the system's execution model.
 * @property[rootPhase] the root of the [Phase] hierarchy.
 */
class ExecutionModel(
        val specification: ExecutionModelSpecification,
        val rootPhase: Phase
) {

    init {
        require(specification.rootPhaseType === rootPhase.type) {
            "The type of the execution model's root must be the execution model specification's root"
        }
    }

    fun resolvePhase(path: Path): Phase? = rootPhase.resolve(path)

}

/**
 * Represents a phase in the execution of a job. Each phase is an instance of a [PhaseType] in an [execution model
 * specification][ExecutionModelSpecification]. Phases may be nested to create a hierarchical model of a job.
 *
 * @param[parent] the parent of this phase, or null if this phase is the root of the hierarchy.
 * @property[type] the [PhaseType] this phase is an instance of.
 * @property[record] a record of the start and end time of this phase.
 * @property[instanceId] a unique identifier for this instance of a repeatable phase type,
 *         or blank if this phase's type is not of repeatable.
 */
class Phase(
        val type: PhaseType,
        parent: Phase?,
        val record: PhaseRecord,
        val instanceId: String = ""
) : HierarchyComponent<Phase>(parent, deriveName(type, instanceId)) {

    /**
     * @return a non-empty name uniquely identifying this phase among all phases with the same [parent],
     *         or the empty string for the root of the hierarchy.
     */
    val name: String
        get() = componentId
    /**
     * @return a shortened [name] of this phase without the name of the instance key.
     */
    val shortName: String = deriveName(type, instanceId, includeKey = false)

    private val _subphases = mutableMapOf<String, Phase>()
    /**
     * @return all subphases of this phase, keyed by their [name].
     */
    val subphases: Map<String, Phase>
        get() = _subphases

    private val _subphasesByShortName = mutableMapOf<String, Phase>()
    /**
     * @return all subphases of this phase, keyed by their [shortName].
     */
    val subphasesByShortName: Map<String, Phase>
        get() = _subphasesByShortName

    /**
     * @return true iff this phase has children.
     */
    val isComposite: Boolean
        get() = _subphases.isNotEmpty()
    /**
     * @return true iff this phase has no children.
     */
    val isLeaf: Boolean
        get() = _subphases.isEmpty()

    private val _annotations = arrayListOf<PhaseAnnotation>()
    /**
     * @return the list of annotations attached to this phase.
     */
    val annotations: List<PhaseAnnotation>
        get() = _annotations

    /**
     * @return the time slice during which this phase started.
     */
    val firstTimeSlice: TimeSliceId
        get() = record.firstTimeSlice
    /**
     * @return the time slice during which this phase ended, may be smaller than [firstTimeSlice] if this phase was too
     *         short to span one time slice.
     */
    val lastTimeSlice: TimeSliceId
        get() = record.lastTimeSlice
    /**
     * @return the range of time slices during which this phase was executing.
     */
    val timeSliceRange: TimeSliceRange
        get() = LongRange(firstTimeSlice, lastTimeSlice)
    /**
     * @return the number of time slices during which this phase was executing.
     */
    val timeSliceDuration: TimeSliceCount
        get() = record.timeSliceDuration

    init {
        if (parent == null) {
            require(type.isRoot) {
                "The type of an execution model's root phase must be the root of an execution model specification."
            }
        } else {
            require(type.parent === parent.type) {
                "The type of this phase's parent and the parent of this phase's type must match."
            }
            require(!type.repeatability.isRepeatable || isValidInstanceId(instanceId)) {
                "Instance ID must be valid for an instance of a repeatable phase type"
            }
        }

        parent?.addSubphase(this)
    }

    override fun lookupChild(childId: PathComponent): Phase? {
        return if ('[' in childId && '=' !in childId) {
            _subphasesByShortName[childId]
        } else {
            _subphases[childId]
        }
    }

    fun subphasesForType(subphaseTypeName: String): Iterable<Phase> {
        val subphaseType = type.subphaseTypes[subphaseTypeName]
                ?: throw IllegalArgumentException("No subphase type found for name \"$subphaseTypeName\"")
        return subphasesForType(subphaseType)
    }

    fun subphasesForType(subphaseType: PhaseType): Iterable<Phase> {
        require(subphaseType.parent === type) { "Parent of subphase type should be this phase's type" }
        return _subphases.values.filter { it.type === subphaseType }
    }

    /**
     * Attaches the given annotation to this phase.
     */
    fun annotate(annotation: PhaseAnnotation) {
        _annotations.add(annotation)
    }

    private fun addSubphase(subphase: Phase) {
        require(subphase.parent === this) {
            "Cannot add a subphase to a phase that is not its parent"
        }
        require(subphase.name !in _subphases) {
            "A subphase with the name \"${subphase.name}\" already exists"
        }
        require(subphase.shortName !in _subphasesByShortName) {
            "A subphase with the short name \"${subphase.shortName}\" already exists"
        }

        _subphases[subphase.name] = subphase
        _subphasesByShortName[subphase.shortName] = subphase
    }

    override fun toString(): String {
        return "Phase(\"$path\")"
    }

    companion object {

        /**
         * Checks if the given string is a valid instance ID, i.e., it contains no whitespace, square brackets,
         * equality signs, or forward slashes.
         *
         * @return true iff the input is a valid instance ID.
         */
        fun isValidInstanceId(instanceId: String): Boolean {
            return instanceId.all { c ->
                !c.isWhitespace() && c != '=' && c != '[' && c != ']' && c != '/'
            }
        }

        fun constructPhasePath(phaseType: PhaseType, instanceKeyLookup: (String) -> String): Path {
            val phaseLineage = generateSequence(phaseType, PhaseType::parent).toList().dropLast(1).asReversed()
            return Path(
                    pathComponents = phaseLineage.map {
                        val instanceId = if (it.repeatability.isRepeatable) {
                            instanceKeyLookup(it.repeatability.instanceKey)
                        } else {
                            ""
                        }
                        deriveName(it, instanceId)
                    },
                    isRelative = false
            )
        }

        private fun deriveName(phaseType: PhaseType, instanceId: String, includeKey: Boolean = true): String {
            return if (phaseType.repeatability.isRepeatable) {
                "${phaseType.name}[${if (includeKey) "${phaseType.repeatability.instanceKey}=" else ""}$instanceId]"
            } else {
                phaseType.name
            }
        }

    }

}

/**
 * Base annotation used to attach additional information to a [Phase].
 */
interface PhaseAnnotation
