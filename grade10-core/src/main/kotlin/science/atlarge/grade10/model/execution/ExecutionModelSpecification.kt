package science.atlarge.grade10.model.execution

import science.atlarge.grade10.model.HierarchyComponent
import science.atlarge.grade10.model.Path
import science.atlarge.grade10.model.PathComponent

/**
 * Specification of a hierarchical model of [PhaseTypes][PhaseType] describing the execution of any job on the modeled
 * system.
 *
 * @property[rootPhaseType] the root of the [PhaseType] hierarchy.
 */
class ExecutionModelSpecification(
        val rootPhaseType: PhaseType
) {

    fun resolvePhaseType(path: Path): PhaseType? = rootPhaseType.resolve(path)

}

/**
 * A type of phase in the execution of the system under test. Phase types may be nested to create a hierarchical model
 * of the system under test. Additionally, the subphase types of a given phase type may form a workflow by adding
 * dependencies between them, represented as a directed acyclic graph (DAG). A dependency of one phase type on another
 * implies a dependency from all instances of the former on all instances of the latter with the same parent.
 *
 * @param[parent] the parent of this type, or null if this type is the root of the hierarchy.
 * @param[name] a non-empty name uniquely identifying this type among all phase types with the same [parent],
 *         ignored for the root of the hierarchy.
 * @property[dependencies] a list of phase types on which this type depends.
 * @property[description] a description of the phase type to display to users.
 * @property[repeatability] the repeatability of the phase type in a job.
 */
class PhaseType(
        parent: PhaseType?,
        name: String,
        val dependencies: List<PhaseType>,
        val description: String,
        val repeatability: PhaseTypeRepeatability
) : HierarchyComponent<PhaseType>(parent, name) {

    /**
     * @return a non-empty name uniquely identifying this phase type among all phase types with the same [parent],
     *         or the empty string for the root of the hierarchy.
     */
    val name: String
        get() = componentId

    private val _subphaseTypes = mutableMapOf<String, PhaseType>()
    /**
     * @return all subphase types of this type, keyed by their [name].
     */
    val subphaseTypes: Map<String, PhaseType>
        get() = _subphaseTypes

    init {
        if (parent == null) {
            require(!repeatability.isRepeatable) { "The root of the hierarchy cannot be repeatable" }
            require(dependencies.isEmpty()) { "The root of the hierarchy cannot have dependencies" }
        }
        require(dependencies.all { it.parent === this.parent }) {
            "Dependencies can only be created between phase types with the same parent"
        }

        parent?.addSubphaseType(this)
    }

    override fun lookupChild(childId: PathComponent): PhaseType? {
        return _subphaseTypes[childId]
    }

    private fun addSubphaseType(subphaseType: PhaseType) {
        require(subphaseType.parent === this) {
            "Cannot add a subphase type to a phase type that is not its parent"
        }
        require(subphaseType.name !in _subphaseTypes) {
            "A subphase type with the name \"${subphaseType.name}\" already exists"
        }

        _subphaseTypes[subphaseType.name] = subphaseType
    }

    override fun toString(): String {
        return "PhaseType(\"$path\")"
    }

}

/**
 * A description of the repeatability of a phase type. It dictates whether multiple instances of a phase type can be
 * executed during the execution of a single instance of the parent phase type, and whether multiple instances of a
 * phase type can execute concurrently.
 *
 * @property[isRepeatable] true iff the phase type is repeatable.
 * @property[instanceKey] the name of a phase property that uniquely identifies instances of the same phase type with
 *         the same parent, or the empty string if not repeatable.
 */
sealed class PhaseTypeRepeatability(val isRepeatable: Boolean, val instanceKey: String = "") {

    /**
     * Indicates that only one instance of a phase type is executed per instance of the parent.
     */
    object NonRepeated : PhaseTypeRepeatability(false)

    /**
     * Indicates that more than one instance of a phase type can be executed in sequence. That is, there is an implicit
     * dependency from the n'th instance of the type on all earlier instances which disallows instances of this
     * phase type to overlap in time.
     *
     * A keyword must be specified to indicate how instances are identified (e.g., "index", "thread",
     * "node_id"). This may be used to associate [Records][science.atlarge.grade10.records.Record] with
     * an instance of a phase type.
     */
    class SequentialRepeated(repetitionKey: String) : PhaseTypeRepeatability(true, repetitionKey)

    /**
     * Indicates that more than one instance of a phase type can be executed concurrently. That is, there are no
     * dependencies between different instances of this type which allows them to occur in any order and overlapping.
     *
     * A keyword must be specified to indicate how instances are identified (e.g., "index", "thread",
     * "node_id"). This may be used to associate [Records][science.atlarge.grade10.records.Record] with
     * an instance of a phase type.
     */
    class ConcurrentRepeated(repetitionKey: String) : PhaseTypeRepeatability(true, repetitionKey)

    init {
        require(!isRepeatable || instanceKey.isNotBlank()) {
            "The instanceKey must not be empty if the phase type is repeatable"
        }
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as PhaseTypeRepeatability

        if (isRepeatable != other.isRepeatable) return false
        if (instanceKey != other.instanceKey) return false

        return true
    }

    override fun hashCode(): Int {
        var result = isRepeatable.hashCode()
        result = 31 * result + instanceKey.hashCode()
        return result
    }


}
