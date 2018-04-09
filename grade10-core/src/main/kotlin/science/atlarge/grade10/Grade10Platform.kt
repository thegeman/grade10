package science.atlarge.grade10

import science.atlarge.grade10.model.execution.ExecutionModelSpecification
import science.atlarge.grade10.model.resources.ResourceModelSpecification
import java.nio.file.Path

interface Grade10Platform {

    val name: String

    val version: String

    val executionModelSpecification: ExecutionModelSpecification

    val resourceModelSpecification: ResourceModelSpecification

    fun createJob(inputDirectories: List<Path>, outputDirectory: Path): Grade10PlatformJob

}

object Grade10PlatformRegistry {

    private val knownPlatforms = mutableMapOf<Pair<String, String>, Grade10Platform>()

    fun listPlatformNamesAndVersions(): Sequence<Pair<String, String>> {
        return knownPlatforms.asSequence()
                .map { it.key }
    }

    operator fun get(name: String, version: String): Grade10Platform? {
        return knownPlatforms[name to version]
    }

    fun add(platform: Grade10Platform) {
        knownPlatforms[platform.name to platform.version] = platform
    }

}
