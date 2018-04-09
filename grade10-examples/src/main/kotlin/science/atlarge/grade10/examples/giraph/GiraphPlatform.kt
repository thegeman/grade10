package science.atlarge.grade10.examples.giraph

import science.atlarge.grade10.Grade10Platform
import science.atlarge.grade10.Grade10PlatformRegistry
import science.atlarge.grade10.model.execution.ExecutionModelSpecification
import science.atlarge.grade10.model.resources.ResourceModelSpecification
import java.nio.file.Path

object GiraphPlatform : Grade10Platform {

    override val name: String = "giraph"
    override val version: String = "1.2.0-model-1"
    override val executionModelSpecification: ExecutionModelSpecification = GiraphExecutionModel.specification
    override val resourceModelSpecification: ResourceModelSpecification = GiraphResourceModel.specification

    override fun createJob(inputDirectories: List<Path>, outputDirectory: Path): GiraphJob {
        return GiraphJob(inputDirectories, outputDirectory)
    }

    fun register() {
        Grade10PlatformRegistry.add(this)
    }

}
