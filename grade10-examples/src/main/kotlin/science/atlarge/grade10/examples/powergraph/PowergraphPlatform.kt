package science.atlarge.grade10.examples.powergraph

import science.atlarge.grade10.Grade10Platform
import science.atlarge.grade10.Grade10PlatformRegistry
import science.atlarge.grade10.model.execution.ExecutionModelSpecification
import science.atlarge.grade10.model.resources.ResourceModelSpecification
import java.nio.file.Path

object PowergraphPlatform : Grade10Platform {

    override val name: String = "powergraph"
    override val version: String = "1.0.0-model-1"
    override val executionModelSpecification: ExecutionModelSpecification = PowergraphExecutionModel.specification
    override val resourceModelSpecification: ResourceModelSpecification = PowergraphResourceModel.specification

    override fun createJob(inputDirectories: List<Path>, outputDirectory: Path): PowergraphJob {
        return PowergraphJob(inputDirectories, outputDirectory)
    }

    fun register() {
        Grade10PlatformRegistry.add(this)
    }

}
