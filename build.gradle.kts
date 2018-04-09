plugins {
    base
    kotlin("jvm") version "1.2.30"
}

allprojects {
    group = "science.atlarge.grade10"
    version = "0.1-SNAPSHOT"

    repositories {
        jcenter()
    }

    extra["versionKotlin"] = "1.2.30"
    extra["versionKryo"] = "4.0.0"
}
