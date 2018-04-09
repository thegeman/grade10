plugins {
    kotlin("jvm")
}

val versionKotlin: String by extra
val versionKryo: String by extra

dependencies {
    compile(kotlin("stdlib", versionKotlin))

    compile("com.esotericsoftware", "kryo", versionKryo)
}