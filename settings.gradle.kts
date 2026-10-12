plugins {
    // Скачивает нужный JDK сам: Spark 4.2 не работает на JDK 26, который стоит локально.
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "data"
