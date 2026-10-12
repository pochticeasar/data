plugins {
    id("java")
    id("application")
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(26)
    }
}

application {
    mainClass = "org.example.Main"
}

dependencies {
    implementation("org.apache.parquet:parquet-avro:1.15.2") {
        exclude(group = "org.apache.hadoop")
    }
    implementation("org.apache.parquet:parquet-hadoop:1.15.2") {
        exclude(group = "org.apache.hadoop")
    }

    // parquet-java завязан на классы hadoop (Configuration, Path), поэтому они нужны в classpath.
    // Файловая система hadoop при этом не используется: читаем и пишем через LocalInputFile/LocalOutputFile.
    implementation("org.apache.hadoop:hadoop-client-api:3.4.1")
    runtimeOnly("org.apache.hadoop:hadoop-client-runtime:3.4.1")

    testImplementation(platform("org.junit:junit-bom:6.0.0"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
