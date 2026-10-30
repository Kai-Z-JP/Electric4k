import org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsRootExtension

plugins {
    kotlin("multiplatform") version "2.1.21"
    kotlin("plugin.serialization") version "2.1.21"
    id("com.android.library") version "8.9.3"
    `maven-publish`
}
group = "io.github.kai-z-jp"
version = "0.3.0-SNAPSHOT"
repositories {
    mavenCentral()
    maven("https://dl.google.com/dl/android/maven2/")
}
kotlin {
    jvm { compilations.all { compileTaskProvider.configure { compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) } } }
    androidTarget { publishLibraryVariants("release"); compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
    @OptIn(org.jetbrains.kotlin.gradle.ExperimentalWasmDsl::class)
    wasmJs { browser { testTask { useKarma { useChromeHeadless() } } } }
    js(IR) { browser { testTask { useKarma { useChromeHeadless() } } }; nodejs() }
    listOf(iosArm64(), iosSimulatorArm64(), iosX64()).forEach {
        it.binaries.framework { baseName = "Electric4k"; isStatic = true }
    }
    jvmToolchain(21)
    sourceSets {
        commonMain.dependencies {
            api("io.ktor:ktor-client-core:3.1.3")
            api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
            api("org.jetbrains.kotlinx:kotlinx-serialization-json:1.8.1")
            implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.6.2")
        }
        commonTest.dependencies {
            implementation(kotlin("test"))
            implementation("io.ktor:ktor-client-mock:3.1.3")
            implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
        }
        jvmMain.dependencies { implementation("io.ktor:ktor-client-cio:3.1.3") }
        androidMain.dependencies { implementation("io.ktor:ktor-client-okhttp:3.1.3") }
        wasmJsMain.dependencies { implementation("io.ktor:ktor-client-js:3.1.3") }
        jsMain.dependencies { implementation("io.ktor:ktor-client-js:3.1.3") }
        iosMain.dependencies { implementation("io.ktor:ktor-client-darwin:3.1.3") }
    }
}
android {
    namespace = "electric4k"
    compileSdk = 35
    defaultConfig { minSdk = 23 }
    compileOptions { sourceCompatibility = JavaVersion.VERSION_17; targetCompatibility = JavaVersion.VERSION_17 }
}
// Use an already installed Node when supplied by a managed development environment.
rootProject.plugins.withType<org.jetbrains.kotlin.gradle.targets.js.nodejs.NodeJsRootPlugin> {
    rootProject.extensions.configure<NodeJsRootExtension> {
        if (providers.gradleProperty("electric4k.systemNode").orNull == "true") download = false
    }
}

val jvmTests = tasks.named<Test>("jvmTest") {
    exclude("**/ElectricIntegrationTest*")
}
tasks.register<Test>("integrationTest") {
    description = "Tests against the local PostgreSQL/Electric Docker fixture"
    group = "verification"
    outputs.upToDateWhen { false }
    dependsOn("jvmTestClasses")
    testClassesDirs = jvmTests.get().testClassesDirs
    classpath = jvmTests.get().classpath
    include("**/ElectricIntegrationTest*")
}
