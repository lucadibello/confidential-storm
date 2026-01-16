# Confidential Storm

Confidential Storm is a robust library designed to enable confidential stream processing applications. By seamlessly integrating [Apache Storm](https://storm.apache.org/) with [Intel SGX](https://www.intel.com/content/www/us/en/developer/tools/software-guard-extensions/overview.html) (via the [Apache Teaclave Java TEE SDK](https://teaclave.apache.org/)), it facilitates the execution of sensitive data processing tasks within secure enclaves. This ensures data privacy and integrity, allowing for secure computation even in potentially untrusted cloud environments.

This library transforms standard stream processing paradigms by providing the necessary primitives to offload sensitive operations to hardware-protected memory regions (enclaves), whilst maintaining the scalability and fault tolerance of Apache Storm.

## Key Features

- **Secure Stream Processing**: Specialized implementations of Storm Bolts and Spouts (`ConfidentialBolt`, `ConfidentialSpout`) that seamlessly delegate processing to secure enclaves.
- **Teaclave Integration**: A streamlined abstraction layer over the Apache Teaclave Java TEE SDK, simplifying the lifecycle management and invocation of SGX enclaves from Java.
- **Differential Privacy**: Built-in implementations of differentially private algorithms (e.g., Binary Aggregation Tree mechanisms) for secure data aggregation and analytics. This implementation is based on the paper _"Differentially Private Stream Processing at Scale"_ (DOI: [10.48550/arXiv.2303.18086](https://doi.org/10.48550/arXiv.2303.18086)).
- **Cryptographic Utilities**: Comprehensive set of tools for secure serialization, encryption, and authenticated communication between the untrusted Host and the trusted Enclave.

## Security Features

Confidential Storm includes active defenses against global adversaries controlling the infrastructure:

- **Packet Replay Protection**: Implements a sliding window mechanism (default size: 128) to track sequence numbers for each producer. This effectively prevents adversaries from replaying old, valid encrypted tuples to skew computation results.
- **Routing Validation**: leverages Authenticated Encryption with Associated Data (AEAD) to cryptographically bind tuples to their specific source and destination components. The Enclave verifies this metadata (AAD) upon decryption to ensure the topology has not been tampered with and that data is flowing along the authorized path.

## Project Structure

The project is organized into a modular architecture to ensure separation of concerns between trusted and untrusted components:

- **`confidentialstorm/common`**: Contains shared interfaces, API models, and topology specifications used by both Host and Enclave. This ensures API consistency across the trust boundary.
- **`confidentialstorm/host`**: Provides the host-side infrastructure, including the Storm topology components (Bolts/Spouts) and the Enclave Bridge logic for managing enclave lifecycles and proxying calls.
- **`confidentialstorm/enclave`**: Houses the trusted logic that executes within the SGX Enclave. This includes service implementations, cryptographic operations, and differential privacy mechanisms.
- **`examples/`**: A collection of reference implementations demonstrating various use cases (e.g., `confidential-word-count`, `synthetic-dp-histogram`).

## Architecture and SGX Integration

This library relies on the Apache `teaclave-java-tee-sdk` to execute Java code within an isolated environment using [SubstrateVM](https://github.com/oracle/graal/tree/master/substratevm).

### 1. Storm Integration & Enclave Lifecycle

Confidential Storm is designed to fit naturally into the Apache Storm architecture.

- **Topology Structure**: An application is defined as a standard Storm **Topology**, a graph of computation where nodes are **Spouts** (data sources) and **Bolts** (data processors).
- **Confidential Components**: Developers extend `ConfidentialBolt` or `ConfidentialSpout` instead of the standard classes. These components act as gateways to the secure world.
- **Parallelism Mapping (1:1)**:
  - In Storm, parallelism is achieved by spawning multiple **Executors** (threads) across multiple **Worker Processes** (JVMs).
  - Confidential Storm maintains a strict **1:1 mapping between a Storm Task and an SGX Enclave**.
  - When a `ConfidentialBolt` is initialized (during the `prepare()` phase of a Task), it spins up its own isolated SGX Enclave instance.
  - This ensures that parallelism in the trusted world scales linearly with the parallelism defined in the Storm topology.

### 2. Module Interconnection

> **Note**: This project utilizes a custom adaptation of the `teaclave-java-tee-sdk` build scripts to support **Java 17** (originally limited to Java 11). This involves specific handling of GraalVM 22.2.0 native image generation and linking against updated static libraries.

An application built with Confidential Storm typically consists of three main modules:

- **Enclave**
  - Contains sensitive business logic (e.g., `ServiceImpl`).
  - **Build Time**: Compiled into a native shared object (`.so`) using [GraalVM](https://www.graalvm.org/) via the native-maven-plugin.
  - **Signing**: The build process signs the `.so` file to produce a `.signed` enclave binary.
  - **Packaging**: The signed binary is embedded as a resource within the Host JAR or placed on the classpath.

- **Host**:
  - Standard Java application running on the Host JVM.
  - **Context**: In Confidential Storm, this module primarily contains the **Topology Specification** and related configurations.
  - Contains the **Enclave Bridge** (`TeeSdkEnclave.java`), acting as the controller.
  - Manages the lifecycle of the enclave (creation, destruction) and proxies method calls to it.

- **Common**:
  - Contains Service Interfaces annotated with `@EnclaveService`.
  - Shared by Host and Enclave to define the API contract.

### 3. Execution Flow (Host-Enclave Bridge)

The runtime execution follows a strict sequence to ensure security:

#### A. Initialization (Host Side)

1. **Bootstrap**: The Host application starts.
2. **Extraction**: The system extracts the embedded `.signed` enclave binary and the JNI bridge library (`lib_jni_tee_sdk_svm.so`) to a temporary location.
3. **Loading**: The `nativeCreateEnclave` function (via JNI) instructs the SGX driver to load the enclave into protected memory.
4. **JVM Attachment**: The Host calls `nativeSvmAttachIsolate`, initializing the SubstrateVM inside the enclave.

#### B. Service Loading

5. **Proxy Creation**: Upon calling `enclave.load(Service.class)`, the Host creates a Java **Dynamic Proxy** for the interface.
6. **Registration**: The service name is sent to the enclave, which uses reflection to instantiate the corresponding implementation class.

#### C. Method Invocation (Host -> Enclave)

When a method is called on the service proxy:

7. **Intercept**: The `ProxyEnclaveInvocationHandler` intercepts the call.
8. **Serialize**: Arguments and method metadata are serialized.
9. **ECALL**: A native JNI function (`nativeInvokeMethod`) performs an SGX ECALL (Entrance Call), transitioning execution to the enclave.
10. **Deserialize (Enclave)**: The C entry point inside the enclave calls the internal JVM.
11. **Execute**: The internal JVM deserializes arguments, invokes the actual method via Reflection, and captures the result.
12. **Return**: The result is serialized and returned (OCALL/Return) to the Host for deserialization.

```
[ HOST JVM ]                                     [ SGX ENCLAVE ]
      |                                                |
      | 1. EnclaveFactory.create()                     |
      |----------------------------------------------->| (Loads .signed binary)
      |                                                |
      | 2. Attach Internal JVM                         |
      |----------------------------------------------->| (Starts SubstrateVM)
      |                                                |
      | 3. service.method()                            |
      |    (Proxy Intercept)                           |
      |          |                                     |
      |    (Serialize Args)                            |
      |          |                                     |
      |    (JNI -> ECALL) ---------------------------->| (Enclave Entry Point)
      |                                                |       |
      |                                                | (Deserialize Args)
      |                                                |       |
      |                                                | (Reflection Invoke ServiceImpl)
      |                                                |       |
      |    (Return Result) <---------------------------| (Serialize Result)
      |                                                |
```

### Native Components

- **`tee_sdk_wrapper`**: A native bridge running inside the enclave. It manages the SubstrateVM lifecycle and exposes C entry points for ECALLs. It also handles memory allocation from the host via `ocall_malloc` to return data.
- **`sgx_mmap`**: Provides the memory management interface required by GraalVM. It wraps POSIX `mmap`/`munmap` calls, redirecting them to the enclave's internal memory manager, as standard system calls are forbidden within SGX.

## Usage

### Prerequisites

- **Java**: [GraalVM Community Edition 22.2.0](https://www.graalvm.org/) (which includes OpenJDK 17).
- **Build Tools**: Maven 3.9+.
- **Environment**: Intel SGX-capable hardware with installed PSW/driver, or configured Simulation Mode.
- **Dependencies**: Apache Teaclave Java TEE SDK 0.1.0 and standard C/C++ toolchain (`gcc`, `make`, `cmake`).
- **Development Tools**: [Go-Task](https://taskfile.dev) is recommended for managing the development environment and executing common tasks (defined in `taskfile.yml`).

> **Note**: A fully configured development environment is available via the provided Dev Container configuration (`.devcontainer/`). This container includes all necessary dependencies, such as the SGX SDK, Teaclave runtime, GraalVM, Maven, and Storm CLI.

### Building the Library

To build the core library modules:

```bash
mvn clean install -DskipTests
```

To build with native enclave compilation (requires SGX environment):

```bash
mvn clean install -DskipTests -Pnative
```

### Developing a Confidential Application

1. **Define Interfaces**: Create your service interfaces in a `common` module.
2. **Implement Enclave Logic**: Implement these interfaces in an `enclave` module, utilizing `confidentialstorm-enclave` dependencies.
3. **Create Topology**: In your `host` module, extend `ConfidentialBolt` or `ConfidentialSpout`. Use the `EnclaveManager` to load services and delegate tuple processing to the enclave.

## Examples

The `examples/` directory contains complete reference implementations:

- **`confidential-word-count`**: The most idiomatic example, designed to showcase a production-ready implementation.
  - **Full Security**: Enables all security features, including **Packet Replay Protection** and **Routing Validation**, via `WordCountEnclaveConfigProvider`.
  - **Secure Logic**: Implements the same robust `StreamingDPMechanism` as the synthetic example for its global histogram aggregation, proving that differential privacy can be seamlessly integrated into standard business logic.
  - **Architecture**: Demonstrates a realistic multi-stage topology where sensitive data (jokes) is ingested, securely split (`SplitSentenceService`), counted (`WordCountService`), and aggregated (`HistogramService`) entirely within enclaves, with strict type checking and contribution bounding.

- **`synthetic-dp-histogram`**: A faithful reproduction of the benchmark experiment from the "Differentially Private Stream Processing at Scale" paper (Section 5.1).
  - **Benchmark Focused**: Intentionally disables some security features (replay protection, routing validation) to match the paper's experimental setup and isolate the performance of the DP algorithms.
  - **Dynamic Configuration**: Demonstrates how to use the **Service API** (`SyntheticHistogramService#configure`) to pass runtime configuration (e.g., `maxTimeSteps`, `mu`) from the Host topology to the Enclave, allowing for flexible experiments without recompiling the enclave.
  - **Advanced DP**: Features the full `StreamingDPMechanism` with hierarchical perturbation and sliding window aggregation, generating privacy-preserving histograms from high-volume synthetic Zipfian data.

- **`simple-topology`**: A minimal "Hello World" example showcasing basic Host-Enclave communication.
  - **Connectivity Check**: Features a simple `SimpleEnclaveService` that reverses a string, serving as a baseline for verifying infrastructure connectivity and understanding the request/response flow without complex business logic.

For detailed instructions on running specific examples, refer to the `README.md` within each example directory.
