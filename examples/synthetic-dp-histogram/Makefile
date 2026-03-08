ROOT_POM := pom.xml

include ../Makefile.common

MVN ?= mvn
MAVEN_GOALS ?= package
MAVEN_FLAGS ?= -DskipTests
ENCLAVE_PROFILE ?= -Pnative

TOPOLOGY_JAR   := host/target/synthetic-dp-histogram-host-1.0-SNAPSHOT.jar
TOPOLOGY_CLASS := ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology
TOPOLOGY_NAME  := SyntheticDP1

.PHONY: all build help clean common enclave host \
        run-local run-test build-profiled run-profiled cluster-submit-profiled

all: build

help:
	@echo "Make targets:"
	@echo "  make [all|build]      - Build common, enclave (native), and host artifacts"
	@echo "  make common           - Build the shared Java library"
	@echo "  make enclave          - Build enclave artifacts (default profile: $(ENCLAVE_PROFILE))"
	@echo "  make host             - Build the Storm topology (depends on enclave output)"
	@echo "  make clean            - Run 'mvn clean' for the example project"
	@echo "  make run-local        - Run locally in benchmark mode (parallelism=8)"
	@echo "  make run-test         - Run locally in test mode (parallelism=1)"
	@echo "  make build-profiled   - Build with profiling baked in (compile-time constants)"
	@echo "  make run-profiled     - Build with profiling + run locally (test mode)"
	@echo ""
	@echo "  Docker cluster (requires 'make cluster-up'):"
	@echo "    make cluster-submit    - Build and submit topology to the Docker Storm cluster"
	@echo "    make cluster-kill      - Kill the running topology on the cluster"
	@echo "    make cluster-up        - Start the Storm Docker cluster"
	@echo "    make cluster-down      - Stop the Storm Docker cluster"
	@echo "    make cluster-status    - Show cluster and topology status"
	@echo "    make cluster-logs-local - Tail worker logs from host filesystem"
	@echo ""
	@echo "Variables:"
	@echo "  MVN=<path>                Override Maven binary (default: mvn)"
	@echo "  MAVEN_FLAGS='<flags>'     Extra flags (default: $(MAVEN_FLAGS))"
	@echo "  ENCLAVE_PROFILE='<flags>' Profile(s) to pass while building enclave"
	@echo ""
	@echo "Profiling variables (build-time, baked into JAR via maven-templating-plugin):"
	@echo "  PROFILER_SAMPLE_RATE=0.05   Fraction of fast ECALLs to time (default: 0.01)"
	@echo "  PROFILER_REPORT_TICKS=5     Ticks between log dumps (default: 10)6
	@echo "  PROFILER_OUTPUT_DIR=/tmp    CSV report output dir (default: /logs/storm)"

build: clean common enclave host

clean:
	$(MVN) -f $(ROOT_POM) clean

common: install-framework
	$(MVN) -f $(ROOT_POM) -pl common -am $(MAVEN_FLAGS) $(MAVEN_GOALS)

enclave: install-framework
	$(MVN) -f $(ROOT_POM) -pl enclave -am $(MAVEN_FLAGS) $(ENCLAVE_PROFILE) $(MAVEN_GOALS)

host: install-framework
	$(MVN) -f $(ROOT_POM) -pl host -am $(MAVEN_FLAGS) $(MAVEN_GOALS)

run-local:
	@echo "Running Synthetic DP topology locally (benchmark mode, parallelism=8)..."
	@sudo storm local --local-ttl 60 $(TOPOLOGY_JAR) \
	  $(TOPOLOGY_CLASS)
	@echo "Finished local run."

run-test:
	@echo "Running Synthetic DP topology locally (test mode, parallelism=1)..."
	@sudo storm local --local-ttl 60 $(TOPOLOGY_JAR) \
	  $(TOPOLOGY_CLASS) -- --test
	@echo "Finished test run."

# Profiling build-time configuration (override on command line, e.g. make build-profiled PROFILER_SAMPLE_RATE=0.05)
# These are baked into the JAR at compile time via maven-templating-plugin.
PROFILER_SAMPLE_RATE ?= 0.05
PROFILER_REPORT_TICKS ?= 10
PROFILER_OUTPUT_DIR ?= /logs/storm

PROFILER_MAVEN_FLAGS := -Dprofiler.enabled=true \
  -Dprofiler.sample.rate=$(PROFILER_SAMPLE_RATE) \
  -Dprofiler.report.interval.ticks=$(PROFILER_REPORT_TICKS) \
  -Dprofiler.output.dir=$(PROFILER_OUTPUT_DIR)

build-profiled:
	@echo "Rebuilding framework + example with profiling baked in..."
	$(MAKE) install-framework EXTRA_FRAMEWORK_FLAGS="$(PROFILER_MAVEN_FLAGS)"
	@echo "Building common, enclave, and host..."
	$(MVN) -f $(ROOT_POM) -pl common -am $(MAVEN_FLAGS) $(MAVEN_GOALS)
	$(MVN) -f $(ROOT_POM) -pl enclave -am $(MAVEN_FLAGS) $(ENCLAVE_PROFILE) $(MAVEN_GOALS)
	$(MVN) -f $(ROOT_POM) -pl host -am $(MAVEN_FLAGS) $(MAVEN_GOALS)
	@echo "Finished profiled build. JAR: $(TOPOLOGY_JAR)"

run-profiled: build-profiled
	@echo "Running Synthetic DP topology locally (profiling enabled, test mode)..."
	@sudo storm local --local-ttl 60 $(TOPOLOGY_JAR) \
	  $(TOPOLOGY_CLASS) -- --test
	@echo "Finished profiled run. Reports in $(PROFILER_OUTPUT_DIR)/profiler-*.csv"

cluster-submit-profiled: build-profiled
	@echo "Submitting $(TOPOLOGY_NAME) to cluster with profiling enabled..."
	storm jar $(TOPOLOGY_JAR) $(TOPOLOGY_CLASS)
	@echo "Topology submitted with profiling. CSV reports will be at $(PROFILER_OUTPUT_DIR)/profiler-*.csv"
