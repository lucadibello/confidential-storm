ROOT_POM := pom.xml

include ../Makefile.common

MVN ?= mvn
MAVEN_GOALS ?= package
MAVEN_FLAGS ?= -DskipTests

TOPOLOGY_JAR   := target/synthetic-benchmark-baseline-1.0-SNAPSHOT.jar
TOPOLOGY_CLASS := ch.usi.inf.examples.synthetic_baseline.SyntheticBaselineTopology
TOPOLOGY_NAME  ?= SyntheticDPBaseline1

# Topology arguments passed to the topology main class (e.g. --num-users 100000 --parallelism 2)
TOPOLOGY_ARGS ?=

.PHONY: all build help clean \
        run-local run-test build-profiled run-profiled cluster-submit cluster-submit-profiled \
        grid-search comparison-grid-search analyze

all: build

help:
	@echo "Make targets (Baseline — No SGX):"
	@echo "  make [all|build]      - Build the baseline topology JAR"
	@echo "  make clean            - Run 'mvn clean' for the project"
	@echo "  make run-local        - Run locally in benchmark mode (parallelism=8)"
	@echo "  make run-test         - Run locally in test mode (parallelism=1)"
	@echo "  make build-profiled   - Build with profiling baked in (compile-time constants)"
	@echo "  make run-profiled     - Build with profiling + run locally (test mode)"
	@echo ""
	@echo "  Docker cluster (requires 'make cluster-up'):"
	@echo "    make cluster-submit              - Build and submit topology to the cluster"
	@echo "    make cluster-submit-profiled     - Build with profiling and submit to the cluster"
	@echo "    make cluster-kill                - Kill the running topology on the cluster"
	@echo "    make cluster-up                  - Start the Storm Docker cluster"
	@echo "    make cluster-down                - Stop the Storm Docker cluster"
	@echo "    make cluster-status              - Show cluster and topology status"
	@echo "    make cluster-logs-local          - Tail worker logs from host filesystem"
	@echo ""
	@echo "  Benchmarking:"
	@echo "    make grid-search                 - Run baseline-only grid search"
	@echo "    make comparison-grid-search      - Run baseline vs enclave A/B comparison"
	@echo "    make analyze                     - Analyze results with Python script"
	@echo ""
	@echo "Variables:"
	@echo "  MVN=<path>                Override Maven binary (default: mvn)"
	@echo "  MAVEN_FLAGS='<flags>'     Extra flags (default: $(MAVEN_FLAGS))"
	@echo "  TOPOLOGY_ARGS='<args>'    Arguments forwarded to topology main class"
	@echo "                            e.g. TOPOLOGY_ARGS='--num-users 100000 --parallelism 2'"
	@echo ""
	@echo "Profiling variables (build-time, baked into JAR via maven-templating-plugin):"
	@echo "  PROFILER_SAMPLE_RATE=0.05   Fraction of operations to time (default: 0.01)"
	@echo "  PROFILER_REPORT_TICKS=5     Ticks between log dumps (default: 10)"
	@echo "  PROFILER_OUTPUT_DIR=/tmp    CSV report output dir (default: /logs/storm/profiler)"

build: clean install-framework
	$(MVN) -f $(ROOT_POM) $(MAVEN_FLAGS) $(MAVEN_GOALS)

clean:
	$(MVN) -f $(ROOT_POM) clean

run-local:
	@echo "Running Baseline DP topology locally (benchmark mode, parallelism=8)..."
	@sudo storm local --local-ttl 60 $(TOPOLOGY_JAR) \
	  $(TOPOLOGY_CLASS)
	@echo "Finished local run."

run-test:
	@echo "Running Baseline DP topology locally (test mode, parallelism=1)..."
	@sudo storm local --local-ttl 60 $(TOPOLOGY_JAR) \
	  $(TOPOLOGY_CLASS) -- --test
	@echo "Finished test run."

# Profiling build-time configuration (override on command line, e.g. make build-profiled PROFILER_SAMPLE_RATE=0.05)
# These are baked into the JAR at compile time via maven-templating-plugin.
PROFILER_SAMPLE_RATE ?= 0.05
PROFILER_REPORT_TICKS ?= 10
PROFILER_OUTPUT_DIR ?= /logs/storm/profiler

PROFILER_MAVEN_FLAGS := -Dprofiler.enabled=true \
  -Dprofiler.sample.rate=$(PROFILER_SAMPLE_RATE) \
  -Dprofiler.report.interval.ticks=$(PROFILER_REPORT_TICKS) \
  -Dprofiler.output.dir=$(PROFILER_OUTPUT_DIR)

build-profiled:
	@echo "Rebuilding framework + baseline with profiling baked in..."
	$(MAKE) install-framework EXTRA_FRAMEWORK_FLAGS="$(PROFILER_MAVEN_FLAGS)"
	$(MVN) -f $(ROOT_POM) clean $(MAVEN_FLAGS) $(PROFILER_MAVEN_FLAGS) $(MAVEN_GOALS)
	@echo "Finished profiled build. JAR: $(TOPOLOGY_JAR)"

run-profiled: build-profiled
	@echo "Running Baseline DP topology locally (profiling enabled, test mode)..."
	@sudo storm local --local-ttl 60 $(TOPOLOGY_JAR) \
	  $(TOPOLOGY_CLASS) -- --test
	@echo "Finished profiled run. Reports in $(PROFILER_OUTPUT_DIR)/profiler-*.csv"

cluster-submit-profiled: build-profiled
	@echo "Submitting $(TOPOLOGY_NAME) to cluster with profiling enabled..."
	storm jar $(TOPOLOGY_JAR) $(TOPOLOGY_CLASS) $(TOPOLOGY_ARGS)
	@echo "Topology submitted with profiling. CSV reports will be at $(PROFILER_OUTPUT_DIR)/profiler-*.csv"

grid-search:
	@echo "Starting baseline grid search benchmark..."
	@python3 ../scripts/run-grid-search.py --mode baseline

comparison-grid-search:
	@echo "Starting baseline vs enclave comparison grid search..."
	@python3 ../scripts/run-grid-search.py --mode comparison

analyze:
	@python3 scripts/analyze-results.py
