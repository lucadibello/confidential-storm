ROOT_POM := pom.xml

include ../Makefile.common

MVN ?= mvn
MAVEN_GOALS ?= package
MAVEN_FLAGS ?= -DskipTests
ENCLAVE_PROFILE ?= -Pnative

TOPOLOGY_JAR   := host/target/synthetic-dp-histogram-host-1.0-SNAPSHOT.jar
TOPOLOGY_CLASS := ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology
TOPOLOGY_NAME  := synthetic-dp-histogram

.PHONY: all build help clean common enclave host \
        run-local run-test submit \
        cluster-submit cluster-kill

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
	@echo "  make submit           - Submit the topology to a cluster via storm jar"
	@echo ""
	@echo "  Docker cluster (requires 'make cluster-up'):"
	@echo "    make cluster-submit - Build and submit topology to the Docker Storm cluster"
	@echo "    make cluster-kill   - Kill the running topology on the cluster"
	@echo "    make cluster-up     - Start the Storm Docker cluster"
	@echo "    make cluster-down   - Stop the Storm Docker cluster"
	@echo "    make cluster-status - Show cluster and topology status"
	@echo ""
	@echo "Variables:"
	@echo "  MVN=<path>                Override Maven binary (default: mvn)"
	@echo "  MAVEN_FLAGS='<flags>'     Extra flags (default: $(MAVEN_FLAGS))"
	@echo "  ENCLAVE_PROFILE='<flags>' Profile(s) to pass while building enclave"

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

submit: build
	@echo "Submitting Synthetic DP topology to cluster..."
	@storm jar $(TOPOLOGY_JAR) \
	  $(TOPOLOGY_CLASS) -- --cluster
	@echo "Finished submission."

# ---------------------------------------------------------------------------
# Docker cluster submission
# ---------------------------------------------------------------------------

cluster-submit: build
	@echo "Submitting $(TOPOLOGY_NAME) to Docker Storm cluster (nimbus=$(NIMBUS_HOST))..."
	storm jar $(TOPOLOGY_JAR) $(TOPOLOGY_CLASS) -- --cluster
	@echo "Topology submitted. Check status with: make cluster-status"

cluster-kill:
	@echo "Killing topology '$(TOPOLOGY_NAME)' on the cluster..."
	storm kill $(TOPOLOGY_NAME) -w 5
	@echo "Kill request sent (5 s wait)."
