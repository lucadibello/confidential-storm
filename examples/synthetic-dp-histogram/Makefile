ROOT_POM := pom.xml

include ../Makefile.common

MVN ?= mvn
MAVEN_GOALS ?= package
MAVEN_FLAGS ?= -DskipTests
ENCLAVE_PROFILE ?= -Pnative

.PHONY: all build help clean common enclave host run-local submit

all: build

help:
	@echo "Make targets:"
	@echo "  make [all|build]   - Build common, enclave (native), and host artifacts"
	@echo "  make common        - Build the shared Java library"
	@echo "  make enclave       - Build enclave artifacts (default profile: $(ENCLAVE_PROFILE))"
	@echo "  make host          - Build the Storm topology (depends on enclave output)"
	@echo "  make clean         - Run 'mvn clean' for the example project"
	@echo "  make run-local     - Run the topology locally via storm CLI"
	@echo "  make submit        - Submit the topology to a cluster via storm jar"
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
	@echo "Running Synthetic DP topology locally with debug logging (60 seconds)..."
	@sudo storm local -c topology.debug=true --local-ttl 60 host/target/synthetic-dp-histogram-host-1.0-SNAPSHOT.jar \
	  ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology -- --local
	@echo "Finished local run."

submit: build
	@echo "Submitting Synthetic DP topology to cluster..."
	@storm jar host/target/synthetic-dp-histogram-host-1.0-SNAPSHOT.jar \
	  ch.usi.inf.examples.synthetic_dp.host.SyntheticTopology -- --cluster
	@echo "Finished submission."
