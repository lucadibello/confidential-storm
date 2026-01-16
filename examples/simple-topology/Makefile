ROOT_POM := pom.xml

include ../Makefile.common

MVN ?= mvn
MAVEN_GOALS ?= package
MAVEN_FLAGS ?= -DskipTests
# Toggle native profile (requires SGX-enabled env) via `make ENCLAVE_PROFILE= enclave`.
ENCLAVE_PROFILE ?= -Pnative

.PHONY: all build help clean common enclave host submit run-local seal-dataset test-enclave

all: build

help:
	@echo "Make targets:"
	@echo "  make [all|build]   - Build common, enclave (native), and host artifacts"
	@echo "  make common        - Build the shared Java library"
	@echo "  make enclave       - Build enclave artifacts (default profile: $(ENCLAVE_PROFILE))"
	@echo "  make host          - Build the Storm topology (depends on enclave output)"
	@echo "  make clean         - Run 'mvn clean' for the example project"
	@echo "  make run-local     - Run the topology locally"
	@echo "  make seal-dataset  - Encrypt the dataset (requires python3)"
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

test-enclave: install-framework
	$(MVN) -f $(ROOT_POM) -pl enclave $(ENCLAVE_PROFILE) test

host: install-framework
	$(MVN) -f $(ROOT_POM) -pl host -am $(MAVEN_FLAGS) $(MAVEN_GOALS)

run-local:
	@echo "Running Storm topology locally with debug logging (120 seconds)..."
	@sudo storm local -c topology.debug=true --local-ttl 120 host/target/simple-topology.jar ch.usi.inf.examples.simple_topology.host.SimpleTopology -- --local
	@echo "Finished local run."

submit:
	@echo "Submitting Storm topology to cluster..."
	@storm jar host/target/simple-topology.jar ch.usi.inf.examples.simple_topology.host.SimpleTopology -- --cluster
	@echo "Finished submission."
