
WORKDIR := confidentialstorm
ROOT_POM := $(WORKDIR)/pom.xml
RUNNER_SCRIPT := $(WORKDIR)/run-local.sh

MVN ?= mvn
MAVEN_GOALS ?= package
MAVEN_FLAGS ?= -DskipTests
# Toggle native profile (requires SGX-enabled env) via `make ENCLAVE_PROFILE= enclave`.
ENCLAVE_PROFILE ?= -Pnative

.PHONY: all build help clean common enclave host submit run-local

all: build

help:
	@echo "Make targets:"
	@echo "  make [all|build]   - Build common, enclave (native), and host artifacts"
	@echo "  make common        - Build the shared Java library"
	@echo "  make enclave       - Build enclave artifacts (default profile: $(ENCLAVE_PROFILE))"
	@echo "  make host          - Build the Storm topology (depends on enclave output)"
	@echo "  make clean         - Run 'mvn clean' for the whole aggregate project"
	@echo ""
	@echo "Variables:"
	@echo "  MVN=<path>                Override Maven binary (default: mvn)"
	@echo "  MAVEN_FLAGS='<flags>'     Extra flags (default: $(MAVEN_FLAGS))"
	@echo "  ENCLAVE_PROFILE='<flags>' Profile(s) to pass while building enclave"

build: common enclave host

build-streamlined:
	$(MVN) -f $(ROOT_POM) $(MAVEN_FLAGS) $(ENCLAVE_PROFILE) clean $(MAVEN_GOALS)

clean:
	$(MVN) -f $(ROOT_POM) clean

common:
	$(MVN) -f $(ROOT_POM) -pl common -am $(MAVEN_FLAGS) $(MAVEN_GOALS)

enclave:
	$(MVN) -f $(ROOT_POM) -pl enclave -am $(MAVEN_FLAGS) $(ENCLAVE_PROFILE) $(MAVEN_GOALS)

host:
	$(MVN) -f $(ROOT_POM) -pl host -am $(MAVEN_FLAGS) $(MAVEN_GOALS)

run-local:
	@echo "Running Storm topology locally..."
	@sudo storm local confidentialstorm/host/target/confidentialstorm-topology.jar ch.usi.inf.confidentialstorm.WordCountTopology -- --local
	@echo "Finished local run."

submit:
	@echo "Submitting Storm topology to cluster..."
	@storm jar confidentialstorm/host/target/confidentialstorm-topology.jar ch.inf.usi.confidentialstorm.WordCountTopology -- --cluster
	@echo "Finished submission.";