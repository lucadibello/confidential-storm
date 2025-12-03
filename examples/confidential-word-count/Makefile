ROOT_POM := pom.xml
TOOLS_DIR := ./tools
DATASET_DIR := $(TOOLS_DIR)/dataset
DATASET_RAW := $(DATASET_DIR)/jokes.json
DATASET_WITH_USERS := $(DATASET_DIR)/out/jokes.with_user_ids.json
MAX_CONTRIB_TOOL := $(TOOLS_DIR)/max-contribution-estimation/main.py

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
	@sudo storm local -c topology.debug=true --local-ttl 120 host/target/confidential-word-count-topology.jar ch.usi.inf.examples.confidential_word_count.host.WordCountTopology -- --local
	@echo "Finished local run."

seal-dataset:
	@echo "Sealing dataset (encrypting jokes.json to jokes.enc.json)..."
	$(MAKE) dataset-with-users
	@STREAM_KEY_HEX=$$(cat $(DATASET_DIR)/stream_key_hex) \
		python3 $(DATASET_DIR)/seal-dataset/main.py $(DATASET_WITH_USERS) host/src/main/resources/jokes.enc.json
	@echo "Finished encryption."

percentile:
	@echo "Computing per-user word-count percentile (default p=0.99)..."
	$(MAKE) dataset-with-users
	@python3 $(MAX_CONTRIB_TOOL) $(DATASET_WITH_USERS)
	@echo "Done."

dataset-with-users:
	@echo "Generating dataset with user_id annotations..."
	@mkdir -p $(dir $(DATASET_WITH_USERS))
	@python3 $(DATASET_DIR)/regen-dataset/main.py $(DATASET_RAW) $(DATASET_WITH_USERS)
	@echo "Dataset with user_ids written to $(DATASET_WITH_USERS)"

submit:
	@echo "Submitting Storm topology to cluster..."
	@storm jar host/target/confidential-word-count-topology.jar ch.usi.inf.examples.confidential_word_count.host.WordCountTopology -- --cluster
	@echo "Finished submission."
