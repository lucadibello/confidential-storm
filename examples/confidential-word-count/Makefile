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
# Encryption scheme for dataset sealing. Must match EncryptionScheme in the enclave config.
# Valid values: CHACHA20_POLY1305 (default), AES_256_GCM, NONE
ENCRYPTION_SCHEME ?= CHACHA20_POLY1305

TOPOLOGY_JAR   := host/target/confidential-word-count-topology.jar
TOPOLOGY_CLASS := ch.usi.inf.examples.confidential_word_count.host.WordCountTopology
TOPOLOGY_NAME  := confidential-word-count

.PHONY: all build help clean common generate-graph enclave host run-local \
       seal-dataset seal-chacha20 seal-aes256gcm seal-none \
       test-enclave dataset-with-users percentile

all: build

help:
	@echo "Make targets:"
	@echo ""
	@echo "  Build & run"
	@echo "    make [all|build]       - Build common, enclave (native), and host artifacts"
	@echo "    make common            - Build the shared Java library"
	@echo "    make generate-graph    - Generate encrypted topology graph (before enclave build)"
	@echo "    make enclave           - Build enclave artifacts (default profile: $(ENCLAVE_PROFILE))"
	@echo "    make host              - Build the Storm topology (depends on enclave output)"
	@echo "    make clean             - Run 'mvn clean' for the example project"
	@echo "    make run-local         - Run the topology locally"
	@echo "    make test-enclave      - Run enclave unit tests"
	@echo ""
	@echo "  Docker cluster (requires 'make cluster-up'):"
	@echo "    make cluster-submit    - Build and submit topology to the Docker Storm cluster"
	@echo "    make cluster-kill      - Kill the running topology on the cluster"
	@echo "    make cluster-up        - Start the Storm Docker cluster"
	@echo "    make cluster-down      - Stop the Storm Docker cluster"
	@echo "    make cluster-status    - Show cluster and topology status"
	@echo "    make cluster-logs-local - Tail worker logs from host filesystem"
	@echo ""
	@echo "  Dataset sealing (requires python3 + cryptography)"
	@echo "    make seal-dataset      - Seal dataset with ENCRYPTION_SCHEME (default: $(ENCRYPTION_SCHEME))"
	@echo "    make seal-chacha20     - Seal dataset with ChaCha20-Poly1305"
	@echo "    make seal-aes256gcm    - Seal dataset with AES-256-GCM"
	@echo "    make seal-none         - Store dataset as plaintext (benchmarking only)"
	@echo ""
	@echo "  Analysis"
	@echo "    make percentile        - Compute per-user word-count percentile"
	@echo ""
	@echo "Variables:"
	@echo "  MVN=<path>                   Override Maven binary (default: mvn)"
	@echo "  MAVEN_FLAGS='<flags>'        Extra flags (default: $(MAVEN_FLAGS))"
	@echo "  ENCLAVE_PROFILE='<flags>'    Profile(s) to pass while building enclave"
	@echo "  ENCRYPTION_SCHEME=<scheme>   Scheme for seal-dataset (default: $(ENCRYPTION_SCHEME))"
	@echo "                               Values: CHACHA20_POLY1305, AES_256_GCM, NONE"

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------

build: clean common generate-graph enclave host

clean:
	$(MVN) -f $(ROOT_POM) clean

common: install-framework
	$(MVN) -f $(ROOT_POM) -pl common -am $(MAVEN_FLAGS) $(MAVEN_GOALS)

generate-graph: install-framework
	$(MVN) -f $(ROOT_POM) -pl enclave -am $(MAVEN_FLAGS) install
	$(MVN) -f $(ROOT_POM) -pl host -am $(MAVEN_FLAGS) process-classes

enclave: install-framework
	$(MVN) -f $(ROOT_POM) -pl enclave -am $(MAVEN_FLAGS) $(ENCLAVE_PROFILE) $(MAVEN_GOALS)

test-enclave: install-framework
	$(MVN) -f $(ROOT_POM) -pl enclave $(ENCLAVE_PROFILE) test

host: install-framework
	$(MVN) -f $(ROOT_POM) -pl host -am $(MAVEN_FLAGS) $(MAVEN_GOALS)

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------

run-local:
	@echo "Running Storm topology locally with debug logging (120 seconds)..."
	@sudo storm local -c topology.debug=true --local-ttl 120 $(TOPOLOGY_JAR) $(TOPOLOGY_CLASS) -- --local
	@echo "Finished local run."

# ---------------------------------------------------------------------------
# Dataset sealing
# ---------------------------------------------------------------------------

# Generic target — uses ENCRYPTION_SCHEME variable
seal-dataset: dataset-with-users
	@echo "Sealing dataset (scheme=$(ENCRYPTION_SCHEME))..."
	@STREAM_KEY_HEX=$$(cat $(DATASET_DIR)/stream_key_hex) \
		python3 $(DATASET_DIR)/seal-dataset/main.py \
			--scheme $(ENCRYPTION_SCHEME) \
			$(DATASET_WITH_USERS) host/src/main/resources/jokes.enc.json
	@echo "Finished sealing (scheme=$(ENCRYPTION_SCHEME))."

# Convenience shortcuts for each scheme
seal-chacha20:
	$(MAKE) seal-dataset ENCRYPTION_SCHEME=CHACHA20_POLY1305

seal-aes256gcm:
	$(MAKE) seal-dataset ENCRYPTION_SCHEME=AES_256_GCM

seal-none:
	$(MAKE) seal-dataset ENCRYPTION_SCHEME=NONE

# ---------------------------------------------------------------------------
# Dataset preparation & analysis
# ---------------------------------------------------------------------------

dataset-with-users:
	@echo "Generating dataset with user_id annotations..."
	@mkdir -p $(dir $(DATASET_WITH_USERS))
	@python3 $(DATASET_DIR)/regen-dataset/main.py $(DATASET_RAW) $(DATASET_WITH_USERS)
	@echo "Dataset with user_ids written to $(DATASET_WITH_USERS)"

percentile:
	@echo "Computing per-user word-count percentile (default p=0.99)..."
	$(MAKE) dataset-with-users
	@python3 $(MAX_CONTRIB_TOOL) $(DATASET_WITH_USERS)
	@echo "Done."
