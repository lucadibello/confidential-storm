
WORKDIR := confidentialstorm
ROOT_POM := $(WORKDIR)/pom.xml
RUNNER_SCRIPT := $(WORKDIR)/run-local.sh

MVN ?= mvn
MAVEN_GOALS ?= install
MAVEN_FLAGS ?= -DskipTests
# Toggle native profile (requires SGX-enabled env) via `make ENCLAVE_PROFILE= enclave`.
ENCLAVE_PROFILE ?= -Pnative

.PHONY: all build help clean common enclave host \
        test test-enclave test-common test-host \
        test-debug test-method test-method-debug

all: build

help:
	@echo "Make targets:"
	@echo "  make [all|build]   - Build common, enclave (native), and host artifacts"
	@echo "  make common        - Build the shared Java library"
	@echo "  make enclave       - Build enclave artifacts (default profile: $(ENCLAVE_PROFILE))"
	@echo "  make host          - Build the Storm topology (depends on enclave output)"
	@echo "  make clean         - Run 'mvn clean' for the whole aggregate project"
	@echo ""
	@echo ""
	@echo "Test targets (JUnit 6, Surefire + ConsoleLauncher):"
	@echo "  make test                 - Run Surefire tests across all submodules (CI-style)."
	@echo "  make test-common          - Run Surefire tests in common only."
	@echo "  make test-enclave         - Run Surefire tests in enclave only."
	@echo "  make test-host            - Run Surefire tests in host only."
	@echo "  make test-debug MODULE=<m> - Run tests via ConsoleLauncher (raw stdout; no Surefire)."
	@echo "  make test-method MODULE=<m> CLASS=<FQCN> METHOD=<name>"
	@echo "                            - Run one method via Surefire (uses -Dtest=FQCN#METHOD)."
	@echo "  make test-method-debug MODULE=<m> CLASS=<FQCN> METHOD=<name>"
	@echo "                            - Run one method via ConsoleLauncher (raw logs)."
	@echo ""
	@echo "Variables:"
	@echo "  MVN=<path>                Override Maven binary (default: mvn)"
	@echo "  MAVEN_FLAGS='<flags>'     Extra flags (default: $(MAVEN_FLAGS))"
	@echo "  ENCLAVE_PROFILE='<flags>' Profile(s) to pass while building enclave"
	@echo "  MODULE=<common|enclave|host>  Submodule for test-debug / test-method* (default: enclave)"
	@echo "  CLASS=<FQCN>              Fully-qualified test class (e.g. ch.usi.inf....MyTest)"
	@echo "  METHOD=<name>             Test method name"

build: clean common enclave host

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

# ------------------------------------------------------------------
# Test targets
# ------------------------------------------------------------------
# MODULE selects the submodule for test-debug / test-method*.
# Defaults to enclave since that is the submodule with the most unit tests.
MODULE ?= enclave

# All three submodules, in reactor order. Each should be Surefire-runnable.
test:
	$(MVN) -f $(ROOT_POM) -am test

test-common:
	$(MVN) -f $(ROOT_POM) -pl common -am test

test-enclave:
	$(MVN) -f $(ROOT_POM) -pl enclave -am $(ENCLAVE_PROFILE) test

test-host:
	$(MVN) -f $(ROOT_POM) -pl host -am test

# Run all tests in MODULE via ConsoleLauncher so that raw stdout/stderr
# (e.g. enclave logs) is visible without Surefire redirection.
# Requires test-classes to be built first (mvn test-compile).
test-debug:
	$(MVN) -f $(ROOT_POM) -pl $(MODULE) -am test-compile
	@TEST_CP=$$($(MVN) -f $(WORKDIR)/$(MODULE)/pom.xml -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout 2>/dev/null); \
	java -cp "$(WORKDIR)/$(MODULE)/target/classes:$(WORKDIR)/$(MODULE)/target/test-classes:$$TEST_CP" \
		org.junit.platform.console.ConsoleLauncher execute \
		--scan-classpath \
		--disable-banner \
		--details=testfeed

# Run a single test method via Surefire (quiet, CI-style).
# Usage: make test-method MODULE=enclave CLASS=ch.usi.inf....FooTest METHOD=bar
test-method:
	@if [ -z "$(CLASS)" ] || [ -z "$(METHOD)" ]; then \
		echo "Usage: make test-method MODULE=<m> CLASS=<FQCN> METHOD=<name>"; exit 2; \
	fi
	$(MVN) -f $(ROOT_POM) -pl $(MODULE) -am test -Dtest="$(CLASS)#$(METHOD)"

# Run a single test method via ConsoleLauncher (raw logs).
# Usage: make test-method-debug MODULE=enclave CLASS=ch.usi.inf....FooTest METHOD=bar
test-method-debug:
	@if [ -z "$(CLASS)" ] || [ -z "$(METHOD)" ]; then \
		echo "Usage: make test-method-debug MODULE=<m> CLASS=<FQCN> METHOD=<name>"; exit 2; \
	fi
	$(MVN) -f $(ROOT_POM) -pl $(MODULE) -am test-compile
	@TEST_CP=$$($(MVN) -f $(WORKDIR)/$(MODULE)/pom.xml -q dependency:build-classpath -DincludeScope=test -Dmdep.outputFile=/dev/stdout 2>/dev/null); \
	java -cp "$(WORKDIR)/$(MODULE)/target/classes:$(WORKDIR)/$(MODULE)/target/test-classes:$$TEST_CP" \
		org.junit.platform.console.ConsoleLauncher execute \
		--select-method="$(CLASS)#$(METHOD)" \
		--disable-banner \
		--details=testfeed
