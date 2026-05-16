BUILD_DIR = build
NPROC    := $(shell nproc)

.PHONY: all debug release test clean asan tsan stress-asan stress-tsan stress-ddl oracle

all: debug

debug:
	@cmake -S . -B $(BUILD_DIR)/debug -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON 2>&1 | tail -5
	$(MAKE) -C $(BUILD_DIR)/debug -j$(NPROC)

release:
	@cmake -S . -B $(BUILD_DIR)/release -DCMAKE_BUILD_TYPE=RelWithDebInfo 2>&1 | tail -5
	$(MAKE) -C $(BUILD_DIR)/release -j$(NPROC)

asan:
	@cmake -S . -B $(BUILD_DIR)/asan -DCMAKE_BUILD_TYPE=Debug -DMOTLITE_SANITIZE=address 2>&1 | tail -5
	$(MAKE) -C $(BUILD_DIR)/asan -j$(NPROC)

tsan:
	@cmake -S . -B $(BUILD_DIR)/tsan -DCMAKE_BUILD_TYPE=Debug -DMOTLITE_SANITIZE=thread 2>&1 | tail -5
	$(MAKE) -C $(BUILD_DIR)/tsan -j$(NPROC)

# --- Concurrency hardening targets (GAPS.md #4) ---
# Each spawns oro_server from the matching build dir, runs the matching
# harness mode, fails on any sanitizer/oracle/race report.

stress-asan: asan
	ASAN_OPTIONS=detect_leaks=0:abort_on_error=1:halt_on_error=1 \
	./test/harness/run_concurrency_check.sh $(BUILD_DIR)/asan stress \
		--threads 16 --duration 60

stress-tsan: tsan
	TSAN_OPTIONS="halt_on_error=1:second_deadlock_stack=1:suppressions=$(CURDIR)/test/harness/tsan.supp" \
	./test/harness/run_concurrency_check.sh $(BUILD_DIR)/tsan stress \
		--threads 16 --duration 60

stress-ddl: debug
	./test/harness/run_concurrency_check.sh $(BUILD_DIR)/debug stress_ddl \
		--threads 16 --duration 30

oracle: debug
	./test/harness/run_concurrency_check.sh $(BUILD_DIR)/debug oracle \
		--threads 1 --duration 5

test: debug
	./$(BUILD_DIR)/debug/motlite_test

clean:
	rm -rf build
