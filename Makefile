BUILD_DIR = build
NPROC    := $(shell nproc)

.PHONY: all debug release test clean

all: debug

debug:
	@cmake -S . -B $(BUILD_DIR)/debug -DCMAKE_BUILD_TYPE=Debug -DCMAKE_EXPORT_COMPILE_COMMANDS=ON 2>&1 | tail -5
	$(MAKE) -C $(BUILD_DIR)/debug -j$(NPROC)

release:
	@cmake -S . -B $(BUILD_DIR)/release -DCMAKE_BUILD_TYPE=RelWithDebInfo 2>&1 | tail -5
	$(MAKE) -C $(BUILD_DIR)/release -j$(NPROC)

test: debug
	./$(BUILD_DIR)/debug/motlite_test

clean:
	rm -rf build
