# SPDX-License-Identifier: Apache-2.0
# Copyright 2026 BoanLab @ DKU

.PHONY: up down check all clean

up:
	./test.sh up

check:
	./test.sh check

down:
	./test.sh down

all:
	./test.sh all

clean: down
	rm -rf artifacts
