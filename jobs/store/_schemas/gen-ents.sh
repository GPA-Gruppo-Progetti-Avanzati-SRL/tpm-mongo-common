#!/bin/sh
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name SysInfo --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name Partition --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name TaskReference --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name Task --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name Job --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name DueDateTriggerCheckPoint --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name TaskLog --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name LogEntry --out-dir ../../../ --with-format
tpm-morphia-cli gen all-entities --schema-file ./schema.yml  --out-dir ../../../ --with-format
