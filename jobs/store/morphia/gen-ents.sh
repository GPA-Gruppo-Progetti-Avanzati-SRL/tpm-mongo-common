#!/bin/sh

# tpm-morphia-cli gen entity --schema-file ./schema.yml --name Partition --out-dir ../../../ --with-format
# tpm-morphia-cli gen entity --schema-file ./schema.yml --name Task --out-dir ../../../ --with-format
tpm-morphia-cli gen all-entities --schema-file ./schema.yml  --out-dir ../../../ --with-format
