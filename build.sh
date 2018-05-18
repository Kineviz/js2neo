#!/usr/bin/env bash

ROOT=$(dirname $0)

cp -a ${ROOT}/src/js2neo.js ${ROOT}/docs/dist/js2neo.js
uglifyjs src/js2neo.js --compress --mangle --output ${ROOT}/docs/dist/js2neo.min.js && ls -l ${ROOT}/docs/dist
