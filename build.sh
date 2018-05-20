#!/usr/bin/env bash

ROOT="$(dirname $0)"
VERSION="$(node ${ROOT}/src/js2neo.js)"
HEADER="/*\n * ${VERSION}\n * Copyright (C) 2011-2018 Nigel Small\n * Licensed under the Apache License, Version 2.0\n */\n"

cp -a ${ROOT}/src/js2neo.js ${ROOT}/docs/dist/js2neo.js

uglifyjs src/js2neo.js --config-file ${ROOT}/uglify.conf.json > ${ROOT}/docs/dist/js2neo.min.js
sed -i "1s;^;${HEADER};" ${ROOT}/docs/dist/js2neo.min.js

ls -l ${ROOT}/docs/dist
