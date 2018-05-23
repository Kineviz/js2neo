/*
 * Copyright (C) 2011-2018 Nigel Small
 * Licensed under the Apache License, Version 2.0
 */
!function (global) {

    var DAYS = "days",
        ID = "id",
        NANOSECONDS ="nanoseconds",
        PROPERTIES = "properties",
        SECONDS = "seconds",
        TZ = "tz",

        str = String.fromCharCode,
        DataView_ = DataView,
        Uint8Array_ = Uint8Array,
        min = Math.min,

        js2neo = global.js2neo = {

            version: "1",

            Node: function(struct) {
                unwind.call(this, struct, ID, "labels", PROPERTIES);
            },

            Relationship: function(struct) {
                unwind.call(this, struct, ID, "start", "end", "type", PROPERTIES);
            },

            Path: function (struct) {
                var nodes = struct[0],
                    relationships = struct[1],
                    sequence = struct[2],
                    lastNode = nodes[0],
                    nextNode,
                    entities = [lastNode],
                    i,
                    relationshipIndex,
                    r,
                    rel = js2neo.Relationship;
                for (i = 0; i < sequence.length; i += 2) {
                    relationshipIndex = sequence[i];
                    nextNode = nodes[sequence[2 * i + 1]];
                    if (relationshipIndex > 0) {
                        r = relationships[relationshipIndex - 1];
                        entities.push(new rel([r.id, lastNode.id, nextNode.id, r.type, r.properties]));
                    } else {
                        r = relationships[relationships.length + relationshipIndex];
                        entities.push(new rel([r.id, nextNode.id, lastNode.id, r.type, r.properties]));
                    }
                    entities.push(nextNode);
                    lastNode = nextNode
                }
                this.entities = entities;
            },

            Point: function(struct) {
                unwind.call(this, struct, "srid", "x", "y");
            },

            Date: function(struct) {
                unwind.call(this, struct, DAYS);
            },

            Time: function(struct) {
                this.seconds = struct[0] / 1000000000;
                this.tz = struct[1];
            },

            LocalTime: function(struct) {
                this.seconds = struct[0] / 1000000000;
            },

            DateTime: function(struct) {
                unwind.call(this, struct, SECONDS, NANOSECONDS, TZ);
            },

            LocalDateTime: function(struct) {
                unwind.call(this, struct, SECONDS, NANOSECONDS);
            },

            Duration: function(struct) {
                unwind.call(this, struct, "months", DAYS, SECONDS, NANOSECONDS);
            },

            /**
             * Open a connection to a Bolt server.
             *
             * @param settings
             * @returns {Connection}
             */
            open: function(settings) { return new Connection(settings); }

        };

    function unwind(struct) {
        for (var i = 0; i < struct.length; i++)
            this[arguments[i + 1]] = struct[i];
    }

    function encode(text)
    {
        var data = new Uint8Array_(text.length);
        for (var i = 0; i < text.length; i++)
            data[i] = text.charCodeAt(i);
        return data.buffer;
    }

    function UnboundRelationship(struct) {
        unwind.call(this, struct, ID, "type", PROPERTIES);
    }

    var types = {
        0x44: js2neo.Date,
        0x45: js2neo.Duration,
        0x46: js2neo.DateTime,
        0x4E: js2neo.Node,
        0x50: js2neo.Path,
        0x52: js2neo.Relationship,
        0x54: js2neo.Time,
        0x58: js2neo.Point,
        0x64: js2neo.LocalDateTime,
        0x66: js2neo.DateTime,
        0x72: UnboundRelationship,
        0x74: js2neo.LocalTime
    };

    function Connection(args) {
        args = args || {};
        var pub = {
                secure: args.secure || document.location.protocol === "https:",
                user: args.user,
                host: args.host || "localhost",
                port: args.port || 7687,
                userAgent: args.userAgent || "js2neo/" + js2neo.version
            },
            auth = {
                scheme: "basic",
                principal: pub.user,
                credentials: args.password
            },
            pvt = {
                inData: "",
                inChunks: [],
                ready: false,
                socket: new WebSocket((pub.secure ? "wss" : "ws") + "://" + pub.host + ":" + pub.port)
            },
            s = pvt.socket,
            onHandshake = args.onHandshake,
            onInit = args.onInit,
            onOpen = args.onOpen,
            requests = [
                [0x01, [pub.userAgent, auth], {
                    0x70: function () {
                        if (onInit)
                            onInit(pub);
                        pvt.ready = true;
                    },
                    0x7F: args.onInitFailure
                }]
            ],
            handlerMaps = [];

        s.binaryType = "arraybuffer";
        s.onmessage = function (event) {
            var view = new DataView_(event.data);
            pub.protocolVersion = view.getInt32(0, false);
            if (onHandshake)
                onHandshake(pub);
            s.onmessage = onData;
            sendRequests();
        };
        s.onclose = args.onClose;

        // Connection opened
        s.onopen = function () {
            if (onOpen)
                onOpen(pub);
            send(new Uint8Array_([0x60, 0x60, 0xB0, 0x17, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]));
        };

        function send(data) {
            s.send(data);
        }

        function sendRequests() {

            while (requests.length > 0) {
                var request = requests.shift(),
                    data = str(0xB0 + request[1].length, request[0]),
                    chunkSize = 0;
                request[1].forEach(pack1);

                function int32(n) {
                    return str(n >> 24) + str(n >> 16 & 255) + str(n >> 8 & 255) + str(n & 255);
                }

                function packInt(n) {
                    if (n >= -0x10 && n < 0x80)
                        data += str((0x100 + n) % 0x100);
                    else if (n >= -0x8000 && n < 0x8000)
                        data += "\xC9" + str(n >> 8) + str(n & 255);
                    else if (n >= -0x80000000 && n < 0x80000000)
                        data += "\xCA" + int32(n);
                    else
                        packFloat(n);
                }

                function packFloat(n) {
                    var array = new Uint8Array_(8),
                        view = new DataView_(array.buffer);
                    view.setFloat64(0, n, false);
                    data += "\xC1" + str.apply(null, array);
                }

                function packHeader(size, tiny, medium, large) {
                    size = min(size, 0x7FFFFFFF);
                    if (size < 0x10)
                        data += str(tiny + size);
                    else if (size < 0x10000)
                        data += medium + str(size >> 8) + str(size & 255);
                    else
                        data += large + int32(size);
                    return size;
                }

                function packObject(x) {
                    var keys = Object.getOwnPropertyNames(x),
                        size = packHeader(keys.length, 0xA0, "\xD9", "\xDA"),
                        key;
                    for (var i = 0; i < size; i++) {
                        key = keys[i];
                        pack1(key);
                        pack1(x[key]);
                    }
                }

                function pack1(x) {
                    var size, i;

                    // Check for an array first, if so pack as a List
                    if (Array.isArray(x)) {
                        size = packHeader(x.length, 0x90, "\xD5", "\xD6");
                        for (i = 0; i < size; i++)
                            pack1(x[i]);
                    }

                    else {
                        // Determine by type
                        var t = typeof x;

                        //
                        if (t === "boolean")
                            data += x ? "\xC3" : "\xC2";

                        //
                        else if (t === "number")
                            (x % 1 === 0) ? packInt(x) : packFloat(x);

                        //
                        else if (t === "string") {
                            size = packHeader(x.length, 0x80, "\xD1", "\xD2");
                            data += x.substr(0, size);
                        }

                        //
                        else if (t === "object")
                            packObject(x);

                        // Everything else is packed as null
                        else
                            data += "\xC0";
                    }
                }

                handlerMaps.push(request[2]);

                do {
                    data = data.substr(chunkSize);
                    chunkSize = min(data.length, 0x7FFF);
                    send(encode(str(chunkSize >> 8, chunkSize & 0xFF) + data.substr(0, chunkSize)));
                } while(data);

            }
        }

        function onMessage(data)
        {
            var view = new DataView_(data),
                p = 0,
                size = readUint8() - 0xB0;

            function readUint8() {
                return view.getUint8(p++);
            }

            function readUint16() {
                p += 2;
                return view.getUint16(p - 2, false)
            }

            function readUint32() {
                p += 4;
                return view.getUint16(p - 4, false)
            }

            function readInt64() {
                var hi = view.getUint32(p, false).toString(16),
                    lo = view.getUint32(p + 4, false).toString(16);
                p += 8;
                while (hi.length < 8)
                    hi = "0" + hi;
                while (lo.length < 8)
                    lo = "0" + lo;
                return parseInt("0x" + hi + lo, 16);
            }

            function unpackString(size) {
                var s = "", end = p + size;
                while (p < end)
                    s += str(readUint8());
                return s;
            }

            function unpackList(size) {
                var list = [];
                while (size--)
                    list.push(unpack1());
                return list;
            }

            function unpackMap(size) {
                var map = {};
                while (size--) {
                    var key = unpack1();
                    map[key] = unpack1();
                }
                return map;
            }

            function unpackStructure(size)
            {
                var fields = [readUint8()];
                while (size--)
                    fields.push(unpack1());
                return fields;
            }

            function hydrateStructure(size) {
                var struct = unpackStructure(size),
                    type = types[struct[0]];
                return type ? new type(struct.slice(1)) : struct;
            }

            function unpack1() {
                var m = readUint8();
                if (m < 0x80)
                    return m;
                else if (m < 0x90)
                    return unpackString(m - 0x80);
                else if (m < 0xA0)
                    return unpackList(m - 0x90);
                else if (m < 0xB0)
                    return unpackMap(m - 0xA0);
                else if (m < 0xC0)
                    return hydrateStructure(m - 0xB0);
                else if (m < 0xC1)
                    return null;
                else if (m < 0xC2) {
                    p += 8;
                    return view.getFloat64(p - 8, false);
                }
                else if (m < 0xC4)
                    return !!(m & 1);
                else if (m < 0xC8)
                    return undefined;
                else if (m < 0xC9)
                    return view.getInt8(p++);
                else if (m < 0xCA) {
                    p += 2;
                    return view.getInt16(p - 2, false);
                }
                else if (m < 0xCB) {
                    p += 4;
                    return view.getInt32(p - 4, false);
                }
                else if (m < 0xCC)
                    return readInt64();
                else if (m < 0xD0)
                    return undefined;
                else if (m < 0xD1)
                    return unpackString(readUint8());
                else if (m < 0xD2)
                    return unpackString(readUint16());
                else if (m < 0xD3)
                    return unpackString(readUint32());
                else if (m < 0xD4)
                    return undefined;
                else if (m < 0xD5)
                    return unpackList(readUint8());
                else if (m < 0xD6)
                    return unpackList(readUint16());
                else if (m < 0xD7)
                    return unpackList(readUint32());
                else if (m < 0xD8)
                    return undefined;
                else if (m < 0xD9)
                    return unpackMap(readUint8());
                else if (m < 0xDA)
                    return unpackMap(readUint16());
                else if (m < 0xDB)
                    return unpackMap(readUint32());
                else if (m < 0xF0)
                    // Technically, longer structures fit here,
                    // but they're never used
                    return undefined;
                else
                    return m - 0x100;
            }

            var message = unpackStructure(size),
                tag = message[0],
                handlers = (tag === 0x71) ? handlerMaps[0] : handlerMaps.shift(),
                handler = handlers[tag];

            // Automatically send ACK_FAILURE as required
            if (tag === 0x7F)
                requests.push([0x0E, [], {
                    0x7F: function (failure) {
                        s.close(4002, failure.code + ": " + failure.message);
                    }
                }]);

            if (handler)
                handler(message[1]);
        }

        function onData(event)
        {
            var more = 1,
                chunkSize,
                endOfChunk,
                chunkData,
                inData = pvt.inData += str.apply(null, new Uint8Array_(event.data)),
                inChunks = pvt.inChunks;
            while (more) {
                chunkSize = inData.charCodeAt(0) << 8 | inData.charCodeAt(1);
                endOfChunk = 2 + chunkSize;
                if (inData.length >= endOfChunk) {
                    chunkData = inData.slice(2, endOfChunk);
                    if (chunkData) {
                        inChunks.push(chunkData);
                    }
                    else {
                        onMessage(encode(inChunks.join()));
                        inChunks = pvt.inChunks = [];
                    }
                    inData = pvt.inData = pvt.inData.substr(endOfChunk);
                }
                else
                {
                    more = 0;
                }
            }
        }

        /**
         * Run a Cypher query.
         *
         * @param cypher
         * @param parameters
         * @param events
         */
        this.run = function(cypher, parameters, events) {
            events = events || {};
            if (cypher) {
                requests.push([0x10, [cypher, parameters || {}], {
                    0x70: events.onHeader,
                    0x7F: events.onFailure
                }]);
                requests.push([0x3F, [], {
                    0x70: events.onFooter,
                    0x71: events.onRecord,
                    0x7F: events.onFailure
                }]);
            }
            if (pvt.ready)
                sendRequests();
        };

        /**
         * Close the connection.
         */
        this.close = function() { s.close(1000) };

    }

    console.log("js2neo " + js2neo.version)

}(typeof window === "object" ? window : global);
