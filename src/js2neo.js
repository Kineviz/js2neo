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
        DV = DataView,
        U8 = Uint8Array;

    global.js2neo = {

        bolt: function(args) { return new Bolt(args); },

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

        version: "1.0.0-beta.1"

    };

    function unwind(struct) {
        for (var i = 0; i < struct.length; i++)
            this[arguments[i + 1]] = struct[i];
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

    function NOOP() {}

    function pack(tag, fields) {
        var d = str(0xB0 + fields.length, tag);

        function int32(n) {
            return str(n >> 24) + str(n >> 16 & 255) + str(n >> 8 & 255) + str(n & 255);
        }

        function packInt(n) {
            // TODO: inRange function
            if (n >= 0 && n < 0x80)
                d += str(n);
            else if (n >= -16 && n < 0)
                d += str(256 + n);
            else if (n >= -0x8000 && n < 0x8000)
                d += "\xC9" + str(n >> 8) + str(n & 255);
            else if (n >= -0x80000000 && n < 0x80000000)
                d += "\xCA" + int32(n);
            else
                packFloat(n);
        }

        function packFloat(n) {
            var array = new U8(8),
                view = new DV(array.buffer);
            view.setFloat64(0, n, false);
            d += "\xC1" + str.apply(null, array);
        }

        function packHeader(size, tiny, small, medium, large)
        {
            size = Math.min(size, 0x7FFFFFFF);
            if (size < 0x10)
                d += str(tiny + size);
            else if (size < 0x100)
                d += small + str(size);
            else if (size < 0x10000)
                d += medium + str(size >> 8) + str(size & 255);
            else
                d += large + int32(size);
            return size;
        }

        function packString(x) {
            var size = packHeader(x.length, 0x80, "\xD0", "\xD1", "\xD2");
            d += x.substr(0, size);
        }

        function packArray(a) {
            var size = packHeader(a.length, 0x90, "\xD4", "\xD5", "\xD6");
            for (var i = 0; i < size; i++)
                pack1(a[i]);
        }

        function packObject(x) {
            var keys = Object.getOwnPropertyNames(x),
                size = packHeader(keys.length, 0xA0, "\xD8", "\xD9", "\xDA"),
                key;
            for (var i = 0; i < size; i++) {
                key = keys[i];
                pack1(key);
                pack1(x[key]);
            }
        }

        function pack1(x) {
            if (Array.isArray(x)) packArray(x);
            else {
                var t = typeof x;
                if (t === "boolean") d += x ? "\xC3" : "\xC2";
                else if (t === "number") (x % 1 === 0) ? packInt(x) : packFloat(x);
                else if (t === "string") packString(x);
                else if (t === "object") packObject(x);
                else d += "\xC0";
            }
        }

        fields.forEach(pack1);

        return d;
    }

    /**
     * Unpack the first value from a DataView (there should be only one -- #Highlander).
     *
     * @param view
     * @returns {Array}
     */
    function unpack(view) {
        var p = 0,
            size = view.getUint8(p++) - 0xB0;

        function using(size)
        {
            var x = p; p += size; return x;
        }

        function getInt64(view, offset) {
            var hi = view.getUint32(offset, false).toString(16),
                lo = view.getUint32(offset + 4, false).toString(16);
            while (hi.length < 8)
                hi = "0" + hi;
            while (lo.length < 8)
                lo = "0" + lo;
            return parseInt("0x" + hi + lo, 16);
        }

        function unpackString(size) {
            var s = "", end = p + size;
            for (; p < end; p++)
                s += str(view.getUint8(p));
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
            var fields = [view.getUint8(p++)];
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
            var m = view.getUint8(p++);
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
            else if (m < 0xC2)
                return view.getFloat64(using(8), false);
            else if (m < 0xC4)
                return !!(m & 1);
            else if (m < 0xC8)
                return undefined;
            else if (m < 0xC9)
                return view.getInt8(using(1));
            else if (m < 0xCA)
                return view.getInt16(using(2), false);
            else if (m < 0xCB)
                return view.getInt32(using(4), false);
            else if (m < 0xCC)
                return getInt64(view, using(8), false);
            else if (m < 0xD0)
                return undefined;
            else if (m < 0xD1)
                return unpackString(view.getUint8(p++));
            else if (m < 0xD2)
                return unpackString(view.getUint16(using(2)));
            else if (m < 0xD3)
                return unpackString(view.getUint32(using(4)));
            else if (m < 0xD4)
                return undefined;
            else if (m < 0xD5)
                return unpackList(view.getUint8(p++));
            else if (m < 0xD6)
                return unpackList(view.getUint16(using(2)));
            else if (m < 0xD7)
                return unpackList(view.getUint32(using(4)));
            else if (m < 0xD8)
                return undefined;
            else if (m < 0xD9)
                return unpackMap(view.getUint8(p++));
            else if (m < 0xDA)
                return unpackMap(view.getUint16(using(2)));
            else if (m < 0xDB)
                return unpackMap(view.getUint32(using(4)));
            else if (m < 0xF0)
                // Technically, longer structures fit here,
                // but they're never used
                return undefined;
            else
                return m - 0x100;
        }

        return unpackStructure(size);
    }

    function encode(text)
    {
        var data = new U8(text.length);
        for (var i = 0; i < text.length; i++)
            data[i] = text.charCodeAt(i);
        return data.buffer;
    }

    function Bolt(args) {
        args = args || {};
        var pub = {
                secure: args.secure || document.location.protocol === "https:",
                host: args.host || "localhost",
                port: args.port || 7687,
                user: args.user || "neo4j",
                userAgent: args.userAgent || "js2neo/" + js2neo.version
            },
            auth = {
                scheme: "basic",
                principal: pub.user,
                credentials: args.password
            },
            chunkHeader = new U8(2),
            pvt = {},
            requests = [],
            handlers = [];

        function open() {
            pvt.inData = "";
            pvt.inChunks = [];
            pvt.ready = false;

            var s = pvt.socket = new WebSocket((pub.secure ? "wss" : "ws") + "://" + pub.host + ":" + pub.port);
            s.binaryType = "arraybuffer";
            s.onmessage = onHandshake;
            s.onerror = args.onError || NOOP;
            s.onclose = args.onClose || NOOP;

            // Connection opened
            s.onopen = function () {
                (args.onOpen || NOOP)(pub);
                send(new U8([0x60, 0x60, 0xB0, 0x17, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]));
            };

            requests.push([0x01, [pub.userAgent, auth], {
                0x70: function () {
                    (args.onInit || NOOP)(pub);
                    pvt.ready = true;
                },
                0x7F: function (failure) {
                    (args.onInitFailure || NOOP)(failure);
                }
            }]);
        }

        function send(data) {
            pvt.socket.send(data);
        }

        function sendRequests() {

            function sendChunkHeader(hi, lo) {
                chunkHeader[0] = hi;
                chunkHeader[1] = lo;
                send(chunkHeader);
            }

            while (requests.length > 0) {
                var request = requests.shift(),
                    data = pack(request[0], request[1]);
                handlers.push(request[2]);
                while (data.length > 0x7FFF) {
                    sendChunkHeader(0x7F, 0xFF);
                    send(encode(data.substr(0, 0x7FFF)));
                    data = data.substr(0x7FFF);
                }
                sendChunkHeader(data.length >> 8, data.length & 0xFF);
                send(encode(data));
                sendChunkHeader(0x00, 0x00);
            }
        }

        function onHandshake(event)
        {
            var view = new DV(event.data);
            pub.protocolVersion = view.getInt32(0, false);
            (args.onHandshake || NOOP)(pub);
            pvt.socket.onmessage = onData;
            sendRequests();
        }

        function onMessage(data)
        {
            var message = unpack(new DV(data)),
                handler = (message[0] === 0x71) ? handlers[0] : handlers.shift();

            // Automatically send ACK_FAILURE as required
            if (message[0] === 0x7F)
                requests.push([0x0E, [], {
                    0x7F: function (failure) {
                        pvt.socket.close(4002, failure.code + ": " + failure.message);
                    }
                }]);

            if (handler)
                (handler[message[0]] || NOOP)(message[1]);
        }

        function onData(event)
        {
            var more = 1,
                chunkSize,
                endOfChunk,
                chunkData,
                inData = pvt.inData += str.apply(null, new U8(event.data)),
                inChunks = pvt.inChunks;
            while (more) {
                chunkSize = inData.charCodeAt(0) << 8 | inData.charCodeAt(1);
                endOfChunk = 2 + chunkSize;
                if (inData.length >= endOfChunk) {
                    chunkData = inData.slice(2, endOfChunk);
                    if (chunkData === "") {
                        onMessage(encode(inChunks.join()));
                        inChunks = pvt.inChunks = [];
                    }
                    else {
                        inChunks.push(chunkData);
                    }
                    inData = pvt.inData = pvt.inData.substr(endOfChunk);
                }
                else
                {
                    more = 0;
                }
            }
        }

        this.run = function(cypher, args) {

            if (cypher.length > 0) {
                requests.push([0x10, [cypher, args.params || {}], {
                    0x70: args.onHeader || NOOP,
                    0x7F: args.onFailure || NOOP
                }]);
                requests.push([0x3F, [], {
                    0x70: args.onFooter || NOOP,
                    0x71: args.onRecord || NOOP,
                    0x7F: args.onFailure || NOOP
                }]);
            }
            if (pvt.ready) sendRequests();
        };

        this.close = function() {
            pvt.socket.close();
        };

        open();

    }

    console.log("js2neo v" + global.js2neo.version)

}(typeof window !== "undefined" ? window : global);
