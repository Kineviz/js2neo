(function () {

    var DAYS = "days",
        ID = "id",
        NANOSECONDS ="nanoseconds",
        PROPERTIES = "properties",
        SECONDS = "seconds",
        TZ = "tz",

        str = String.fromCharCode;

    DataView.prototype.getInt64 = function(offset, littleEndian) {
        var hi = this.getUint32(offset, littleEndian),
            lo = this.getUint32(offset + 4, littleEndian);
        hi = hi.toString(16);
        while (hi.length < 8)
            hi = "0" + hi;
        lo = lo.toString(16);
        while (lo.length < 8)
            lo = "0" + lo;
        return parseInt("0x" + hi + lo, 16);
    };

    window.js2neo = {

        bolt: function(args) { return new Bolt(args); },

        Node: function(struct) {
            unwind.call(this, struct, ID, "labels", PROPERTIES);
        },

        Relationship: function(struct) {
            unwind.call(this, struct, ID, "start", "end", "type", PROPERTIES);
        },

        Path: function(struct) {
            var nodes = struct[0],
                relationships = struct[1],
                sequence = struct[2],
                lastNode = nodes[0],
                nextNode,
                entities = [lastNode],
                i,
                relationshipIndex,
                r;
            for(i = 0; i < sequence.length; i += 2)
            {
                relationshipIndex = sequence[i];
                nextNode = nodes[sequence[2 * i + 1]];
                if (relationshipIndex > 0) {
                    r = relationships[relationshipIndex - 1];
                    entities.push(new js2neo.Relationship([r.id, lastNode.id, nextNode.id, r.type, r.properties]));
                } else {
                    r = relationships[relationships.length + relationshipIndex];
                    entities.push(new js2neo.Relationship([r.id, nextNode.id, lastNode.id, r.type, r.properties]));
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

        version: "1.0.0-alpha.0"

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

        function pack1(x) {
            var i,  // loop counter
                z;  // size
            if (x === null) {
                d += "\xC0";
            }
            else {
                if (typeof x === "string") {
                    z = x.length;
                    if (z < 0x10) {
                        d += str(0x80 + z) + x;
                    }
                    else if (z < 0x100) {
                        d += "\xD0" + str(z) + x;
                    }
                    else if (z < 0x10000) {
                        d += "\xD1" + str(z >> 8) + str(z & 255) + x;
                    }
                    // TODO
                }
                else {
                    var keys = Object.getOwnPropertyNames(x);
                    d += str(0xA0 + keys.length);
                    for (i = 0; i < keys.length; i++) {
                        pack1(keys[i]);
                        pack1(x[keys[i]]);
                    }
                }
            }
        }

        for (var i = 0; i < fields.length; i++)
            pack1(fields[i]);

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

        function unpackString(size) {
            var s = "", end = p + size;
            for (; p < end; p++)
                s += str(view.getUint8(p));
            return s;
        }

        function unpackList(size)
        {
            var list = [];
            for (var i = 0; i < size; i++)
                list.push(unpack1());
            return list;
        }

        function unpackMap(size)
        {
            var map = {};
            for (var i = 0; i < size; i++) {
                var key = unpack1();
                map[key] = unpack1();
            }
            return map;
        }

        function unpackStructure(size)
        {
            var fields = [view.getUint8(p++)];
            for (var i = 0; i < size; i++)
                fields.push(unpack1());
            return fields;
        }

        function hydrateStructure(size) {
            var struct = unpackStructure(size),
                tag = struct[0],
                type = types[tag];
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
                return view.getInt64(using(8), false);
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
        var data = new Uint8Array(text.length);
        for (var i = 0; i < text.length; i++)
        {
            data[i] = text.charCodeAt(i);
        }
        return data.buffer;
    }

    function Bolt(args) {
        args = args || {};
        var pub = {
                secure: document.location.protocol === "https:",
                host: args.host || "localhost",
                port: args.port || 7687,
                user: args.user || "neo4j",
                userAgent: args.userAgent || "js2neo/" + js2neo.version
            },
            auth = {
                scheme: "basic",
                principal: pub.user,
                credentials: args.password || "password"
            },
            chunkHeader = new Uint8Array(2),
            pvt = {},
            requests = [],
            handlers = [],
            onConnect = args.onConnect || NOOP;

        function open() {
            pvt.inData = "";
            pvt.inChunks = [];
            pvt.ready = false;

            pvt.socket = new WebSocket((pub.secure ? "wss" : "ws") + "://" + pub.host + ":" + pub.port);
            pvt.socket.binaryType = "arraybuffer";
            pvt.socket.onmessage = onHandshake;
            pvt.socket.onerror = args.onError || NOOP;
            pvt.socket.onclose = args.onClose || NOOP;

            // Connection opened
            pvt.socket.onopen = function () {
                // Handshake
                send(new Uint8Array([0x60, 0x60, 0xB0, 0x17, 0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0]));
            };

            requests.push([0x01, [pub.userAgent, auth], {
                0x70: function () { pvt.ready = true; },
                0x7F: reopen
            }]);
        }

        function send(data) {
            pvt.socket.send(data);
        }

        function reopen(failure)
        {
            pvt.socket.close(1002, failure.code + ": " + failure.message);
            open();
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
            var view = new DataView(event.data);
            pub.protocolVersion = view.getInt32(0, false);
            onConnect(pub);
            pvt.socket.onmessage = onData;
            sendRequests();
        }

        function onMessage(data)
        {
            var message = unpack(new DataView(data)),
                handler = (message[0] === 0x71) ? handlers[0] : handlers.shift();

            // Automatically send ACK_FAILURE as required
            if (message[0] === 0x7F)
                requests.push([0x0E, [], {0x7F: reopen}]);

            if (handler)
                (handler[message[0]] || NOOP)(message[1]);
        }

        function onData(event)
        {
            var more = true,
                chunkSize,
                endOfChunk,
                chunkData,
                inData = pvt.inData += str.apply(null, new Uint8Array(event.data)),
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
                    more = false;
                }
            }
        }

        this.run = function(workload) {

            var cypher = workload.cypher;
            if (cypher.length > 0) {
                requests.push([0x10, [cypher, workload.parameters || {}], {
                    0x70: workload.onHeader || NOOP,
                    0x7F: workload.onFailure || NOOP
                }]);
                requests.push([0x3F, [], {
                    0x70: workload.onFooter || NOOP,
                    0x71: workload.onRecord || NOOP,
                    0x7F: workload.onFailure || NOOP
                }]);
            }
            if (pvt.ready) sendRequests();
        };

        open();

    }

})();