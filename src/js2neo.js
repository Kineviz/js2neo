(function () {

    function pack(values) {
        var d = "";

        function pack1(value) {
            var i,  // loop counter
                z;  // size
            if (value === null) {
                d += "\xC0";
            }
            else if (typeof value === "string") {
                z = value.length;
                if (z < 0x10) {
                    d += String.fromCharCode(0x80 + z) + value;
                }
                else if (z < 0x100) {
                    d += "\xD0" + String.fromCharCode(z) + value;
                }
                else if (z < 0x10000) {
                    d += "\xD1" + String.fromCharCode(z >> 8) + String.fromCharCode(z & 255) + value;
                }
                // TODO
            }
            else if (value instanceof Structure) {
                d += String.fromCharCode(0xB0 + value.fields.length, value.tag);
                for (i = 0; i < value.fields.length; i++) {
                    pack1(value.fields[i]);
                }
            }
            else {
                var keys = Object.getOwnPropertyNames(value);
                d += String.fromCharCode(0xA0 + keys.length);
                for (i = 0; i < keys.length; i++) {
                    pack1(keys[i]);
                    pack1(value[keys[i]]);
                }
            }
        }

        for (var i = 0; i < values.length; i++) {
            pack1(values[i]);
        }
        return d;
    }

    function unpack(view) {
        var values = [],
            p = 0;

        function using(size)
        {
            var x = p; p += size; return x;
        }

        function unpackString(size) {
            var s = "", end = p + size;
            for (; p < end; p++)
                s += String.fromCharCode(view.getUint8(p));
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
            var tag = view.getUint8(p++),
                fields = [];
            for (var i = 0; i < size; i++)
                fields.push(unpack1());
            return new Structure(tag, fields);
        }

        function unpack1() {
            var m = view.getUint8(p++);
            if (m < 128)
                return m;
            else if (m < 144)
                return unpackString(m - 128);
            else if (m < 160)
                return unpackList(m - 144);
            else if (m < 176)
                return unpackMap(m - 160);
            else if (m < 192)
                return unpackStructure(m - 176);
            else if (m < 193)
                return null;
            else if (m < 194)
                return view.getFloat64(using(8));
            else if (m < 195)
                return false;
            else if (m < 196)
                return true;
            else if (m < 200)
                return undefined;
            else if (m < 201)
                return view.getInt8(using(1));
            else if (m < 202)
                return view.getInt16(using(2));
            else if (m < 203)
                return view.getInt32(using(4));
            else if (m < 204)
                return [view.getUint32(using(4)), view.getUint32(using(4))];
            else if (m < 208)
                return undefined;
            else if (m < 209)
                return unpackString(view.getUint8(p++));
            else if (m < 210)
                return unpackString(view.getUint16(using(2)));
            else if (m < 211)
                return unpackString(view.getUint32(using(4)));
            else if (m < 212)
                return undefined;
            else if (m < 213)
                return unpackList(view.getUint8(p++));
            else if (m < 214)
                return unpackList(view.getUint16(using(2)));
            else if (m < 215)
                return unpackList(view.getUint32(using(4)));
            else if (m < 216)
                return undefined;
            else if (m < 217)
                return unpackMap(view.getUint8(p++));
            else if (m < 218)
                return unpackMap(view.getUint16(using(2)));
            else if (m < 219)
                return unpackMap(view.getUint32(using(4)));
            else if (m < 240)
                // Technically, longer structures fit here,
                // but they're never used
                return undefined;
            else
                return m - 256;
        }

        while (p < view.byteLength)
            values.push(unpack1());
        return values;
    }

    function Structure(tag, fields)
    {
        this.tag = tag;
        this.fields = fields;
        this.toString = function() {
            return "Structure<" + this.tag + ">(" + JSON.stringify(this.fields) + ")";
        }
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

    function decode(data) {
        var view = new DataView(data);
        var text = "";
        for (var i = 0; i < view.byteLength; i++) {
            text += String.fromCharCode(view.getUint8(i));
        }
        return text;
    }

    function Request(message, handler)
    {
        this.message = message;
        this.handler = handler;
    }

    function Graph(args) {
        var $ = this;
        args = args || {};
        $.socket = new WebSocket("ws://" + (args.host || "localhost") + ":" + (args.port || 7687));
        $.socket.binaryType = "arraybuffer";
        $.rawInputBuffer = "";
        $.chunkInputBuffer = [];
        $.requests = [new Request(
            new Structure(0x01, ["js2neo/1.0.0-alpha.0", {
                scheme: "basic",
                principal: args.user || "neo4j",
                credentials: args.password || "password"
            }]),
            {
                0x70: console.log,
                0x71: console.log,
                0x7E: console.log,
                0x7F: console.log
            }
        )];
        $.handlers = [];
        $.ready = false;

        function send()
        {
            while($.requests.length > 0)
            {
                var request = $.requests.shift();
                $.handlers.push(request.handler);
                console.log("C: " + request.message);
                var data = pack([request.message]);
                while(data.length > 32767)
                {
                    $.socket.send(new Uint8Array([0x7F, 0xFF]));
                    $.socket.send(encode(data.substr(0, 32767)));
                    data = data.substr(32767);
                }
                $.socket.send(new Uint8Array([data.length >> 8, data.length & 0xFF]));
                $.socket.send(encode(data));
                $.socket.send(new Uint8Array([0x00, 0x00]));
            }
        }

        function onHandshake(event)
        {
            $.socket.removeEventListener('message', onHandshake);
            var v = new DataView(event.data);
            $.protocolVersion = v.getInt32(0, false);
            console.log("Using protocol version " + $.protocolVersion);
            $.socket.addEventListener('message', onChunkedData);
            send();
            $.ready = true;
        }

        function onMessage(data)
        {
            var message = unpack(new DataView(data))[0],
                handler = (message.tag === 0x71) ? $.handlers[0] : $.handlers.shift();
            if (handler) handler[message.tag](message.fields[0]);
        }

        function onChunk(data)
        {
            if (data === "")
            {
                onMessage(encode($.chunkInputBuffer.join()));
                $.chunkInputBuffer = [];
            }
            else {
                $.chunkInputBuffer.push(data);
            }
        }

        function onChunkedData(event)
        {
            $.rawInputBuffer += decode(event.data);
            var more = true;
            while (more) {
                var chunkSize = 0x100 * $.rawInputBuffer.charCodeAt(0) + $.rawInputBuffer.charCodeAt(1);
                var end = 2 + chunkSize;
                if ($.rawInputBuffer.length >= end) {
                    onChunk($.rawInputBuffer.slice(2, end));
                    $.rawInputBuffer = $.rawInputBuffer.substr(end);
                }
                else
                {
                    more = false;
                }
            }
        }

        $.socket.addEventListener('message', onHandshake);

        // Connection opened
        $.socket.addEventListener('open', function (event) {
            // Handshake
            var MAGIC = new Uint8Array([0x60, 0x60, 0xB0, 0x17]);
            var VERSIONS = new DataView(new ArrayBuffer(16));
            VERSIONS.setInt32(0, 2, false);
            VERSIONS.setInt32(4, 1, false);
            $.socket.send(MAGIC);
            $.socket.send(VERSIONS);
        });

        $.socket.addEventListener('error', function (event) {
            console.log(event);
        });

        $.socket.addEventListener('close', function (event) {
            console.log(event);
        });
        $.protocolVersion = null;
        $.run = function(workload) {
            var cypher = workload.cypher;
            if (cypher.length > 0) {
                var run = new Request(
                    new Structure(0x10, [cypher, workload.parameters || {}]),
                    {
                        0x70: console.log,
                        0x71: console.log,
                        0x7E: console.log,
                        0x7F: console.log
                    }
                );
                var pull = new Request(
                    new Structure(0x3F, []),
                    {
                        0x70: console.log,
                        0x71: console.log,
                        0x7E: console.log,
                        0x7F: console.log
                    }
                );
                $.requests.push(run);
                $.requests.push(pull);
            }
            if ($.ready) send();
        }
;
    }

    window.js2neo = {

        graph: function(args) { return new Graph(args); },

        Z: Z

    };

})();