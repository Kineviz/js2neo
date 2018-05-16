(function () {
    var B = Uint8Array;

    function pack(values) {
        var b = String.fromCharCode,
            d = "";

        function pack1(value) {
            var i,  // loop counter
                z;  // size
            if (value === null) {
                d += "\xC0";
            }
            else if (typeof value === "string") {
                z = value.length;
                if (z < 0x10) {
                    d += b(0x80 + z) + value;
                }
                else if (z < 0x100) {
                    d += "\xD0" + b(z) + value;
                }
                else if (z < 0x10000) {
                    d += "\xD1" + b(z >> 8) + b(z & 255) + value;
                }
                // TODO
            }
            else if (value instanceof Structure) {
                d += b(0xB0 + value.fields.length, value.tag);
                for (i = 0; i < value.fields.length; i++) {
                    pack1(value.fields[i]);
                }
            }
            else {
                var keys = Object.getOwnPropertyNames(value);
                d += b(0xA0 + keys.length);
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

    function unpack(data) {
        var values = [];
        var p = 0;

        function unpackString(size)
        {
            var value = data.substr(p, size);
            p += size;
            return value;
        }

        function unpackList(size)
        {
            var list = [];
            for (var i = 0; i < size; i++) {
                list.push(unpack1());
            }
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
            var tag = data.charCodeAt(p++),
                fields = [];
            for (var i = 0; i < size; i++) {
                fields.push(unpack1());
            }
            return new Structure(tag, fields);
        }

        function unpack1() {
            var marker = data.charCodeAt(p++);
            if (marker === 0xC0) {
                return null;
            }
            else if (marker === 0xC2) {
                return false;
            }
            else if (marker === 0xC3) {
                return true;
            }
            else if (marker >= 0x00 && marker <= 0x7F) {
                return marker;
            }
            else if (marker >= 0x80 && marker <= 0x8F) {
                return unpackString(marker - 0x80);
            }
            else if (marker >= 0x90 && marker <= 0x9F) {
                return unpackList(marker - 0x90);
            }
            else if (marker >= 0xA0 && marker <= 0xAF) {
                return unpackMap(marker - 0xA0);
            }
            else if (marker >= 0xB0 && marker <= 0xBF) {
                return unpackStructure(marker - 0xB0);
            }
            else if (marker === 0xD0)
            {
                return unpackString(data.charCodeAt(p++));
            }
            else if (marker >= 0xF0 && marker <= 0xFF) {
                return marker - 0x100;
            }
            else {
                console.log(marker);
                return undefined;
            }
        }

        while (p < data.length) {
            values.push(unpack1());
        }
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
        var data = new B(text.length);
        for (var i = 0; i < text.length; i++)
        {
            data[i] = text.charCodeAt(i);
        }
        return data;
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
        $.requests = [];
        $.handlers = [];
        $.ready = false;

        var init1 = new Request(
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
        );
        $.requests.push(init1);

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
                    $.socket.send(new B([0x7F, 0xFF]));
                    $.socket.send(encode(data.substr(0, 32767)));
                    data = data.substr(32767);
                }
                $.socket.send(new B([data.length >> 8, data.length & 0xFF]));
                $.socket.send(encode(data));
                $.socket.send(new B([0x00, 0x00]));
            }
        }

        function onHandshake(event)
        {
            $.socket.removeEventListener('message', onHandshake);
            var v = new DataView(event.data);
            $.protocolVersion = v.getInt32(0, false);
            console.log("Using protocol version " + $.protocolVersion);
            init();
        }

        function onMessage(data)
        {
            var message = unpack(data)[0];
            var handler;
            if (message.tag === 0x71) {
                handler = $.handlers[0];
            }
            else
            {
                handler = $.handlers.shift();
            }
            handler[message.tag](message.fields[0]);
        }

        function onChunk(data)
        {
            if (data === "")
            {
                onMessage($.chunkInputBuffer.join());
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

        function init() {
            $.socket.addEventListener('message', onChunkedData);
            send();
            $.ready = true;
        }

        $.socket.addEventListener('message', onHandshake);

        // Connection opened
        $.socket.addEventListener('open', function (event) {
            // Handshake
            var MAGIC = new B([0x60, 0x60, 0xB0, 0x17]);
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

        graph: function(args) { return new Graph(args); }

    };

})();