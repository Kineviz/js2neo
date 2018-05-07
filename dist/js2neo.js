/*
 * Copyright 2011-2018, Nigel Small
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


(function(define, require) {

    function loadD3() {
        if (window.d3) return true;
        require.config({
            paths: {
                d3: '//d3js.org/d3.v3.min'
            }
        });
        return true;
    }

    function loadCSS(url) {
        var link = document.createElement("link");
        link.type = "text/css";
        link.rel = "stylesheet";
        link.href = url;
        document.getElementsByTagName("head")[0].appendChild(link);
    }

    function draw(container, data) {
        if (!loadD3()) return;
        require(['d3'], function (d3) {
            {

                var width = 960,
                    height = 500;

                var svg = d3.select(container)
                    .attr("width", width)
                    .attr("height", height);

                var force = d3.layout.force()
                    .gravity(.05)
                    .distance(100)
                    .charge(-100)
                    .size([width, height])
                    .nodes(data.nodes)
                    .links(data.links)
                    .start();

                var link = svg.selectAll(".link")
                    .data(data.links)
                    .enter().append("g")
                    .attr("class", "link");

                var line = link.append("line");

                var node = svg.selectAll(".node")
                    .data(data.nodes)
                    .enter().append("g")
                    .attr("class", "node")
                    .call(force.drag);

                node.append("circle");

                var g = node.append("g")
                    .attr("class", "text");

                g.append("text")
                    // .attr("dx", 0)
                    // .attr("dy", "24px")
                    .text(function (d) { return d.name || d.title });

                force.on("tick", function () {
                    line.attr("x1", function (d) { return d.source.x; })
                        .attr("y1", function (d) { return d.source.y; })
                        .attr("x2", function (d) { return d.target.x; })
                        .attr("y2", function (d) { return d.target.y; });
                    node.attr("transform", function (d) { return "translate(" + d.x + "," + d.y + ")"; });
                });

            }
        });

    }

    define({

        /**
         * Load a CSS
         */
        loadCSS: loadCSS,

        /**
         * Draw a graph
         */
        draw: draw,

        version: 1.0

    });

})(define, requirejs);
