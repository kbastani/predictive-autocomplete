var holdRequests = false;
var cache = [];
var params = {};
$(function () {

    $(".search-query").width($(".search-query").parents(".form-search").width() - 100);

    $(window).resize(function () {
        $(".search-query").width($(".search-query").parents(".form-search").width() - 100);
    });

    var previousRequest;
    var cancelled = false;
    $("#search_query").typeahead({
        source: function (request, response) {

            if (previousRequest != undefined) {
                previousRequest.abort();
                previousRequest = undefined;
            }

            if (!holdRequests) {
                var acArray = new Array();
                for (var i = 0; i < (cache.length) ; i++) {
                    if (cache[i] != undefined) {
                        if (cache[i].label.match(new RegExp(this.query.replace('(^|\b)\s+?(\b|$)', '+ '), "g"))) {
                            if ($.inArray(cache[i].label.toLowerCase(), acArray) == -1) {
                                acArray.push(cache[i].label);
                            }
                        }
                    }

                }
                if (request.length > 5) {
                    acArray.sort(function (a, b) { return params[b] - params[a]; });
                    response(acArray);
                }

                if (acArray.length >= 100) {
                    return;
                }

                previousRequest = $.ajax({
                    dataType: 'jsonp',
                    jsonpCallback: 'dataCallback',
                    url: "http://arkteraeast.blob.core.windows.net/" + cacheId + "/cache/" + request.toLowerCase(),
                    success: function (data) {
                        for (var i = 0; i < data.length; i++) {
                            if ($.inArray(data[i], cache) == -1) {
                                cache.push(data[i]);
                                params[data[i].label] = data[i].size;
                            }
                        }
                        cache.sort(function (a, b) { return params[b] - params[a]; });
                        data.sort(function (a, b) { return b.size - a.size; });
                        return response($.map(data, function(a) { return a.label; }));
                    },
                    error: function () { return $.map(cache, function (a) { return a.label; }) }
                });

            }
        },
        select: function (event, ui) {
            $("#search_query").val(this.value.toLowerCase());
        },
        matcher: function (item) {
            // Here, the item variable is the item to check for matching.
            // Use this.query to get the current query.
            // return true to signify that the item was matched.
            if (item.match != undefined) {
                return item.match(new RegExp(this.query.replace('(^|\b)\s+?(\b|$)', '+ '), "gi"));
            }
            else {
                return true;
            }
            //return item.match(new RegExp(this.query + ".*", "i"));
        }
    });
});

function dataCallback(data) {
    for (var i = 0; i < data.length; i++) {
        if ($.inArray(data[i], cache) == -1) {
            cache.push(data[i]);
            params[data[i].label] = data[i].size;
        }
    }
    cache.sort(function (a, b) { return params[b] - params[a]; });
}

// HashTable implementation
function HashTable(obj) {
    this.length = 0;
    this.items = {};
    for (var p in obj) {
        if (obj.hasOwnProperty(p)) {
            this.items[p] = { Key: p, Value: obj[p] };
            this.length++;
        }
    }
    this.setItem = function (key, value) {
        var previous = undefined;
        if (this.hasItem(key)) {
            previous = this.items[key];
        }
        else {
            this.length++;
        }
        this.items[key] = { Key: key, Value: value };
        return previous;
    }
    this.getItem = function (key) {
        return this.hasItem(key) ? this.items[key] : undefined;
    }
    this.hasItem = function (key) {
        return this.items.hasOwnProperty(key);
    }
    this.removeItem = function (key) {
        if (this.hasItem(key)) {
            previous = this.items[key];
            this.length--;
            delete this.items[key];
            return previous;
        }
        else {
            return undefined;
        }
    }
    this.keys = function () {
        var keys = [];
        for (var k in this.items) {
            if (this.Value.hasItem(k)) {
                keys.push(k);
            }
        }
        return keys;
    }
    this.values = function () {
        var values = [];
        for (var k in this.items) {
            if (this.Value.hasItem(k)) {
                values.push(this.items[k].Value);
            }
        }
        return values;
    }
    this.each = function (fn) {
        for (var k in this.items) {
            if (this.hasItem(k)) {
                fn(k, this.items[k]);
            }
        }
    }
    this.clear = function () {
        this.items = {}
        this.length = 0;
    }
}



var thisQuery;
var doSearch = function (relatedQuery, append) {
    thisQuery = append === undefined ? relatedQuery : append;
    if (relatedQuery.trim() != "") {
        $("#search_query").attr("disabled", "disabled");
        $("#search_button").attr("disabled", "disabled");
        $.ajax({
            url: "/services/search",
            cache: false,
            data: { sourceQuery: relatedQuery, relatedQuery: thisQuery }
        }).done(function (data) {
            
            $("#search_query").val("");

            $(data).each(function (i, item) {
                var wrapper = $("#qa-content-container");
                var contentString = "";
                var nodeString = "";
                var imgString = "";
                $(item.Content).each(function (j, subitem) { contentString = unescape(contentString) + unescape(subitem) + " " });
                if (item.Nodes.length > 0) {
                    $(item.Nodes).each(function (j, subitem) { nodeString = unescape(nodeString) + "<a class='related-topic-contain btn' href='#'>" + unescape(subitem) + "</a>" });
                }
                if (item.Images.length > 0) {
                    
                    $(item.Images).each(function (j, subitem) { imgString = imgString + "<div class='thumbnail' style='width: " + subitem.width + "; height: " + subitem.height + ";'><a class='swipebox' title='" + subitem.title + "' href='" + subitem.source.replace(/(\/[0-9]{3}px.*$)|(\/thumb)/gi, "") + "' target='_blank'><img src='" + subitem.source.replace("500" + "px", subitem.width) + "'/></a></div>" });
                }

                var contentResultWrapper = "<div class='content-result'>" + unescape(contentString) + "<div class='related-topic-wrapper " + (nodeString === "" ? "" : "well well-small") + "'>" + unescape(nodeString) + "</div><div class='clear'></div>" + "<div" + " class='related-topic-wrapper image-collection js-masonry " + (imgString === "" ? "" : "well well-small") + "'>" + imgString + "</div><div class='clear'></div>" + "</div>";
                
                contentResultWrapper = $(contentResultWrapper);

                $(wrapper).prepend(contentResultWrapper);

                var $container = $('.related-topic-wrapper.image-collection');

                // initialize
                $container.masonry({
                    columnWidth: 20,
                    itemSelector: '.thumbnail'
                });

          

                $($container).find(".thumbnail .swipebox").swipebox({
                    useCSS: true, // false will force the use of jQuery for animations
                    hideBarsDelay: 3000, // 0 to always show caption and action bar
                    videoMaxWidth: 1140
                });



                MathJax.Hub.Update()
                $(contentResultWrapper).find(".topic-link-container").unbind().click(function () {
                    $("#search_query").val($(this).find(".topic-link").val());
                    $("#search_query").focus();
                    doSearch($(this).text(), relatedQuery);
                });

                $(contentResultWrapper).find(".related-topic-contain").unbind().click(function () {
                    $("#search_query").val($(this).text());
                    $("#search_query").focus();
                    doSearch($(this).text(), relatedQuery);
                });
            });

            $("#search_query").removeAttr("disabled");
            $("#search_button").removeAttr("disabled");
            $("#search_query").focus();

            $(".info-box:visible").slideToggle();
        })
    }
}

var escapeRegExp;

(function () {
    // Referring to the table here:
    // https://developer.mozilla.org/en/JavaScript/Reference/Global_Objects/regexp
    // these characters should be escaped
    // \ ^ $ * + ? . ( ) | { } [ ]
    // These characters only have special meaning inside of brackets
    // they do not need to be escaped, but they MAY be escaped
    // without any adverse effects (to the best of my knowledge and casual testing)
    // : ! , = 
    // my test "~!@#$%^&*(){}[]`/=?+\|-_;:'\",<.>".match(/[\#]/g)

    var specials = [
          // order matters for these
            "-"
          , "["
          , "]"
          // order doesn't matter for any of these
          , "/"
          , "{"
          , "}"
          , "("
          , ")"
          , "*"
          , "+"
          , "?"
          , "."
          , "\\"
          , "^"
          , "$"
          , "|"
    ]

        // I choose to escape every character with '\'
        // even though only some strictly require it when inside of []
      , regex = RegExp('[' + specials.join('\\') + ']', 'g')
    ;

    escapeRegExp = function (str) {
        return str.replace(regex, "\\$&");
    };

    // test escapeRegExp("/path/to/res?search=this.that")
}());

var myVar;

$(document).ready(function () {

    $("#search_query").mouseover(zoomDisable).mousedown(zoomEnable);
    function zoomDisable() {
        $('head meta[name=viewport]').remove();
        $('head').prepend('<meta name="viewport" content="user-scalable=0" />');
    }
    function zoomEnable() {
        $('head meta[name=viewport]').remove();
        $('head').prepend('<meta name="viewport" content="user-scalable=1" />');
    }

    $("#search_query").focus();

    $(".topic-link-container").unbind().click(function () {
        $("#search_query").val("What is the definition of " + $(this).find(".topic-link").val() + "?");
        $("#search_query").focus();
    });

    $('#search_query').keypress(function (e) {
        if (e.which == 13) {
            $("#search_button").click(); $('.typeahead.dropdown-menu').hide();
        }

        if (e.which == 40 || e.which == 38) {
            $("#search_query").val($('.typeahead.dropdown-menu .active').attr("data-value"));
        }
    });
    $("#search_button").click(function () {
        var relatedQuery = $("#search_query").val()
        doSearch(relatedQuery);
    });

   

});




function myTimer() {
    holdRequests = false;
    clearTimeout(myVar);
}