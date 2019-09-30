var map = L.map('map').setView([39.916667, 116.383333], 12);
var colors = {
    0: '#800026',
    1: '#BD0026',
    2: '#E31A1C',
    3: '#FC4E2A',
    4: '#FD8D3C',
    5: '#FEB24C',
    6: '#FED976',
    7: '#FFEDA0'
}

var tileLayer = L.tileLayer('https://stamen-tiles-{s}.a.ssl.fastly.net/toner/{z}/{x}/{y}{r}.{ext}', {
    attribution: 'Map tiles by <a href="http://stamen.com">Stamen Design</a>, <a href="http://creativecommons.org/licenses/by/3.0">CC BY 3.0</a> &mdash; Map data &copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
    subdomains: 'abcd',
    minZoom: 0,
    maxZoom: 20,
    ext: 'png'
}).addTo(map);

var taxis = {}

var socket = io('/tdrive');
socket.on('connect', function() {
    socket.emit('my event', {data: 'Connected!'});
});
socket.on('add', function(data) {
    var obj = JSON.parse(data);

    var taxiId = obj['id'] + '/' + obj['hexAddr']
    console.log()

    var polygon = taxis[taxiId];
    var color = colors[parseInt(obj['id']) % 8]
    if (polygon === undefined) {
        console.log('taxiId = ' + taxiId + ', add to map');
        // add to map
        var hexBoundary = h3.h3ToGeoBoundary(obj['hexAddr']);
        var polygon = L.polygon(hexBoundary, {color: color, opacity: 0.5, weight: 2}).bindPopup("Taxi " + obj['id'] + "</br>Inserted At " + obj['startTs']);
        var poly = polygon.addTo(map);

        taxis[taxiId] = {
            'hexAddr': obj['hexAddr'],
            'polygon': polygon
        };
    } else if (polygon['hexAddr'] != obj['hexAddr']) {
        // NOTE: this case shouldn't really happen...
        console.log('taxiId = ' + taxiId + ', different location, previous = ' + polygon['hexAddr']);
        map.removeLayer(polygon['polygon']);

        var hexBoundary = h3.h3ToGeoBoundary(obj['hexAddr']);
        var polygon = L.polygon(hexBoundary, {color: color, opacity: 0.5, weight: 2}).bindPopup("Taxi " + obj['id']);
        var poly = polygon.addTo(map);

        // overwrite key in cache
        taxis[taxiId] = {
            'hexAddr': obj['hexAddr'],
            'polygon': polygon
        };
    } else {
        // if we get here, the key in the cache is the same of the current object and we do nothing
        console.log('taxiId = ' + taxiId + ', same location');
    }
});
socket.on('rem', function(data) {
    var obj = JSON.parse(data);

    var taxiId = obj['id'] + '/' + obj['hexAddr']

    var polygon = taxis[taxiId];
    if (polygon === undefined) {
        return;
    }

    console.log('taxiId = ' + taxiId + ', remove');
    map.removeLayer(polygon['polygon']);

    delete taxis[taxiId];
});