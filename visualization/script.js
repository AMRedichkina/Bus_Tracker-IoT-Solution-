define([
   "esri/map",
   "esri/layers/OpenStreetMapLayer",
   "esri/geometry/Point",
   "esri/symbols/SimpleMarkerSymbol",
   "esri/Color",
   "esri/graphic",
   "dojo/domReady!"
 ], function (Map, OpenStreetMapLayer, Point, SimpleMarkerSymbol, Color, Graphic) {
 
   var map, openStreetMapLayer;
 
   map = new Map("esri-map-container", {
     center: [24.56, 60.10],
     zoom: 12
   });
   openStreetMapLayer = new OpenStreetMapLayer();
   map.addLayer(openStreetMapLayer);
 
   async function fetchBusLocations() {
     console.log('start request')
     const response = await fetch("http://localhost:80/buses/location");
     const busLocations = await response.json();
     console.log(busLocations)
     return busLocations;
   }
 
   function displayBuses(map, busLocations) {
     const markerSymbol = new SimpleMarkerSymbol().setColor(new Color([255, 0, 0]));
 
     for (const busId in busLocations) {
       const busLocation = busLocations[busId];
       const point = new Point(busLocation.lon, busLocation.lat);
       const graphic = new Graphic(point, markerSymbol);
       map.graphics.add(graphic);
     }
   }
 
   async function initialize() {
     console.log('start')
     const busLocations = await fetchBusLocations();
     console.log('after request')
     displayBuses(map, busLocations);
     console.log('response')
   }
 
   initialize();
 
 });
