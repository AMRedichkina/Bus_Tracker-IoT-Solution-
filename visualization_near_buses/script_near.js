define([
  "esri/map",
  "esri/layers/OpenStreetMapLayer",
  "esri/geometry/Extent",
  "esri/geometry/Point",
  "esri/symbols/PictureMarkerSymbol",
  "esri/graphic",
  "dojo/dom",
  "dojo/on",
  "esri/dijit/PopupTemplate",
  "dojo/domReady!"
], function (Map, OpenStreetMapLayer, Extent, Point, PictureMarkerSymbol, Graphic, dom, on, PopupTemplate) {
  
    var map, openStreetMapLayer;

    var finlandExtent = new Extent({
      xmin: 20.5,
      ymin: 59.5,
      xmax: 31.5,
      ymax: 70.0,
      spatialReference: {
        wkid: 4326
      }
    });
  
    map = new Map("esri-map-container", {
      basemap: "gray",
      extent: finlandExtent,
      center: [24.56, 60.10],
      zoom: 12
    });
    openStreetMapLayer = new OpenStreetMapLayer();
    map.addLayer(openStreetMapLayer);
  
    async function fetchBusLocations(lat, lon) {
      console.log('start request');
      const response = await fetch(`http://localhost:80/buses/near?lat=${lat}&lon=${lon}`);
      const busLocations = await response.json();
      console.log(busLocations);
      return busLocations;
    }
  
    function displayBuses(map, busLocations) {
      const busIconUrl = "./bus.png";
      const markerSymbol = new PictureMarkerSymbol(busIconUrl, 32, 32);
    
      map.graphics.clear();
    
      for (const busId in busLocations) {
        const busLocation = busLocations[busId];
        const point = new Point(busLocation.lon, busLocation.lat);
        const attributes = {
          'id': busId,
          'lon': busLocation.lon,
          'lat': busLocation.lat,
          'next_stop': busLocation.next_stop,
        };
        const popupTemplate = new PopupTemplate({
          title: 'Bus {id}, next stop {next_stop}',
        });
        const graphic = new Graphic(point, markerSymbol, attributes);
        graphic.setInfoTemplate(popupTemplate);
        map.graphics.add(graphic);
      }
    }
    
  
    on(dom.byId("submit-coordinates"), "click", async function () {
      const lat = parseFloat(dom.byId("latitude").value);
      const lon = parseFloat(dom.byId("longitude").value);
    
      if (!isNaN(lat) && !isNaN(lon)) {
        const busLocations = await fetchBusLocations(lat, lon);
        displayBuses(map, busLocations);
        
        map.centerAt(new Point(lon, lat));
    
      } else {
        alert("Please, try it again. The coordinates may have been entered incorrectly.");
      }
    });
  });
 