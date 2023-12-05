//Visualization script

let yourDataArray = [];
let map;

function initMap() {
    map = new google.maps.Map(document.getElementById('map'), {
        center: { lat: 40, lng: -100 },
        zoom: 4
    });

    // Fetch and process data
    fetch('/output.json')
        .then(response => response.json())
        .then(data => {
            yourDataArray = data; // Store data in global array
            const averages = calculateAverages(data); // Process initial data

            const deckOverlay = new deck.GoogleMapsOverlay({
                layers: [
                    new deck.ScatterplotLayer({
                        id: 'scatterplot-layer',
                        data: averages,
                        getPosition: d => [d.longitude, d.latitude],
                        getRadius: d => Math.max(d.averageTemp, 1) * 1000, 
                        getFillColor: tempToColor, // Convert temperature to color
                        pickable: true
                    })
                ]
            });

            deckOverlay.setMap(map); 
        })
        .catch(error => console.error('Error loading data:', error));
}

function calculateAverages(data) {
    const tempByLocation = {};

    data.forEach(entry => {
        const key = `${entry.LATITUDE},${entry.LONGITUDE}`;
        if (!tempByLocation[key]) {
            tempByLocation[key] = { sum: 0, count: 0, latitude: parseFloat(entry.LATITUDE), longitude: parseFloat(entry.LONGITUDE) };
        }
        tempByLocation[key].sum += parseFloat(entry.T_DAILY_AVG);
        tempByLocation[key].count++;
    });

    // Return temp averages as array for visualization
    return Object.values(tempByLocation).map(loc => ({
        latitude: loc.latitude,
        longitude: loc.longitude,
        averageTemp: loc.sum / loc.count
    }));
}

function tempToColor(d) {
    const temp = d.averageTemp;
    if (temp < 0) return [0, 0, 255]; // Cold temperatues blue
    if (temp >= 0 && temp < 15) return [0, 255, 0]; // Average temperatures green
    return [255, 0, 0]; // Warm temperatures red
}

function sliderValueToDate(value) {
    const baseDate = new Date(2022, 0, 1); 
    baseDate.setDate(baseDate.getDate() + parseInt(value) - 1);
    return baseDate.toISOString().split('T')[0].replace(/-/g, '');
}


function updateMapData(sliderValue) {
    const selectedDate = sliderValueToDate(sliderValue);
    document.getElementById('sliderValue').innerText = `Date: ${selectedDate}`;

    const filteredData = yourDataArray.filter(entry => entry.LST_DATE === selectedDate);
    const averages = calculateAverages(filteredData);

    const deckOverlay = new deck.GoogleMapsOverlay({
        layers: [
            new deck.ScatterplotLayer({
                id: 'scatterplot-layer',
                data: averages,
                getPosition: d => [d.longitude, d.latitude],
                getRadius: d => Math.max(d.averageTemp, 1) * 1000,
                getFillColor: tempToColor, // Convert to color
                pickable: true
            })
        ]
    });

    deckOverlay.setMap(map);
}


window.onload = initMap;
