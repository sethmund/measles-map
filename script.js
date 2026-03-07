const width = window.innerWidth;
const height = window.innerHeight;

const projection = d3.geoConicConic()
    .parallels([15, 60])
    .rotate([100, 0])
    .center([0, 38])
    .scale(width * 0.5)
    .translate([width / 2, height / 2]);

const path = d3.geoPath().projection(projection);
const svg = d3.select("#map-container").append("svg").attr("viewBox", [0, 0, width, height]);
const g = svg.append("g");

// Data URLs - Now down to just 3 files
const NA_UPDATE_URL = "measles_na_update.csv"; // Unified US/CA/MX data from Cron
const NA_GEO = "https://raw.githubusercontent.com/codeforgermany/click_dummy_poverty_map/master/src/data/topojson/north-america.json";
const US_GEO = "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json";

const colorScale = d3.scaleThreshold()
    .domain([1, 10, 50, 100, 500])
    .range(["#1a1a1a", "#4a0404", "#8a0808", "#c40c0c", "#ff1111", "#ff5555"]);

// 1. Unified Parallel Load
Promise.all([
    d3.json(NA_GEO),
    d3.json(US_GEO),
    d3.csv(NA_UPDATE_URL)
]).then(([na, us, rawData]) => {
    
    // Create a single lookup map for everything
    // US uses FIPS (ISO3166_2 column), Intl uses Names (Province_State column)
    const masterDataMap = new Map();
    rawData.forEach(d => {
        if (d.Country_Region === "US") {
            masterDataMap.set(d.ISO3166_2, +d.Confirmed);
        } else {
            masterDataMap.set(d.Province_State, +d.Confirmed);
        }
    });

    // 2. Draw Canada & Mexico
    const intlFeatures = topojson.feature(na, na.objects.na_states).features
        .filter(d => d.properties.country !== "USA");

    g.append("g")
        .selectAll("path")
        .data(intlFeatures)
        .join("path")
        .attr("d", path)
        .attr("class", "intl-province")
        .attr("fill", d => colorScale(masterDataMap.get(d.properties.name) || 0))
        .attr("stroke", "#333")
        .on("mouseover", (e, d) => showTooltip(e, d.properties.name, masterDataMap.get(d.properties.name)))
        .on("mouseout", hideTooltip);

    // 3. Draw US States (Now colored immediately)
    g.append("g")
        .selectAll("path")
        .data(topojson.feature(us, us.objects.states).features)
        .join("path")
        .attr("d", path)
        .attr("class", "state")
        .attr("fill", d => {
            // Aggregate county data for the state color
            let stateSum = 0;
            rawData.forEach(row => {
                if (row.Country_Region === "US" && row.ISO3166_2.startsWith(d.id)) {
                    stateSum += +row.Confirmed;
                }
            });
            return stateSum > 0 ? colorScale(stateSum) : "#151515";
        })
        .attr("stroke", "#444")
        .on("click", (e, d) => clicked(e, d, us, rawData, colorScale)); // Pass data to zoom handler

    d3.select("#status").html("● North America Synchronized");
});

// 4. Optimized Zoom Handler
function clicked(event, d, usTopology, rawData, color) {
    // Zoom logic remains same...
    // But renderCounties now pulls from your local 'rawData' array
    renderCounties(d, usTopology, rawData, color);
}

function renderCounties(stateFeature, us, rawData, color) {
    g.selectAll(".county").remove();
    const counties = topojson.feature(us, us.objects.counties).features
        .filter(c => c.id.startsWith(stateFeature.id));

    const countyMap = new Map(rawData.filter(r => r.Country_Region === "US").map(r => [r.ISO3166_2, +r.Confirmed]));

    g.append("g")
        .selectAll("path")
        .data(counties)
        .join("path")
        .attr("class", "county")
        .attr("d", path)
        .attr("fill", d => color(countyMap.get(d.id) || 0))
        .style("opacity", 0)
        .transition().duration(400).style("opacity", 1);
}

function showTooltip(event, name, cases = 0) {
    d3.select(".tooltip")
        .style("opacity", 1)
        .html(`<strong>${name}</strong><br>Confirmed Cases: ${cases.toLocaleString()}`)
        .style("left", `${event.pageX}px`)
        .style("top", `${event.pageY - 20}px`);
}

function hideTooltip() { d3.select(".tooltip").style("opacity", 0); }
