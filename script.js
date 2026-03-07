const width = window.innerWidth;
const height = window.innerHeight;

// Optimization: Use a constant for the projection to avoid re-calculation
const projection = d3.geoConicConic()
    .parallels([15, 60])
    .rotate([100, 0])
    .center([0, 38])
    .scale(width * 0.5)
    .translate([width / 2, height / 2]);

const path = d3.geoPath().projection(projection);
const svg = d3.select("#map-container").append("svg").attr("viewBox", [0, 0, width, height]);
const g = svg.append("g");

// Data URLs
const JHU_URL = "https://raw.githubusercontent.com/CSSEGISandData/measles_data/main/measles_daily_cases_by_county.csv";
const NA_UPDATE_URL = "measles_na_update.csv";
const NA_GEO = "https://raw.githubusercontent.com/codeforgermany/click_dummy_poverty_map/master/src/data/topojson/north-america.json";
const US_GEO = "https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json";

const colorScale = d3.scaleThreshold()
    .domain([1, 10, 50, 100, 500])
    .range(["#1a1a1a", "#4a0404", "#8a0808", "#c40c0c", "#ff1111", "#ff5555"]);

// Performance: Cache data in these variables
let usTopology, usDataMap, intlDataMap;

// 1. Initial Load: Only fetch Top-Level Geometry and Provincial Data
Promise.all([
    d3.json(NA_GEO),
    d3.csv(NA_UPDATE_URL),
    d3.json(US_GEO) // Pre-fetching US Atlas in background
]).then(([na, rawNA, us]) => {
    usTopology = us;
    intlDataMap = new Map(rawNA.map(d => [d.Province_State, +d.Confirmed]));

    // Draw Canada & Mexico immediately
    const intlFeatures = topojson.feature(na, na.objects.na_states).features
        .filter(d => d.properties.country !== "USA");

    g.append("g")
        .selectAll("path")
        .data(intlFeatures)
        .join("path")
        .attr("d", path)
        .attr("class", "intl-province")
        .attr("fill", d => colorScale(intlDataMap.get(d.properties.name) || 0))
        .attr("stroke", "#333")
        .on("mouseover", (e, d) => showTooltip(e, d.properties.name, intlDataMap.get(d.properties.name)))
        .on("mouseout", hideTooltip);

    // Draw US States Placeholder
    g.append("g")
        .selectAll("path")
        .data(topojson.feature(us, us.objects.states).features)
        .join("path")
        .attr("d", path)
        .attr("class", "state")
        .attr("fill", "#222")
        .attr("stroke", "#444")
        .on("click", loadUSDetailedData); // Trigger heavy load on first interaction

    d3.select("#status").html("● System Ready (Global)");
});

// 2. Heavy Lifting: Load JHU CSV only when requested or after initial render
async function loadUSDetailedData(event, d) {
    if (usDataMap) return clicked(event, d); // Already loaded

    d3.select("#status").html("⏳ Loading US County Data...");
    
    const rawUS = await d3.csv(JHU_URL);
    
    // Performance: Faster than d3.rollups for massive datasets
    usDataMap = new Map();
    for (let i = 0; i < rawUS.length; i++) {
        const row = rawUS[i];
        const fips = row.location_id.padStart(5, "0");
        const val = +row.value || 0;
        usDataMap.set(fips, (usDataMap.get(fips) || 0) + val);
    }

    // Update US State colors once data is in
    g.selectAll(".state").transition().duration(500)
        .attr("fill", stateFeature => {
            // Aggregate sum for state view
            let stateSum = 0;
            usDataMap.forEach((val, fips) => {
                if (fips.startsWith(stateFeature.id)) stateSum += val;
            });
            return stateSum > 0 ? colorScale(stateSum) : "#151515";
        });

    d3.select("#status").html("● US Data Synchronized");
    clicked(event, d);
}

// 3. Tooltip Logic (Optimized for no transition lag)
function showTooltip(event, name, cases = 0) {
    const tooltip = d3.select(".tooltip");
    tooltip.style("opacity", 1)
        .html(`<strong>${name}</strong><br>Cases: ${cases.toLocaleString()}`)
        .style("left", `${event.pageX}px`)
        .style("top", `${event.pageY - 20}px`);
}

function hideTooltip() {
    d3.select(".tooltip").style("opacity", 0);
}
