const width = window.innerWidth;
const height = window.innerHeight;

// Change to a North American projection to include CA and MX
const projection = d3.geoConicConic()
    .parallels([15, 60])
    .rotate([100, 0])
    .center([0, 38])
    .scale(width * 0.6)
    .translate([width / 2, height / 2]);

const path = d3.geoPath().projection(projection);

const svg = d3.select("#map-container")
    .append("svg")
    .attr("viewBox", [0, 0, width, height])
    .on("click", reset);

const g = svg.append("g");

// Data URLs
const JHU_URL = "https://raw.githubusercontent.com/CSSEGISandData/measles_data/main/measles_daily_cases_by_county.csv";
const NA_UPDATE_URL = "measles_na_update.csv"; // Your new ETL output

const colorScale = d3.scaleThreshold()
    .domain([1, 10, 50, 100, 500])
    .range(["#fee5d9", "#fcae91", "#fb6a4a", "#de2d26", "#a50f15", "#67000d"]);

Promise.all([
    d3.json("https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json"),
    d3.json("https://raw.githubusercontent.com/codeforgermany/click_dummy_poverty_map/master/src/data/topojson/north-america.json"),
    d3.csv(JHU_URL),
    d3.csv(NA_UPDATE_URL)
]).then(([us, na, rawUS, rawNA]) => {

    // 1. Process US County Data
    const usDataMap = new Map(d3.rollups(rawUS, 
        v => d3.sum(v, d => +d.value), 
        d => d.location_id.padStart(5, "0")
    ));

    // 2. Process International Provincial Data (Canada/Mexico)
    const intlDataMap = new Map(rawNA.map(d => [d.Province_State, +d.Confirmed]));

    // Update Global Stats
    const totalGlobal = d3.sum([...usDataMap.values()]) + d3.sum([...intlDataMap.values()]);
    d3.select("#stats").html(`<strong>Total North American Cases (2026):</strong> ${totalGlobal.toLocaleString()}`);

    // 3. Draw Canada & Mexico (Provincial Layer)
    // Filter North America topojson for non-US features
    const intlFeatures = topojson.feature(na, na.objects.na_states).features
        .filter(d => d.properties.country !== "USA");

    g.append("g")
        .selectAll("path")
        .data(intlFeatures)
        .join("path")
        .attr("d", path)
        .attr("class", "intl-province")
        .attr("fill", d => {
            const cases = intlDataMap.get(d.properties.name);
            return cases ? colorScale(cases) : "#f0f0f0";
        })
        .attr("stroke", "#fff")
        .on("mouseover", (event, d) => showTooltip(event, d.properties.name, intlDataMap.get(d.properties.name)))
        .on("mouseout", hideTooltip);

    // 4. Draw US Counties (Bottom Layer - Initially Hidden)
    g.append("g")
        .selectAll("path")
        .data(topojson.feature(us, us.objects.counties).features)
        .join("path")
        .attr("d", path)
        .attr("class", "county")
        .attr("fill", d => colorScale(usDataMap.get(d.id) || 0))
        .attr("opacity", 0)
        .on("mouseover", (event, d) => showTooltip(event, d.properties.name, usDataMap.get(d.id)))
        .on("mouseout", hideTooltip);

    // 5. Draw US States (Interactive Top Layer)
    g.append("g")
        .selectAll("path")
        .data(topojson.feature(us, us.objects.states).features)
        .join("path")
        .attr("d", path)
        .attr("class", "state")
        .attr("fill", "#ccc")
        .attr("stroke", "#fff")
        .on("click", clicked);
});

// Tooltip helper
function showTooltip(event, name, cases = 0) {
    d3.select("#tooltip")
        .html(`<strong>${name}</strong><br>Cases: ${cases}`)
        .style("left", (event.pageX + 10) + "px")
        .style("top", (event.pageY - 28) + "px")
        .transition().duration(200).style("opacity", 1);
}
