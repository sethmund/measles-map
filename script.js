const width = window.innerWidth;
const height = window.innerHeight;

const svg = d3.select("#map-container")
    .append("svg")
    .attr("viewBox", [0, 0, width, height])
    .on("click", reset);

const g = svg.append("g");
const path = d3.geoPath();

// JHU Raw Data URL
const JHU_URL = "https://raw.githubusercontent.com/CSSEGISandData/measles_data/main/measles_daily_cases_by_county.csv";

let active = d3.select(null);

// Color Scale (Red intensity)
const colorScale = d3.scaleThreshold()
    .domain([1, 5, 10, 20, 50])
    .range(["#fee5d9", "#fcbba1", "#fc9272", "#fb6a4a", "#de2d26", "#a50f15"]);

// Load Map Geometry and Live Data
Promise.all([
    d3.json("https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json"),
    d3.csv(JHU_URL)
]).then(([us, rawData]) => {

    // 1. Process Data: Aggregate daily rows into total cases by FIPS
    // JHU data has one row per day/county. We sum 'value' by 'location_id' (FIPS)
    const casesByFips = d3.rollups(rawData, 
        v => d3.sum(v, d => +d.value), 
        d => d.location_id.padStart(5, "0") // Ensure FIPS is 5 digits (e.g. "06037")
    );
    
    const dataMap = new Map(casesByFips);
    
    // Update Stats Panel
    const totalCases = d3.sum(casesByFips, d => d[1]);
    d3.select("#stats").html(`<strong>Total US Cases:</strong> ${totalCases}`);

    // 2. Draw Counties (Bottom Layer - Initially Hidden)
    g.append("g")
        .selectAll("path")
        .data(topojson.feature(us, us.objects.counties).features)
        .join("path")
        .attr("d", path)
        .attr("class", "county")
        .attr("fill", d => {
            const cases = dataMap.get(d.id); 
            return cases ? colorScale(cases) : "#e0e0e0";
        })
        .attr("opacity", 0) // Hidden until zoom
        .on("mouseover", (event, d) => showTooltip(event, d, dataMap))
        .on("mouseout", hideTooltip);

    // 3. Draw States (Top Layer - Interactive)
    g.append("g")
        .selectAll("path")
        .data(topojson.feature(us, us.objects.states).features)
        .join("path")
        .attr("d", path)
        .attr("class", "state")
        .attr("fill", d => {
            // Aggregate county cases to calculate state total color
            // (Optional: simple grey state view is often cleaner, keeping it simple here)
            return "#ccc"; 
        })
        .on("click", clicked);

    // 4. Draw State Boundaries (Overlay)
    g.append("path")
        .datum(topojson.mesh(us, us.objects.states, (a, b) => a !== b))
        .attr("class", "state-boundary")
        .attr("d", path);
});

// --- Interaction Logic ---

function clicked(event, d) {
    if (active.node() === this) return reset();

    active.classed("active", false);
    active = d3.select(this).classed("active", true);

    const [[x0, y0], [x1, y1]] = path.bounds(d);
    const dx = x1 - x0, dy = y1 - y0;
    const x = (x0 + x1) / 2, y = (y0 + y1) / 2;
    const scale = Math.max(1, Math.min(8, 0.9 / Math.max(dx / width, dy / height)));
    const translate = [width / 2 - scale * x, height / 2 - scale * y];

    // Zoom Animation
    svg.transition().duration(750).call(
        zoomFn, d3.zoomIdentity.translate(translate[0], translate[1]).scale(scale)
    );

    // Semantic Zoom: Fade out States, Fade in Counties
    d3.selectAll(".state").transition().duration(750).style("fill-opacity", 0.05).style("stroke", "none");
    d3.selectAll(".county").transition().duration(750).attr("opacity", 1);
}

function reset() {
    active.classed("active", false);
    active = d3.select(null);

    svg.transition().duration(750).call(zoomFn, d3.zoomIdentity);

    // Reset View
    d3.selectAll(".state").transition().duration(750).style("fill-opacity", 1).style("stroke", "#fff");
    d3.selectAll(".county").transition().duration(750).attr("opacity", 0);
}

function zoomFn(transition, transform) {
    g.transition(transition).attr("transform", transform);
}

// Tooltip Logic
function showTooltip(event, d, dataMap) {
    const cases = dataMap.get(d.id) || 0;
    const tooltip = d3.select("#tooltip");
    
    tooltip.html(`<strong>${d.properties.name}</strong><br>Cases: ${cases}`)
        .style("left", (event.pageX + 10) + "px")
        .style("top", (event.pageY - 28) + "px")
        .classed("hidden", false)
        .transition().style("opacity", 1);
}

function hideTooltip() {
    d3.select("#tooltip").transition().style("opacity", 0);
}
