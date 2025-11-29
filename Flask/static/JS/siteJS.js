// Global state for Database Toggle
let useNoSQL = localStorage.getItem('dashboard_useNoSQL') === 'true';

// Helper to update UI Text/Colors
function updateDashboardUI() {
    const title = document.getElementById('dashboard-title');
    const labelSql = document.getElementById('db-label-sql');
    const labelNoSql = document.getElementById('db-label-nosql');
    const toggle = document.getElementById('dashboardToggle');
    
    // Safety check: if elements aren't found (e.g., on a different page), stop here
    if (!title || !labelSql || !labelNoSql || !toggle) return;

    if (useNoSQL) {
        // Set to NoSQL State (Green)
        toggle.checked = true;
        title.textContent = "NoSQL Database Analytics (MongoDB)";
        title.style.color = "#2E7D32"; 
        labelSql.style.color = "#aaa";
        labelNoSql.style.color = "#4CAF50";
    } else {
        // Set to SQL State (Blue)
        toggle.checked = false;
        title.textContent = "Relational Database Analytics (SQLite)";
        title.style.color = "#333";
        labelSql.style.color = "#2196F3";
        labelNoSql.style.color = "#aaa";
    }
}

// Tab functionality
function openOptions(optionName, elmnt) {
    var i, tabcontent, tablinks;
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }
    tablinks = document.getElementsByClassName("tablink");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].classList.remove("active");
    }
    document.getElementById(optionName).style.display = "block";
    elmnt.classList.add("active");
}

// Store chart instances globally to destroy them before recreating
let chartInstances = {};

// Helper function to destroy and create chart
function createOrUpdateChart(canvasId, config) {
    const ctx = document.getElementById(canvasId);
    if (!ctx) return; // Skip if canvas doesn't exist

    // Destroy existing chart if it exists
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    // Create new chart
    chartInstances[canvasId] = new Chart(ctx.getContext('2d'), config);
}

// Toggle Source Function
function toggleDashboardSource() {
    const toggle = document.getElementById('dashboardToggle');
    useNoSQL = toggle.checked;
    
    // Save new state to browser storage
    localStorage.setItem('dashboard_useNoSQL', useNoSQL);
    
    // Update UI and Reload Data
    updateDashboardUI();
    loadDashboardData();
}

// Initialize on load
document.addEventListener('DOMContentLoaded', function() {
    // 1. Restore Checkbox State
    const toggle = document.getElementById('dashboardToggle');
    if(toggle) {
        toggle.checked = useNoSQL;
        // 2. Restore UI Text/Colors
        updateDashboardUI();
    }
    
    // 3. Load Data
    loadDashboardData();
});

// === PERFORMANCE WIDGET LOGIC ===
function togglePerfLog() {
    const widget = document.getElementById('perf-widget');
    const icon = document.getElementById('perf-toggle-icon');
    if (widget && icon) {
        widget.classList.toggle('open');
        icon.innerHTML = widget.classList.contains('open') ? '<i class="fas fa-chevron-down"></i>' : '<i class="fas fa-chevron-up"></i>';
    }
}

function logPerformance(logs) {
    const container = document.getElementById('perf-log-content');
    if (!container || !logs || logs.length === 0) return;

    logs.forEach(log => {
        const planStr = log.plan ? log.plan.join(' ') : '';
        const isIndexUsed = planStr.includes('USING INDEX') || planStr.includes('COVERING INDEX') || planStr.includes('IXSCAN');
        const isScan = planStr.includes('SCAN TABLE') || planStr.includes('COLLSCAN');
        
        // 1. Determine Scan/Index Tag
        let methodTagClass = '';
        let methodTagText = 'QUERY';
        
        if (isIndexUsed) {
            methodTagClass = 'tag-index';
            methodTagText = 'INDEXED';
        } else if (isScan) {
            methodTagClass = 'tag-scan';
            methodTagText = 'SCAN';
        }

        // 2. Determine DB Type Tag (SQL vs NoSQL)
        const isNoSQL = log.type === 'NoSQL';
        const dbTypeColor = isNoSQL ? '#4CAF50' : '#2196F3'; // Green for NoSQL, Blue for SQL
        const dbTypeText = log.type || 'SYSTEM';

        // 3. Create the Entry
        const entry = document.createElement('div');
        entry.className = `log-entry ${isNoSQL ? 'nosql' : 'sql'}`;
        
        let planHtml = log.plan ? log.plan.map(p => `<div>${p}</div>`).join('') : '';
        
        entry.innerHTML = `
            <span class="log-title">
                <span class="log-tag" style="background-color: ${dbTypeColor}; color: white;">${dbTypeText}</span>
                <span class="log-tag ${methodTagClass}">${methodTagText}</span>
                ${log.label}
            </span>
            <span class="log-time">${log.duration} ms</span>
            <span class="log-detail">${planHtml}</span>
        `;
        
        container.insertBefore(entry, container.firstChild); 
    });
}

// Initialize on load
document.addEventListener('DOMContentLoaded', function() {
    loadDashboardData();
});

// === SAFE RENDER HELPER ===
function safeRender(renderFunction, data, name) {
    try {
        renderFunction(data);
    } catch (e) {
        console.error(`Failed to render chart [${name}]:`, e);
    }
}

async function loadDashboardData() {
    const logContainer = document.getElementById('perf-log-content');
    if(logContainer) {
        // Don't wipe previous logs, just append/prepend
        // logContainer.innerHTML = '<div class="log-entry system">Fetching new data...</div>';
    }

    try {
        const prefix = useNoSQL ? '/api/nosql' : '/api';
        
        // Fetch all data in parallel
        const responses = await Promise.all([
            fetch(`${prefix}/flight-operations`).then(r => r.json()),
            fetch(`${prefix}/route-performance`).then(r => r.json()),
            fetch(`${prefix}/passenger-demand`).then(r => r.json()),
            fetch(`${prefix}/revenue-analysis`).then(r => r.json()),
            fetch(`${prefix}/resource-planning`).then(r => r.json())
        ]);

        const [flightOps, routePerf, passengerDemand, revenue, resources] = responses;

        // === AGGREGATE LOGS ===
        let allLogs = [];
        responses.forEach(res => {
            if (res._perf) allLogs = allLogs.concat(res._perf);
        });
        logPerformance(allLogs);

        // === RENDER CHARTS SAFELY ===
        if(flightOps) {
            safeRender(updateKeyMetrics, flightOps.overview, "Key Metrics");
            safeRender(createPunctualChart, flightOps.least_punctual_routes, "Punctuality");
        }

        if(routePerf) {
            safeRender(createRoutesChart, routePerf.busiest_routes, "Route Perf");
        }

        if(passengerDemand) {
            safeRender(createOccupancyChart, passengerDemand.top_occupancy_routes, "Occupancy");
            safeRender(createMarketShareChart, passengerDemand.busiest_routes_market_share, "Market Share");
            safeRender(createLeastBusyChart, passengerDemand.least_busy_routes, "Least Busy");
        }

        if(revenue) {
            try {
                createProfitableRoutesChart(revenue.top_revenue_routes, revenue.least_revenue_routes);
            } catch (e) { console.error("Failed Profit Chart", e); }

            safeRender(createRevenueClassChart, revenue.revenue_by_class, "Rev Class");
            safeRender(createRevenueClassRouteChart, revenue.revenue_by_class_route, "Rev Route");
        }

        if(resources) {
            safeRender(createAircraftRouteChart, resources.aircraft_by_route, "Matrix Chart");
            safeRender(createDestinationsChart, resources.top_destinations, "Destinations");
            safeRender(createUtilizationChart, resources.aircraft_utilization, "Utilization");
            if(resources.cancellation_stats) {
                createCancellationDelayChart(resources.cancellation_stats);
            }
        }
        
    } catch (error) {
        console.error('CRITICAL ERROR loading dashboard:', error);
    }
}

function createCancellationDelayChart(data) {
    if(!data) data = [];
    const labels = data.map(d => {
        try { return JSON.parse(d.model).en; } catch(e) { return d.model || d.aircraft_model; }
    });

    createOrUpdateChart('cancellationDelayChart', {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [
                {
                    label: 'Avg Delay (Minutes)',
                    data: data.map(d => d.avg_delay_minutes),
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1,
                    order: 2,
                    yAxisID: 'y'
                },
                {
                    label: 'Total Cancellations',
                    data: data.map(d => d.total_cancellations),
                    borderColor: '#FF5722',
                    backgroundColor: '#FF5722',
                    type: 'line',
                    tension: 0.4,
                    borderWidth: 3,
                    order: 1,
                    yAxisID: 'y1'
                }
            ]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: { legend: { position: 'top' } },
            scales: {
                y: { type: 'linear', display: true, position: 'left', title: { display: true, text: 'Avg Delay (min)' } },
                y1: { type: 'linear', display: true, position: 'right', title: { display: true, text: 'Cancellations', color: '#FF5722' }, grid: { drawOnChartArea: false }, beginAtZero: true }
            }
        }
    });
}

function updateKeyMetrics(overview) {
    if(!overview) return;
    document.getElementById('totalFlights').textContent = (overview.total_flights || 0).toLocaleString();
    let ontimeRate = 0;
    if(overview.total_flights > 0) {
        ontimeRate = ((overview.ontime_flights / overview.total_flights) * 100).toFixed(1);
    }
    document.getElementById('ontimeRate').textContent = ontimeRate + '%';
    document.getElementById('avgDelay').textContent = overview.avg_delay_minutes || 0;
    document.getElementById('cancellations').textContent = (overview.cancelled_flights || 0).toLocaleString();
}

// 1. Top 5 Least Punctual Routes
function createPunctualChart(data) {
    if(!data) data = [];
    createOrUpdateChart('punctualChart', {
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'Average Delay (mins)',
                data: data.map(d => d.avg_delay_mins),
                backgroundColor: '#FFCE56',
                borderColor: '#FFCE56',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            plugins: { legend: { display: false } },
            scales: { x: { beginAtZero: true, title: { display: true, text: 'Average Delay (mins)' } } }
        }
    });
}

// 5. Top 10 Routes by Flight Volume
function createRoutesChart(data) {
    if(!data) data = [];
    createOrUpdateChart('routesChart', { 
        type: 'treemap',
        data: {
            datasets: [{
                label: 'Flight Volume',
                tree: data, 
                key: 'flight_count',
                groups: ['route'],
                backgroundColor: function(context) {
                    if (!context.raw) return '#eee';
                    const value = context.raw.v;
                    const alpha = (value / 350) + 0.3; 
                    return `rgba(54, 162, 235, ${alpha})`;
                },
                borderColor: 'white',
                borderWidth: 2,
                borderRadius: 6,
                spacing: 1,
                labels: {
                    display: true, color: 'white', align: 'left', position: 'top',
                    font: { size: 14, weight: 'bold' },
                    formatter: function(context) { return context.raw.g; }
                }
            }]
        },
        options: {
            maintainAspectRatio: false,
            plugins: { legend: { display: false }, title: { display: true, text: 'Top 10 Routes by Flight Volume', font: { size: 16 } } }
        }
    });
}

// 2. Top 10 Routes by Average Occupancy Rate
function createOccupancyChart(data) {
    if(!data) data = [];
    createOrUpdateChart('occupancyChart', {
        type: 'line',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'Occupancy Rate (%)',
                data: data.map(d => d.avg_occupancy_percent),
                backgroundColor: 'rgba(153, 102, 255, 0.7)', 
                borderColor: 'rgba(120, 81, 245, 0.5)', 
                borderWidth: 2, borderDash: [5, 5],
                pointRadius: 8, pointHoverRadius: 10,
                pointBackgroundColor: 'rgba(153, 102, 255, 0.7)',
                pointBorderColor: '#ffffff', pointBorderWidth: 2,
                showLine: true, tension: 0.3
            }]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: {
                y: { min: 50, max: 105, beginAtZero: false, title: { display: true, text: 'Occupancy %' }, grid: { color: 'rgba(0,0,0,0.1)', borderDash: [5, 5] } },
                x: { ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } }, grid: { display: true, color: 'rgba(0,0,0,0.05)', borderDash: [5, 5] } }
            },
            plugins: { legend: { display: false } }
        }
    });
}

function createMarketShareChart(data) {
    if(!data) data = [];
    const sortedData = [...data].sort((a, b) => b.market_share_percent - a.market_share_percent);
    
    createOrUpdateChart('marketShareChart', {
        type: 'bar',
        data: {
            labels: sortedData.map(d => d.route),
            datasets: [
                { type: 'line', label: 'Market Share (%)', data: sortedData.map(d => d.market_share_percent), backgroundColor: '#2196F3', borderColor: '#2196F3', pointRadius: 6, pointHoverRadius: 8, borderWidth: 0, fill: false },
                { type: 'bar', label: 'Stick', data: sortedData.map(d => d.market_share_percent), backgroundColor: 'rgba(33, 150, 243, 0.5)', barPercentage: 0.1, categoryPercentage: 0.8 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            scales: { y: { beginAtZero: true, title: { display: true, text: 'Market Share (%)' }, grid: { color: 'rgba(0,0,0,0.05)', borderDash: [5, 5] } }, x: { ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } }, grid: { display: false } } },
            plugins: { legend: { display: false }, tooltip: { filter: function(tooltipItem) { return tooltipItem.datasetIndex === 0; } } }
        }
    });
}

function createLeastBusyChart(data) {
    if(!data) data = [];
    data.sort((a, b) => a.market_share_percent - b.market_share_percent);

    createOrUpdateChart('leastBusyChart', {
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [
                { type: 'line', label: 'Market Share %', data: data.map(d => d.market_share_percent), backgroundColor: '#FF5722', borderColor: '#FF5722', pointRadius: 6, pointHoverRadius: 8, borderWidth: 0, fill: false },
                { type: 'bar', label: 'Stick', data: data.map(d => d.market_share_percent), backgroundColor: 'rgba(255, 87, 34, 0.5)', barPercentage: 0.1, categoryPercentage: 0.8 }
            ]
        },
        options: {
            responsive: true, maintainAspectRatio: false,
            plugins: { legend: { display: false }, tooltip: { filter: function(tooltipItem) { return tooltipItem.datasetIndex === 0; } } },
            scales: { y: { beginAtZero: true, title: { display: true, text: 'Market Share (%)' }, grid: { color: 'rgba(0,0,0,0.05)', borderDash: [5, 5] } }, x: { ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } }, grid: { display: false } } }
        }
    });
}

function createProfitableRoutesChart(topRoutes, leastRoutes) {
    if(!topRoutes) topRoutes = [];
    if(!leastRoutes) leastRoutes = [];
    
    createOrUpdateChart('profitableRoutesChart', {
        type: 'bar',
        data: { labels: topRoutes.map(d => d.route), datasets: [{ label: 'Revenue ($)', data: topRoutes.map(d => d.total_revenue), backgroundColor: '#66BB6A', }] },
        options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', scales: { x: { beginAtZero: true, ticks: { callback: function(value) { return '$' + (value / 1000000).toFixed(1) + 'M'; } } } }, plugins: { title: { display: true, text: 'Most Profitable Routes', color: '#66BB6A' }, legend: { display: false } } }
    });
    
    createOrUpdateChart('leastProfitableChart', {
        type: 'bar',
        data: { labels: leastRoutes.map(d => d.route), datasets: [{ label: 'Revenue ($)', data: leastRoutes.map(d => d.total_revenue), backgroundColor: '#EF5350', }] },
        options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', scales: { x: { beginAtZero: true, ticks: { callback: function(value) { return '$' + (value / 1000).toFixed(0) + 'K'; } } } }, plugins: { title: { display: true, text: 'Least Profitable Routes', color: '#EF5350' }, legend: { display: false } } }
    });
}

function createRevenueClassChart(data) {
    if(!data) data = [];
    createOrUpdateChart('revenueClassChart', {
        type: 'pie',
        data: { labels: data.map(d => d.fare_conditions), datasets: [{ data: data.map(d => d.total_revenue_by_class), backgroundColor: ['rgba(255, 99, 132, 0.7)', 'rgba(54, 162, 235, 0.7)', 'rgba(255, 206, 86, 0.7)'], }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }
    });
}

function createRevenueClassRouteChart(data) {
    if(!data || data.length === 0) return;
    const uniqueRoutes = [...new Set(data.map(d => d.route))];
    const uniqueClasses = ['Economy', 'Business', 'Comfort'];

    const routeTotals = uniqueRoutes.map(route => {
        const total = data.filter(d => d.route === route).reduce((sum, current) => sum + current.total_revenue_by_class, 0);
        return { route, total };
    });
    const sortedRoutes = routeTotals.sort((a, b) => b.total - a.total).slice(0, 20).map(item => item.route);
    const classColors = { 'Economy': 'rgba(153, 102, 255, 0.7)', 'Business': 'rgba(75, 192, 192, 0.7)', 'Comfort': '#FFC107' };
    const datasets = uniqueClasses.map(fareClass => {
        return {
            label: fareClass, backgroundColor: classColors[fareClass] || '#999',
            data: sortedRoutes.map(route => { const record = data.find(d => d.route === route && d.fare_conditions === fareClass); return record ? record.total_revenue_by_class : 0; })
        };
    });

    createOrUpdateChart('revenueClassRouteChart', {
        type: 'bar', data: { labels: sortedRoutes, datasets: datasets },
        options: { responsive: true, maintainAspectRatio: false, scales: { x: { stacked: true, ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } }, title: { display: true, text: 'Route' } }, y: { stacked: true, beginAtZero: true, title: { display: true, text: 'Total Revenue ($)' }, ticks: { callback: function(value) { return '$' + (value / 1000000).toFixed(0) + 'M'; } } } }, plugins: { tooltip: { mode: 'index', intersect: false }, legend: { position: 'top' } } }
    });
}

let cachedAircraftData = [];

function createAircraftRouteChart(data) {
    if (!data || data.length === 0) return;
    cachedAircraftData = data;
    const dropdown = document.getElementById('routeSelect');
    if (dropdown) {
        const currentSelection = dropdown.value;
        dropdown.innerHTML = '<option value="ALL">Show Top 15 Routes</option>';
        const allRoutes = [...new Set(data.map(d => d.route))].sort();
        allRoutes.forEach(route => { const option = document.createElement('option'); option.value = route; option.textContent = route; dropdown.appendChild(option); });
        if (allRoutes.includes(currentSelection)) { dropdown.value = currentSelection; }
    }
    renderFilteredAircraftChart();
}

function renderFilteredAircraftChart() {
    const dropdown = document.getElementById('routeSelect');
    const selectedRoute = dropdown ? dropdown.value : 'ALL';
    if (!cachedAircraftData || cachedAircraftData.length === 0) return;

    let displayData = [], routesToShow = [];
    if (selectedRoute === 'ALL') {
        routesToShow = [...new Set(cachedAircraftData.map(d => d.route))].slice(0, 15);
        displayData = cachedAircraftData.filter(d => routesToShow.includes(d.route));
    } else {
        routesToShow = [selectedRoute];
        displayData = cachedAircraftData.filter(d => d.route === selectedRoute);
    }
    const allAircraft = [...new Set(displayData.map(d => d.aircraft_code))];
    const colors = { 'SU9': 'rgba(255, 99, 132, 0.7)', 'CN1': 'rgba(255, 206, 86, 0.7)', '321': 'rgba(75, 192, 192, 0.7)', '773': 'rgba(54, 162, 235, 0.7)', '763': 'rgba(153, 102, 255, 0.7)', 'CR2': 'rgba(255, 159, 64, 0.7)', '319': 'rgba(201, 203, 207, 0.7)', '733': 'rgba(100, 221, 23, 0.7)' };
    const datasets = allAircraft.map(ac => {
        return { label: ac, backgroundColor: colors[ac] || 'rgba(0,0,0,0.5)', data: routesToShow.map(route => { const record = displayData.find(d => d.route === route && d.aircraft_code === ac); return record ? record.total_flights_on_route : 0; }) };
    });

    createOrUpdateChart('aircraftRouteChart', {
        type: 'bar', data: { labels: routesToShow, datasets: datasets },
        options: { responsive: true, maintainAspectRatio: false, scales: { x: { stacked: true, ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } }, title: { display: true, text: selectedRoute === 'ALL' ? 'Top Routes' : 'Selected Route' } }, y: { stacked: true, beginAtZero: true, title: { display: true, text: 'Flight Frequency' } } }, plugins: { tooltip: { mode: 'index', intersect: false }, legend: { position: 'top' } } }
    });
}

function createDestinationsChart(data) {
    if(!data) data = [];
    createOrUpdateChart('destinationsChart', {
        type: 'doughnut', data: { labels: data.map(d => `${d.city} (${d.airport_code})`), datasets: [{ data: data.map(d => d.total_arrivals), backgroundColor: ['#FF6384', '#36A2EB', '#FFCE56'], }] },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' } } }
    });
}

function createUtilizationChart(data) {
    if(!data) data = [];
    data.sort((a, b) => b.total_mileage - a.total_mileage);
    const top5Data = data.slice(0, 5);
    const backgroundColors = ['rgba(255, 99, 132, 0.7)', 'rgba(54, 162, 235, 0.7)', 'rgba(255, 206, 86, 0.7)', 'rgba(75, 192, 192, 0.7)', 'rgba(153, 102, 255, 0.7)'];

    createOrUpdateChart('utilizationChart', {
        type: 'polarArea',
        data: { labels: top5Data.map(d => `${d.aircraft_code} (${d.aircraft_model}) : ${(d.total_mileage/1000).toFixed(0)}k mi`), datasets: [{ label: 'Total Mileage', data: top5Data.map(d => d.total_mileage), backgroundColor: backgroundColors, borderWidth: 1 }] },
        options: { responsive: true, maintainAspectRatio: false, layout: { padding: 10 }, scales: { r: { grid: { color: 'rgba(200, 200, 200, 0.3)' }, ticks: { display: false }, pointLabels: { display: false } } }, plugins: { legend: { display: true, position: 'right', labels: { font: { size: 11 }, padding: 15 } }, tooltip: { callbacks: { label: function(context) { return ' ' + context.formattedValue + ' miles'; } } } } }
    });
}
// Initialize aircraft filter dropdown
function initializeAircraftFilter(aircraftList) {
    const filterSelect = document.getElementById('aircraftFilter');
    if(!filterSelect) return;
    filterSelect.innerHTML = '';
    
    if(!aircraftList || aircraftList.length === 0) {
        const option = document.createElement('option');
        option.text = "No Aircraft Data";
        filterSelect.appendChild(option);
        return;
    }
    
    aircraftList.forEach(aircraft => {
        const option = document.createElement('option');
        option.value = aircraft.aircraft_code;
        option.textContent = `${aircraft.aircraft_code} - ${aircraft.aircraft_model}`;
        filterSelect.appendChild(option);
    });
    
    if(aircraftList.length > 0) filterSelect.value = aircraftList[0].aircraft_code;
    
    filterSelect.addEventListener('change', async function() {
        await loadAircraftRoutes(this.value);
    });
}

async function loadAircraftRoutes(aircraftCode) {
    if(!aircraftCode) return;
    const tbody = document.getElementById('su9RoutesBody');
    if(!tbody) return;
    tbody.innerHTML = '<tr><td colspan="4" class="text-center">Loading...</td></tr>';
    
    try {
        const prefix = useNoSQL ? '/api/nosql' : '/api';
        const response = await fetch(`${prefix}/aircraft-routes/${aircraftCode}`);
        if (!response.ok) throw new Error(`Failed to load routes`);
        const data = await response.json();
        createSU9RoutesTable(data.routes);
    } catch (error) {
        console.error('Error loading aircraft routes:', error);
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-danger">Error loading data</td></tr>';
    }
}

function createSU9RoutesTable(data) {
    const tbody = document.getElementById('su9RoutesBody');
    if(!tbody) return;
    tbody.innerHTML = '';
    
    if (!data || data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center">No data available</td></tr>';
        return;
    }
    
    data.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${row.flight_id}</td><td>${row.route}</td><td>${row.scheduled_departure_time}</td><td><span class="badge bg-${getStatusColor(row.status)}">${row.status}</span></td>`;
        tbody.appendChild(tr);
    });
}

function getStatusColor(status) {
    if (status === 'Arrived' || status === 'On Time') return 'success';
    if (status.includes('Delayed')) return 'warning';
    if (status.includes('Cancel')) return 'danger';
    return 'secondary';
}