// Global state for Database Toggle
let useNoSQL = false;

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
    
    // Update UI visuals
    const title = document.getElementById('dashboard-title');
    const labelSql = document.getElementById('db-label-sql');
    const labelNoSql = document.getElementById('db-label-nosql');
    
    if (useNoSQL) {
        title.textContent = "NoSQL Database Analytics (MongoDB)";
        title.style.color = "#2E7D32"; // Green shade
        labelSql.style.color = "#aaa";
        labelNoSql.style.color = "#4CAF50";
    } else {
        title.textContent = "Relational Database Analytics (SQLite)";
        title.style.color = "#333";
        labelSql.style.color = "#2196F3";
        labelNoSql.style.color = "#aaa";
    }
    
    // Reload data
    loadDashboardData();
}

// === PERFORMANCE WIDGET LOGIC ===
function togglePerfLog() {
    const widget = document.getElementById('perf-widget');
    const icon = document.getElementById('perf-toggle-icon');
    widget.classList.toggle('open');
    icon.innerHTML = widget.classList.contains('open') ? '<i class="fas fa-chevron-down"></i>' : '<i class="fas fa-chevron-up"></i>';
}

function logPerformance(logs) {
    const container = document.getElementById('perf-log-content');
    if (!container || !logs || logs.length === 0) return;

    logs.forEach(log => {
        const planStr = log.plan ? log.plan.join(' ') : '';
        const isIndexUsed = planStr.includes('USING INDEX') || planStr.includes('COVERING INDEX') || planStr.includes('IXSCAN');
        const isScan = planStr.includes('SCAN TABLE') || planStr.includes('COLLSCAN');
        
        let tagClass = '';
        let tagText = 'QUERY';
        
        if (isIndexUsed) {
            tagClass = 'tag-index';
            tagText = 'INDEXED';
        } else if (isScan) {
            tagClass = 'tag-scan';
            tagText = 'SCAN';
        }
        
        const entry = document.createElement('div');
        entry.className = `log-entry ${log.type ? log.type.toLowerCase() : 'system'}`;
        
        let planHtml = log.plan ? log.plan.map(p => `<div>${p}</div>`).join('') : '';
        
        entry.innerHTML = `
            <span class="log-title">
                <span class="log-tag ${tagClass}">${tagText}</span>
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
        logContainer.innerHTML = '<div class="log-entry system">Fetching new data...</div>';
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
            // Wrapper to pass two args to the profitable chart
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
            
            if(resources.aircraft_list) initializeAircraftFilter(resources.aircraft_list);
            if(resources.su9_routes) createSU9RoutesTable(resources.su9_routes);
        }
        
    } catch (error) {
        console.error('CRITICAL ERROR loading dashboard:', error);
    }
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
                backgroundColor: 'rgba(255, 99, 132, 0.8)',
                borderColor: 'rgba(255, 99, 132, 1)',
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
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'Number of Flights',
                data: data.map(d => d.flight_count),
                backgroundColor: '#667eea',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } } },
                y: { beginAtZero: true }
            }
        }
    });
}

// 2. Top 10 Routes by Average Occupancy Rate
function createOccupancyChart(data) {
    if(!data) data = [];
    createOrUpdateChart('occupancyChart', {
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'Occupancy Rate (%)',
                data: data.map(d => d.avg_occupancy_percent),
                backgroundColor: '#4CAF50',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { beginAtZero: true, max: 100 },
                x: { ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } } }
            }
        }
    });
}

// 3. Busiest Routes by Market Share % - TRUE GRID HEATMAP
function createMarketShareChart(data) {
    if(!data) data = [];
    const sortedData = [...data].sort((a, b) => b.market_share_percent - a.market_share_percent);
    const matrixData = sortedData.map((d, index) => ({
        x: 0,
        y: index,
        v: d.market_share_percent,
        route: d.route,
        tickets: d.total_tickets_sold
    }));
    
    let maxShare = 0, minShare = 0;
    if(sortedData.length > 0) {
        maxShare = Math.max(...sortedData.map(d => d.market_share_percent));
        minShare = Math.min(...sortedData.map(d => d.market_share_percent));
    }
    
    createOrUpdateChart('marketShareChart', {
        type: 'matrix',
        data: {
            datasets: [{
                label: 'Market Share %',
                data: matrixData,
                backgroundColor(context) {
                    if (!context.dataset.data[context.dataIndex]) return 'rgba(33, 150, 243, 0)';
                    const value = context.dataset.data[context.dataIndex].v;
                    const intensity = (value - minShare) / (maxShare - minShare || 1);
                    const r = Math.round(33 + (200 - 33) * (1 - intensity));
                    const g = Math.round(150 + (230 - 150) * (1 - intensity));
                    const b = 243;
                    return `rgba(${r}, ${g}, ${b}, ${0.5 + intensity * 0.5})`;
                },
                borderColor: 'rgba(255, 255, 255, 0.8)',
                borderWidth: 2,
                width: ({chart}) => (chart.chartArea || {}).width * 0.8,
                height: ({chart}) => {
                    const count = sortedData.length || 1;
                    const height = ((chart.chartArea || {}).height / count) - 4;
                    return Math.max(height, 25);
                }
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        title: () => '',
                        label(context) {
                            const v = context.dataset.data[context.dataIndex];
                            return [`Route: ${v.route}`, `Market Share: ${v.v}%`];
                        }
                    }
                }
            },
            scales: {
                x: { display: false, min: -0.5, max: 0.5 },
                y: {
                    type: 'category',
                    labels: sortedData.map(d => d.route),
                    offset: true,
                    ticks: { font: { size: 11, weight: 'bold' }, color: '#333' },
                    grid: { display: false }
                }
            },
            layout: { padding: { left: 10, right: 20 } }
        }
    });
}

// 4. 10 Least Busy Routes
function createLeastBusyChart(data) {
    if(!data) data = [];
    createOrUpdateChart('leastBusyChart', {
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'Market Share (%)',
                data: data.map(d => d.market_share_percent),
                backgroundColor: '#FF5722',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true }, x: { ticks: { maxRotation: 45, minRotation: 45, font: { size: 10 } } } },
            plugins: { tooltip: { callbacks: { label: function(context) { return `Market Share: ${context.parsed.y}%`; } } } }
        }
    });
}

// 6. Top 3 Most & Least Profitable Routes
function createProfitableRoutesChart(topRoutes, leastRoutes) {
    if(!topRoutes) topRoutes = [];
    if(!leastRoutes) leastRoutes = [];
    
    createOrUpdateChart('profitableRoutesChart', {
        type: 'bar',
        data: {
            labels: topRoutes.map(d => d.route),
            datasets: [{
                label: 'Revenue ($)',
                data: topRoutes.map(d => d.total_revenue),
                backgroundColor: '#4CAF50',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            scales: { x: { beginAtZero: true, ticks: { callback: function(value) { return '$' + (value / 1000000).toFixed(1) + 'M'; } } } },
            plugins: {
                title: { display: true, text: 'Most Profitable Routes', color: '#4CAF50' },
                legend: { display: false }
            }
        }
    });
    
    createOrUpdateChart('leastProfitableChart', {
        type: 'bar',
        data: {
            labels: leastRoutes.map(d => d.route),
            datasets: [{
                label: 'Revenue ($)',
                data: leastRoutes.map(d => d.total_revenue),
                backgroundColor: '#f44336',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y',
            scales: { x: { beginAtZero: true, ticks: { callback: function(value) { return '$' + (value / 1000).toFixed(0) + 'K'; } } } },
            plugins: {
                title: { display: true, text: 'Least Profitable Routes', color: '#f44336' },
                legend: { display: false }
            }
        }
    });
}

// 7a. Revenue by Fare Class
function createRevenueClassChart(data) {
    if(!data) data = [];
    createOrUpdateChart('revenueClassChart', {
        type: 'pie',
        data: {
            labels: data.map(d => d.fare_conditions),
            datasets: [{
                data: data.map(d => d.total_revenue_by_class),
                backgroundColor: ['#2196F3', '#4CAF50', '#FFC107'],
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } }
        }
    });
}

// 7b. Revenue by Fare Class & Route
function createRevenueClassRouteChart(data) {
    if(!data) data = [];
    createOrUpdateChart('revenueClassRouteChart', {
        type: 'bar',
        data: {
            labels: data.map(d => `${d.route} (${d.fare_conditions})`),
            datasets: [{
                label: 'Revenue',
                data: data.map(d => d.total_revenue_by_class),
                backgroundColor: '#9C27B0',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: { y: { beginAtZero: true }, x: { ticks: { maxRotation: 90, minRotation: 90, font: { size: 8 } } } },
            plugins: { legend: { display: false } }
        }
    });
}

// 8. Aircraft Type by Route
function createAircraftRouteChart(data) {
    // Prevent Crash on Empty Data
    if(!data || data.length === 0) {
        return; // Simply return, chart canvas remains empty but no crash
    }

    const counts = data.map(d => d.total_flights_on_route);
    const maxFlights = Math.max(...counts) || 1; // Prevent div by zero

    const uniqueRoutes = [...new Set(data.map(d => d.route))];
    const uniquePlanes = [...new Set(data.map(d => d.aircraft_code))];
    
    const routeCount = uniqueRoutes.length || 1;
    const planeCount = uniquePlanes.length || 1;

    createOrUpdateChart('aircraftRouteChart', {
        type: 'matrix', 
        data: {
            datasets: [{
                label: 'Flight Density',
                data: data.map(d => ({
                    x: d.route,               
                    y: d.aircraft_code,       
                    v: d.total_flights_on_route 
                })),
                backgroundColor: function(context) {
                    const value = context.dataset.data[context.dataIndex].v;
                    const alpha = (value / maxFlights) + 0.15;
                    return `rgba(255, 152, 0, ${alpha})`; 
                },
                borderColor: 'rgba(200, 200, 200, 0.5)',
                borderWidth: 1,
                // Prevents Infinity if width/height is weird
                width: ({chart}) => ((chart.chartArea || {}).width / routeCount) - 1,
                height: ({chart}) => ((chart.chartArea || {}).height / planeCount) - 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { display: false } },
            scales: {
                x: { type: 'category', labels: uniqueRoutes, ticks: { maxRotation: 90, minRotation: 90, font: { size: 9 } }, grid: { display: false } },
                y: { type: 'category', labels: uniquePlanes, offset: true, ticks: { font: { size: 10 } }, grid: { display: false } }
            }
        }
    });
}

// 9. Top 3 Most Visited Destinations
function createDestinationsChart(data) {
    if(!data) data = [];
    createOrUpdateChart('destinationsChart', {
        type: 'doughnut',
        data: {
            labels: data.map(d => `${d.city} (${d.airport_code})`),
            datasets: [{
                data: data.map(d => d.total_arrivals),
                backgroundColor: ['#FF6384', '#36A2EB', '#FFCE56'],
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: { legend: { position: 'bottom' } }
        }
    });
}

// 10. Top 10 Planes with Most Mileage
function createUtilizationChart(data) {
    if(!data) data = [];
    const backgroundColors = ['rgba(255, 99, 132, 0.7)', 'rgba(54, 162, 235, 0.7)', 'rgba(255, 206, 86, 0.7)', 'rgba(75, 192, 192, 0.7)', 'rgba(153, 102, 255, 0.7)'];

    createOrUpdateChart('utilizationChart', {
        type: 'polarArea',
        data: {
            labels: data.map(d => `${d.aircraft_code} (${d.aircraft_model})`),
            datasets: [{
                label: 'Total Mileage',
                data: data.map(d => d.total_mileage),
                backgroundColor: backgroundColors,
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: { padding: 20 },
            scales: { r: { grid: { color: 'rgba(200, 200, 200, 0.3)' }, ticks: { display: false }, pointLabels: { display: false } } },
            plugins: {
                legend: { display: true, position: 'right', labels: { font: { size: 11 }, padding: 20 } },
                tooltip: { callbacks: { label: function(context) { return ' ' + context.label + ': ' + context.parsed.r.toLocaleString() + ' miles'; } } }
            }
        }
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