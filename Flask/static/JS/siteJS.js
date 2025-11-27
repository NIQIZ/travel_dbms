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
    // Destroy existing chart if it exists
    if (chartInstances[canvasId]) {
        chartInstances[canvasId].destroy();
    }
    
    // Create new chart
    const ctx = document.getElementById(canvasId);
    if (ctx) {
        chartInstances[canvasId] = new Chart(ctx.getContext('2d'), config);
    }
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

// Initialize all charts when page loads
document.addEventListener('DOMContentLoaded', function() {
    loadDashboardData();
});

async function loadDashboardData() {
    try {
        // Determine API prefix based on toggle
        const prefix = useNoSQL ? '/api/nosql' : '/api';
        
        // Load all data in parallel
        const [flightOps, routePerf, passengerDemand, revenue, resources] = await Promise.all([
            fetch(`${prefix}/flight-operations`).then(r => {
                if (!r.ok) throw new Error(`Flight operations failed: ${r.statusText}`);
                return r.json();
            }),
            fetch(`${prefix}/route-performance`).then(r => {
                if (!r.ok) throw new Error(`Route performance failed: ${r.statusText}`);
                return r.json();
            }),
            fetch(`${prefix}/passenger-demand`).then(r => {
                if (!r.ok) throw new Error(`Passenger demand failed: ${r.statusText}`);
                return r.json();
            }),
            fetch(`${prefix}/revenue-analysis`).then(r => {
                if (!r.ok) throw new Error(`Revenue analysis failed: ${r.statusText}`);
                return r.json();
            }),
            fetch(`${prefix}/resource-planning`).then(r => {
                if (!r.ok) throw new Error(`Resource planning failed: ${r.statusText}`);
                return r.json();
            })
        ]);
        
        // Update key metrics
        updateKeyMetrics(flightOps.overview);
        
        // Create all charts/visualizations
        createPunctualChart(flightOps.least_punctual_routes);
        createRoutesChart(routePerf.busiest_routes);
        createOccupancyChart(passengerDemand.top_occupancy_routes);
        createMarketShareChart(passengerDemand.busiest_routes_market_share);
        createLeastBusyChart(passengerDemand.least_busy_routes);
        createProfitableRoutesChart(revenue.top_revenue_routes, revenue.least_revenue_routes);
        createRevenueClassChart(revenue.revenue_by_class);
        createRevenueClassRouteChart(revenue.revenue_by_class_route);
        createAircraftRouteChart(resources.aircraft_by_route);
        createDestinationsChart(resources.top_destinations);
        createUtilizationChart(resources.aircraft_utilization);
        
        // Initialize aircraft filter and table
        initializeAircraftFilter(resources.aircraft_list);
        createSU9RoutesTable(resources.su9_routes);
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        // We do not alert here to avoid spamming user on switch, just log
    }
}

function updateKeyMetrics(overview) {
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
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Average Delay (mins)'
                    }
                }
            }
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
            plugins: {
                legend: { display: false }
            },
            scales: {
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: { size: 10 }
                    }
                },
                y: {
                    beginAtZero: true
                }
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
                y: {
                    beginAtZero: true,
                    max: 100
                },
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: { size: 10 }
                    }
                }
            }
        }
    });
}

// 3. Busiest Routes by Market Share % - TRUE GRID HEATMAP
function createMarketShareChart(data) {
    if(!data) data = [];
    // Sort data by market share
    const sortedData = [...data].sort((a, b) => b.market_share_percent - a.market_share_percent);
    
    // Create matrix data points
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
                    const height = ((chart.chartArea || {}).height / (sortedData.length || 1)) - 4;
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
                        title() {
                            return '';
                        },
                        label(context) {
                            const v = context.dataset.data[context.dataIndex];
                            return [
                                `Route: ${v.route}`,
                                `Market Share: ${v.v}%`,
                                `Tickets Sold: ${v.tickets ? v.tickets.toLocaleString() : 0}`
                            ];
                        }
                    }
                }
            },
            scales: {
                x: {
                    display: false,
                    min: -0.5,
                    max: 0.5
                },
                y: {
                    type: 'category',
                    labels: sortedData.map(d => d.route),
                    offset: true,
                    ticks: {
                        font: { 
                            size: 11,
                            weight: 'bold'
                        },
                        color: '#333'
                    },
                    grid: {
                        display: false
                    }
                }
            },
            layout: {
                padding: {
                    left: 10,
                    right: 20
                }
            }
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
            scales: {
                y: { beginAtZero: true },
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: { size: 10 }
                    }
                }
            },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return `Market Share: ${context.parsed.y}% (${data[context.dataIndex].total_tickets_sold} tickets)`;
                        }
                    }
                }
            }
        }
    });
}

// 6. Top 3 Most & Least Profitable Routes - SPLIT INTO TWO CHARTS
function createProfitableRoutesChart(topRoutes, leastRoutes) {
    if(!topRoutes) topRoutes = [];
    if(!leastRoutes) leastRoutes = [];
    
    // Top 3 Most Profitable
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
            scales: {
                x: { 
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return '$' + (value / 1000000).toFixed(1) + 'M';
                        }
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Most Profitable Routes',
                    font: { size: 14, weight: 'bold' },
                    color: '#4CAF50'
                },
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return 'Revenue: $' + context.parsed.x.toLocaleString();
                        }
                    }
                }
            }
        }
    });
    
    // Top 3 Least Profitable
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
            scales: {
                x: { 
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return '$' + (value / 1000).toFixed(0) + 'K';
                        }
                    }
                }
            },
            plugins: {
                title: {
                    display: true,
                    text: 'Least Profitable Routes',
                    font: { size: 14, weight: 'bold' },
                    color: '#f44336'
                },
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return 'Revenue: $' + context.parsed.x.toLocaleString();
                        }
                    }
                }
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
            plugins: {
                legend: { position: 'bottom' },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.label || '';
                            if (label) {
                                label += ': ';
                            }
                            label += '$' + context.parsed.toLocaleString();
                            return label;
                        }
                    }
                }
            }
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
            scales: {
                y: { beginAtZero: true },
                x: {
                    ticks: {
                        maxRotation: 90,
                        minRotation: 90,
                        font: { size: 8 }
                    }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return 'Revenue: $' + context.parsed.y.toLocaleString();
                        }
                    }
                }
            }
        }
    });
}

// 8. Aircraft Type by Route
function createAircraftRouteChart(data) {
    if(!data) data = [];
    createOrUpdateChart('aircraftRouteChart', {
        type: 'bar',
        data: {
            labels: data.map(d => `${d.route} (${d.aircraft_code})`),
            datasets: [{
                label: 'Total Flights',
                data: data.map(d => d.total_flights_on_route),
                backgroundColor: '#FF9800',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: { beginAtZero: true },
                x: {
                    ticks: {
                        maxRotation: 90,
                        minRotation: 90,
                        font: { size: 8 }
                    }
                }
            },
            plugins: {
                legend: { display: false }
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
            plugins: {
                legend: { position: 'bottom' },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.label + ': ' + context.parsed + ' arrivals';
                        }
                    }
                }
            }
        }
    });
}

// 10. Top 10 Planes with Most Mileage
function createUtilizationChart(data) {
    if(!data) data = [];
    createOrUpdateChart('utilizationChart', {
        type: 'bar',
        data: {
            labels: data.map(d => `${d.aircraft_code} (${d.aircraft_model})`),
            datasets: [{
                label: 'Total Mileage',
                data: data.map(d => d.total_mileage),
                backgroundColor: '#9C27B0',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return value.toLocaleString() + ' miles';
                        }
                    }
                },
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: { size: 9 }
                    }
                }
            },
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return 'Total Mileage: ' + context.parsed.y.toLocaleString() + ' miles';
                        }
                    }
                }
            }
        }
    });
}

// Initialize aircraft filter dropdown
function initializeAircraftFilter(aircraftList) {
    const filterSelect = document.getElementById('aircraftFilter');
    
    // Clear loading option
    filterSelect.innerHTML = '';
    
    if(!aircraftList || aircraftList.length === 0) {
        const option = document.createElement('option');
        option.text = "No Aircraft Data";
        filterSelect.appendChild(option);
        return;
    }
    
    // Add options
    aircraftList.forEach(aircraft => {
        const option = document.createElement('option');
        option.value = aircraft.aircraft_code;
        option.textContent = `${aircraft.aircraft_code} - ${aircraft.aircraft_model}`;
        filterSelect.appendChild(option);
    });
    
    // Set default if exists
    if(aircraftList.length > 0) {
        filterSelect.value = aircraftList[0].aircraft_code;
    }
    
    // Add change event listener
    filterSelect.addEventListener('change', async function() {
        const selectedAircraft = this.value;
        await loadAircraftRoutes(selectedAircraft);
    });
}

// Load routes for selected aircraft
async function loadAircraftRoutes(aircraftCode) {
    if(!aircraftCode) return;
    const tbody = document.getElementById('su9RoutesBody');
    tbody.innerHTML = '<tr><td colspan="4" class="text-center">Loading...</td></tr>';
    
    try {
        const prefix = useNoSQL ? '/api/nosql' : '/api';
        // Note: For simplicity, assuming endpoint handles aircraft routes logic or we disable it for NoSQL if complex
        // Here we attempt to fetch it
        const response = await fetch(`${prefix}/aircraft-routes/${aircraftCode}`);
        if (!response.ok) throw new Error(`Failed to load routes: ${response.statusText}`);
        
        const data = await response.json();
        createSU9RoutesTable(data.routes);
        
    } catch (error) {
        console.error('Error loading aircraft routes:', error);
        tbody.innerHTML = '<tr><td colspan="4" class="text-center text-danger">Error loading data</td></tr>';
    }
}

// 11. Routes Taken by Aircraft (Table)
function createSU9RoutesTable(data) {
    const tbody = document.getElementById('su9RoutesBody');
    tbody.innerHTML = '';
    
    if (!data || data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="text-center">No data available for this aircraft</td></tr>';
        return;
    }
    
    data.forEach(row => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td>${row.flight_id}</td>
            <td>${row.route}</td>
            <td>${row.scheduled_departure_time}</td>
            <td><span class="badge bg-${getStatusColor(row.status)}">${row.status}</span></td>
        `;
        tbody.appendChild(tr);
    });
}

function getStatusColor(status) {
    if (status === 'Arrived' || status === 'On Time') return 'success';
    if (status.includes('Delayed')) return 'warning';
    if (status.includes('Cancel')) return 'danger';
    return 'secondary';
}