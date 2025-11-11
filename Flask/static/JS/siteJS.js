
function cancerDesc() {
    document.getElementById("testDesc").innerHTML = "Cancer is a group of diseases involving abnormal cell growth with the potential to invade or spread to other parts of the body. Possible signs and symptoms include a lump, abnormal bleeding, prolonged cough, unexplained weight loss, and a change in bowel movements.";
}

function heartDesc() {
    document.getElementById("testDesc").innerHTML = "Heart disease describes a range of conditions that affect your heart. Diseases under the heart disease umbrella include blood vessel diseases, such as coronary artery disease; heart rhythm problems (arrhythmias); and heart defects you're born with (congenital heart defects), among others.";
}

function strokeDesc() {
    document.getElementById("testDesc").innerHTML = "A stroke is a medical condition in which poor blood flow to the brain results in cell death. There are two main types of stroke: ischemic, due to lack of blood flow, and hemorrhagic, due to bleeding. Both result in parts of the brain not functioning properly.";
}
    
function pneumoniaDesc() {
    document.getElementById("testDesc").innerHTML = "Pneumonia is an inflammatory condition of the lung affecting primarily the small air sacs known as alveoli. Typically symptoms include some combination of productive or dry cough, chest pain, fever, and difficulty breathing.";
}

function openOptions(optionName,elmnt) {
    var i, tabcontent, tablinks;
    tabcontent = document.getElementsByClassName("tabcontent");
    for (i = 0; i < tabcontent.length; i++) {
        tabcontent[i].style.display = "none";
    }
    tablinks = document.getElementsByClassName("tablink");
    for (i = 0; i < tablinks.length; i++) {
        tablinks[i].style.backgroundColor = "";
    }
    document.getElementById(optionName).style.display = "block";
    elmnt.style.backgroundColor = '#ff9900';
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

// Initialize all charts when page loads
document.addEventListener('DOMContentLoaded', function() {
    loadDashboardData();
});

async function loadDashboardData() {
    try {
        const [flightOps, routePerf, passengerDemand, revenue, resources] = await Promise.all([
            fetch('/api/flight-operations').then(r => r.json()),
            fetch('/api/route-performance').then(r => r.json()),
            fetch('/api/passenger-demand').then(r => r.json()),
            fetch('/api/revenue-analysis').then(r => r.json()),
            fetch('/api/resource-planning').then(r => r.json())
        ]);
        
        updateKeyMetrics(flightOps.overview);
        createPunctualChart(flightOps.least_punctual_routes);
        createRoutesChart(routePerf.busiest_routes);
        createOntimeChart(routePerf.ontime_performance);
        createLoadFactorChart(passengerDemand.load_factors);
        createRevenueClassChart(revenue.revenue_by_class);
        createRevenueRouteChart(revenue.revenue_by_route);
        createUtilizationChart(resources.aircraft_utilization);
        createMonthlyRevenueChart(revenue.monthly_revenue);
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
    }
}

function updateKeyMetrics(overview) {
    document.getElementById('totalFlights').textContent = overview.total_flights.toLocaleString();
    const ontimeRate = ((overview.ontime_flights / overview.total_flights) * 100).toFixed(1);
    document.getElementById('ontimeRate').textContent = ontimeRate + '%';
    document.getElementById('avgDelay').textContent = overview.avg_delay_minutes || 0;
    document.getElementById('cancellations').textContent = overview.cancelled_flights.toLocaleString();
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

// Initialize all charts when page loads
document.addEventListener('DOMContentLoaded', function() {
    loadDashboardData();
});

async function loadDashboardData() {
    try {
        // Load all data in parallel
        const [flightOps, routePerf, passengerDemand, revenue, resources] = await Promise.all([
            fetch('/api/flight-operations').then(r => r.json()),
            fetch('/api/route-performance').then(r => r.json()),
            fetch('/api/passenger-demand').then(r => r.json()),
            fetch('/api/revenue-analysis').then(r => r.json()),
            fetch('/api/resource-planning').then(r => r.json())
        ]);
        
        // Update key metrics
        updateKeyMetrics(flightOps.overview);
        
        // Create charts
        createPunctualChart(flightOps.least_punctual_routes);
        createRoutesChart(routePerf.busiest_routes);
        createOntimeChart(routePerf.ontime_performance);
        createLoadFactorChart(passengerDemand.load_factors);
        createRevenueClassChart(revenue.revenue_by_class);
        createRevenueRouteChart(revenue.revenue_by_route);
        createUtilizationChart(resources.aircraft_utilization);
        createMonthlyRevenueChart(revenue.monthly_revenue);
        
    } catch (error) {
        console.error('Error loading dashboard data:', error);
        alert('Error loading dashboard data. Please check the console for details.');
    }
}

function updateKeyMetrics(overview) {
    document.getElementById('totalFlights').textContent = overview.total_flights.toLocaleString();
    const ontimeRate = ((overview.ontime_flights / overview.total_flights) * 100).toFixed(1);
    document.getElementById('ontimeRate').textContent = ontimeRate + '%';
    document.getElementById('avgDelay').textContent = overview.avg_delay_minutes || 0;
    document.getElementById('cancellations').textContent = overview.cancelled_flights.toLocaleString();
}

function createPunctualChart(data) {
    const ctx = document.getElementById('punctualChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'Average Delay (mins)',
                data: data.map(d => d.avg_delay_mins),
                backgroundColor: 'rgba(255, 99, 132, 0.8)', // Example color
                borderColor: 'rgba(255, 99, 132, 1)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            indexAxis: 'y', // Horizontal bar chart, for better label reading
            plugins: {
                legend: {
                    display: false
                },
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

function createRoutesChart(data) {
    const ctx = document.getElementById('routesChart').getContext('2d');
    new Chart(ctx, {
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
                legend: {
                    display: false
                }
            },
            scales: {
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: {
                            size: 10
                        }
                    }
                },
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

function createOntimeChart(data) {
    const ctx = document.getElementById('ontimeChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'On-Time Rate (%)',
                data: data.map(d => d.ontime_rate),
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
                        font: {
                            size: 10
                        }
                    }
                }
            }
        }
    });
}

function createLoadFactorChart(data) {
    const ctx = document.getElementById('loadFactorChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.aircraft_model),
            datasets: [{
                label: 'Load Factor (%)',
                data: data.map(d => d.load_factor),
                backgroundColor: '#FF9800',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    max: 100
                }
            }
        }
    });
}

function createRevenueClassChart(data) {
    const ctx = document.getElementById('revenueClassChart').getContext('2d');
    new Chart(ctx, {
        type: 'pie',
        data: {
            labels: data.map(d => d.fare_conditions),
            datasets: [{
                data: data.map(d => d.total_revenue),
                backgroundColor: ['#2196F3', '#4CAF50', '#FFC107'],
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom'
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            let label = context.label || '';
                            if (label) {
                                label += ': ';
                            }
                            label += new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(context.parsed);
                            return label;
                        }
                    }
                }
            }
        }
    });
}

function createRevenueRouteChart(data) {
    const ctx = document.getElementById('revenueRouteChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.route),
            datasets: [{
                label: 'Total Revenue',
                data: data.map(d => d.total_revenue),
                backgroundColor: '#764ba2',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    ticks: {
                        maxRotation: 45,
                        minRotation: 45,
                        font: {
                            size: 10
                        }
                    }
                },
                y: {
                    beginAtZero: true,
                    ticks: {
                        callback: function(value) {
                            return '$' + value.toLocaleString();
                        }
                    }
                }
            },
            plugins: {
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

function createUtilizationChart(data) {
    const ctx = document.getElementById('utilizationChart').getContext('2d');
    new Chart(ctx, {
        type: 'bar',
        data: {
            labels: data.map(d => d.aircraft_model),
            datasets: [{
                label: 'Flights per Day',
                data: data.map(d => d.flights_per_day),
                backgroundColor: '#9C27B0',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true
                }
            }
        }
    });
}

function createMonthlyRevenueChart(data) {
    const ctx = document.getElementById('monthlyRevenueChart').getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.map(d => d.month),
            datasets: [{
                label: 'Monthly Revenue',
                data: data.map(d => d.revenue),
                borderColor: '#667eea',
                backgroundColor: 'rgba(102, 126, 234, 0.1)',
                tension: 0.4,
                fill: true
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
                            return '$' + value.toLocaleString();
                        }
                    }
                }
            },
            plugins: {
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