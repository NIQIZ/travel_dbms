let currentTab = 'flights';
let currentPage = 1;
let currentMode = 'create'; // 'create' or 'edit'
let currentRecordId = null;

// Tab switching
function switchTab(tab) {
    currentTab = tab;
    currentPage = 1;
    
    // Update tab buttons
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    event.target.classList.add('active');
    
    // Update tab content
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    document.getElementById(`${tab}-tab`).classList.add('active');
    
    // Load data
    loadData();
}

// Load data based on current tab
async function loadData(search = '') {
    const endpoint = `/api/${currentTab}`;
    const params = new URLSearchParams({
        page: currentPage,
        per_page: 20,
        search: search
    });
    
    try {
        const response = await fetch(`${endpoint}?${params}`);
        const data = await response.json();
        
        if (currentTab === 'flights') {
            renderFlights(data.flights);
            renderPagination('flights', data);
        } else if (currentTab === 'bookings') {
            renderBookings(data.bookings);
            renderPagination('bookings', data);
        } else if (currentTab === 'aircraft') {
            renderAircraft(data.aircraft);
            renderPagination('aircraft', data);
        }
    } catch (error) {
        console.error('Error loading data:', error);
        showMessage(currentTab, 'Error loading data', 'error');
    }
}

// Render flights table
function renderFlights(flights) {
    const tbody = document.getElementById('flights-tbody');
    
    if (flights.length === 0) {
        tbody.innerHTML = '<tr><td colspan="7" class="loading">No flights found</td></tr>';
        return;
    }
    
    tbody.innerHTML = flights.map(flight => `
        <tr>
            <td>${flight.flight_id}</td>
            <td>${flight.flight_no}</td>
            <td>${flight.departure_airport} → ${flight.arrival_airport}</td>
            <td>${formatDateTime(flight.scheduled_departure)}</td>
            <td><span class="badge badge-${getStatusBadge(flight.status)}">${flight.status}</span></td>
            <td>${flight.aircraft_code}</td>
            <td>
                <div class="action-buttons">
                    <button class="btn btn-warning btn-sm" onclick="editFlight(${flight.flight_id})">✏️ Edit</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteFlight(${flight.flight_id})">🗑️ Delete</button>
                </div>
            </td>
        </tr>
    `).join('');
}

// Render bookings table
function renderBookings(bookings) {
    const tbody = document.getElementById('bookings-tbody');
    
    if (bookings.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="loading">No bookings found</td></tr>';
        return;
    }
    
    tbody.innerHTML = bookings.map(booking => `
        <tr>
            <td>${booking.ticket_no}</td>
            <td>${booking.book_ref}</td>
            <td>${booking.passenger_name}</td>
            <td>${booking.passenger_id}</td>
            <td>${formatContact(booking.contact_data)}</td>
            <td>
                <div class="action-buttons">
                    <button class="btn btn-warning btn-sm" onclick="editBooking('${booking.ticket_no}')">✏️ Edit</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteBooking('${booking.ticket_no}')">🗑️ Delete</button>
                </div>
            </td>
        </tr>
    `).join('');
}

// Render aircraft table
function renderAircraft(aircraft) {
    const tbody = document.getElementById('aircraft-tbody');
    
    if (aircraft.length === 0) {
        tbody.innerHTML = '<tr><td colspan="4" class="loading">No aircraft found</td></tr>';
        return;
    }
    
    tbody.innerHTML = aircraft.map(ac => `
        <tr>
            <td>${ac.aircraft_code}</td>
            <td>${ac.model}</td>
            <td>${ac.range.toLocaleString()}</td>
            <td>
                <div class="action-buttons">
                    <button class="btn btn-warning btn-sm" onclick="editAircraft('${ac.aircraft_code}')">✏️ Edit</button>
                    <button class="btn btn-danger btn-sm" onclick="deleteAircraft('${ac.aircraft_code}')">🗑️ Delete</button>
                </div>
            </td>
        </tr>
    `).join('');
}

// Render pagination
function renderPagination(type, data) {
    const pagination = document.getElementById(`${type}-pagination`);
    
    pagination.innerHTML = `
        <button onclick="changePage(${data.page - 1})" ${data.page === 1 ? 'disabled' : ''}>
            « Previous
        </button>
        <span>Page ${data.page} of ${data.total_pages}</span>
        <button onclick="changePage(${data.page + 1})" ${data.page === data.total_pages ? 'disabled' : ''}>
            Next »
        </button>
    `;
}

function changePage(page) {
    currentPage = page;
    loadData();
}

// Search functions
function searchFlights() {
    const search = document.getElementById('flights-search').value;
    currentPage = 1;
    loadData(search);
}

function searchBookings() {
    const search = document.getElementById('bookings-search').value;
    currentPage = 1;
    loadData(search);
}

function searchAircraft() {
    const search = document.getElementById('aircraft-search').value;
    currentPage = 1;
    loadData(search);
}

// Modal functions
function openCreateModal(type) {
    currentMode = 'create';
    currentRecordId = null;
    document.getElementById('modal-title').textContent = `Create New ${capitalizeFirst(type)}`;
    
    const formHtml = getFormHtml(type, null);
    document.getElementById('modal-form').innerHTML = formHtml;
    document.getElementById('recordModal').classList.add('active');
}

function closeModal() {
    document.getElementById('recordModal').classList.remove('active');
}

// CRUD Operations - Flights
async function editFlight(flightId) {
    try {
        const response = await fetch(`/api/flights/${flightId}`);
        const flight = await response.json();
        
        currentMode = 'edit';
        currentRecordId = flightId;
        document.getElementById('modal-title').textContent = `Edit Flight ${flightId}`;
        
        const formHtml = getFormHtml('flight', flight);
        document.getElementById('modal-form').innerHTML = formHtml;
        document.getElementById('recordModal').classList.add('active');
    } catch (error) {
        showMessage('flights', 'Error loading flight data', 'error');
    }
}

async function deleteFlight(flightId) {
    if (!confirm(`Are you sure you want to delete flight ${flightId}?`)) return;
    
    try {
        const response = await fetch(`/api/flights/${flightId}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showMessage('flights', result.message, 'success');
            loadData();
        } else {
            showMessage('flights', result.error, 'error');
        }
    } catch (error) {
        showMessage('flights', 'Error deleting flight', 'error');
    }
}

// CRUD Operations - Bookings
async function editBooking(ticketNo) {
    try {
        const response = await fetch(`/api/bookings/${ticketNo}`);
        const booking = await response.json();
        
        currentMode = 'edit';
        currentRecordId = ticketNo;
        document.getElementById('modal-title').textContent = `Edit Booking ${ticketNo}`;
        
        const formHtml = getFormHtml('booking', booking);
        document.getElementById('modal-form').innerHTML = formHtml;
        document.getElementById('recordModal').classList.add('active');
    } catch (error) {
        showMessage('bookings', 'Error loading booking data', 'error');
    }
}

async function deleteBooking(ticketNo) {
    if (!confirm(`Are you sure you want to delete booking ${ticketNo}?`)) return;
    
    try {
        const response = await fetch(`/api/bookings/${ticketNo}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showMessage('bookings', result.message, 'success');
            loadData();
        } else {
            showMessage('bookings', result.error, 'error');
        }
    } catch (error) {
        showMessage('bookings', 'Error deleting booking', 'error');
    }
}

// CRUD Operations - Aircraft
async function editAircraft(aircraftCode) {
    try {
        const response = await fetch(`/api/aircraft/${aircraftCode}`);
        const aircraft = await response.json();
        
        currentMode = 'edit';
        currentRecordId = aircraftCode;
        document.getElementById('modal-title').textContent = `Edit Aircraft ${aircraftCode}`;
        
        const formHtml = getFormHtml('aircraft', aircraft);
        document.getElementById('modal-form').innerHTML = formHtml;
        document.getElementById('recordModal').classList.add('active');
    } catch (error) {
        showMessage('aircraft', 'Error loading aircraft data', 'error');
    }
}

async function deleteAircraft(aircraftCode) {
    if (!confirm(`Are you sure you want to delete aircraft ${aircraftCode}?`)) return;
    
    try {
        const response = await fetch(`/api/aircraft/${aircraftCode}`, {
            method: 'DELETE'
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showMessage('aircraft', result.message, 'success');
            loadData();
        } else {
            showMessage('aircraft', result.error, 'error');
        }
    } catch (error) {
        showMessage('aircraft', 'Error deleting aircraft', 'error');
    }
}

// Form submission
async function submitForm(type) {
    const formData = new FormData(document.getElementById('record-form'));
    const data = Object.fromEntries(formData.entries());
    
    // Convert contact_data to JSON if it's a booking
    if (type === 'booking' && data.contact_email) {
        data.contact_data = JSON.stringify({
            email: data.contact_email,
            phone: data.contact_phone || ''
        });
        delete data.contact_email;
        delete data.contact_phone;
    }
    
    let url, method;
    
    if (currentMode === 'create') {
        url = `/api/${type}s`;
        method = 'POST';
    } else {
        if (type === 'flight') {
            url = `/api/flights/${currentRecordId}`;
        } else if (type === 'booking') {
            url = `/api/bookings/${currentRecordId}`;
        } else if (type === 'aircraft') {
            url = `/api/aircraft/${currentRecordId}`;
        }
        method = 'PUT';
    }
    
    try {
        const response = await fetch(url, {
            method: method,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(data)
        });
        
        const result = await response.json();
        
        if (response.ok) {
            showMessage(`${type}s`, result.message, 'success');
            closeModal();
            loadData();
        } else {
            showMessage(`${type}s`, result.error, 'error');
        }
    } catch (error) {
        showMessage(`${type}s`, `Error saving ${type}`, 'error');
    }
}

// Form HTML generators
function getFormHtml(type, data) {
    if (type === 'flight') {
        return getFlightForm(data);
    } else if (type === 'booking') {
        return getBookingForm(data);
    } else if (type === 'aircraft') {
        return getAircraftForm(data);
    }
}

function getFlightForm(flight) {
    return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm('flight');">
            <div class="form-group">
                <label>Flight Number *</label>
                <input type="text" name="flight_no" value="${flight?.flight_no || ''}" required>
            </div>
            <div class="form-group">
                <label>Departure Airport *</label>
                <input type="text" name="departure_airport" value="${flight?.departure_airport || ''}" required maxlength="3">
            </div>
            <div class="form-group">
                <label>Arrival Airport *</label>
                <input type="text" name="arrival_airport" value="${flight?.arrival_airport || ''}" required maxlength="3">
            </div>
            <div class="form-group">
                <label>Scheduled Departure *</label>
                <input type="datetime-local" name="scheduled_departure" value="${formatDateTimeLocal(flight?.scheduled_departure)}" required>
            </div>
            <div class="form-group">
                <label>Scheduled Arrival *</label>
                <input type="datetime-local" name="scheduled_arrival" value="${formatDateTimeLocal(flight?.scheduled_arrival)}" required>
            </div>
            <div class="form-group">
                <label>Status</label>
                <select name="status">
                    <option value="Scheduled" ${flight?.status === 'Scheduled' ? 'selected' : ''}>Scheduled</option>
                    <option value="On Time" ${flight?.status === 'On Time' ? 'selected' : ''}>On Time</option>
                    <option value="Delayed" ${flight?.status === 'Delayed' ? 'selected' : ''}>Delayed</option>
                    <option value="Departed" ${flight?.status === 'Departed' ? 'selected' : ''}>Departed</option>
                    <option value="Arrived" ${flight?.status === 'Arrived' ? 'selected' : ''}>Arrived</option>
                    <option value="Cancelled" ${flight?.status === 'Cancelled' ? 'selected' : ''}>Cancelled</option>
                </select>
            </div>
            <div class="form-group">
                <label>Aircraft Code *</label>
                <input type="text" name="aircraft_code" value="${flight?.aircraft_code || ''}" required maxlength="3">
            </div>
            <div class="form-group">
                <label>Actual Departure</label>
                <input type="datetime-local" name="actual_departure" value="${formatDateTimeLocal(flight?.actual_departure)}">
            </div>
            <div class="form-group">
                <label>Actual Arrival</label>
                <input type="datetime-local" name="actual_arrival" value="${formatDateTimeLocal(flight?.actual_arrival)}">
            </div>
            <div class="form-actions">
                <button type="button" class="btn" onclick="closeModal()">Cancel</button>
                <button type="submit" class="btn btn-primary">${currentMode === 'create' ? 'Create' : 'Update'}</button>
            </div>
        </form>
    `;
}

function getBookingForm(booking) {
    let email = '', phone = '';
    if (booking?.contact_data) {
        try {
            const contact = JSON.parse(booking.contact_data);
            email = contact.email || '';
            phone = contact.phone || '';
        } catch (e) {}
    }
    
    return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm('booking');">
            ${currentMode === 'create' ? `
            <div class="form-group">
                <label>Ticket Number *</label>
                <input type="text" name="ticket_no" value="${booking?.ticket_no || ''}" required>
            </div>
            <div class="form-group">
                <label>Book Reference *</label>
                <input type="text" name="book_ref" value="${booking?.book_ref || ''}" required>
            </div>
            <div class="form-group">
                <label>Passenger ID *</label>
                <input type="text" name="passenger_id" value="${booking?.passenger_id || ''}" required>
            </div>
            ` : ''}
            <div class="form-group">
                <label>Passenger Name *</label>
                <input type="text" name="passenger_name" value="${booking?.passenger_name || ''}" required>
            </div>
            <div class="form-group">
                <label>Contact Email</label>
                <input type="email" name="contact_email" value="${email}">
            </div>
            <div class="form-group">
                <label>Contact Phone</label>
                <input type="text" name="contact_phone" value="${phone}">
            </div>
            <div class="form-actions">
                <button type="button" class="btn" onclick="closeModal()">Cancel</button>
                <button type="submit" class="btn btn-primary">${currentMode === 'create' ? 'Create' : 'Update'}</button>
            </div>
        </form>
    `;
}

function getAircraftForm(aircraft) {
    return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm('aircraft');">
            ${currentMode === 'create' ? `
            <div class="form-group">
                <label>Aircraft Code *</label>
                <input type="text" name="aircraft_code" value="${aircraft?.aircraft_code || ''}" required maxlength="3">
            </div>
            ` : ''}
            <div class="form-group">
                <label>Model *</label>
                <input type="text" name="model" value="${aircraft?.model || ''}" required>
            </div>
            <div class="form-group">
                <label>Range (km) *</label>
                <input type="number" name="range" value="${aircraft?.range || ''}" required min="0">
            </div>
            <div class="form-actions">
                <button type="button" class="btn" onclick="closeModal()">Cancel</button>
                <button type="submit" class="btn btn-primary">${currentMode === 'create' ? 'Create' : 'Update'}</button>
            </div>
        </form>
    `;
}

// Utility functions
function showMessage(section, message, type) {
    const messageDiv = document.getElementById(`${section}-message`);
    messageDiv.innerHTML = `<div class="${type}-message">${message}</div>`;
    setTimeout(() => {
        messageDiv.innerHTML = '';
    }, 5000);
}

function formatDateTime(datetime) {
    if (!datetime) return '-';
    return new Date(datetime).toLocaleString('en-US', {
        year: 'numeric',
        month: 'short',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
    });
}

function formatDateTimeLocal(datetime) {
    if (!datetime) return '';
    const date = new Date(datetime);
    const year = date.getFullYear();
    const month = String(date.getMonth() + 1).padStart(2, '0');
    const day = String(date.getDate()).padStart(2, '0');
    const hours = String(date.getHours()).padStart(2, '0');
    const minutes = String(date.getMinutes()).padStart(2, '0');
    return `${year}-${month}-${day}T${hours}:${minutes}`;
}

function formatContact(contactData) {
    try {
        const contact = JSON.parse(contactData);
        return contact.email || contact.phone || '-';
    } catch {
        return '-';
    }
}

function getStatusBadge(status) {
    if (status === 'Arrived' || status === 'On Time') return 'success';
    if (status === 'Delayed') return 'warning';
    if (status === 'Cancelled') return 'danger';
    return 'info';
}

function capitalizeFirst(str) {
    return str.charAt(0).toUpperCase() + str.slice(1);
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', function() {
    loadData();
});