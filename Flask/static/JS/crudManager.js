// ==========================================
// STATE & CONFIG
// ==========================================
let currentTab = localStorage.getItem('crud_currentTab') || 'flights';
let crudUseNoSQL = localStorage.getItem('crud_useNoSQL') === 'true';
let currentPage = 1;
let currentMode = 'create'; 
let currentRecordId = null; // Can be object for composite keys
let lastDataSnapshot = "";
let pollingInterval = null;

// ==========================================
// INIT & UI
// ==========================================
document.addEventListener('DOMContentLoaded', function() {
    const toggle = document.getElementById('crudToggle');
    if (toggle) {
        toggle.checked = crudUseNoSQL;
        updateCrudUI();
    }
    // Set active tab
    switchTab(currentTab);
});

function updateCrudUI() {
    const title = document.getElementById('page-title');
    const labelSql = document.getElementById('crud-label-sql');
    const labelNoSql = document.getElementById('crud-label-nosql');
    
    if (crudUseNoSQL) {
        title.textContent = "Database Management (MongoDB)";
        title.style.color = "#2E7D32";
        labelSql.style.color = "#aaa";
        labelNoSql.style.color = "#4CAF50";
        startPolling();
    } else {
        title.textContent = "Database Management (SQLite)";
        title.style.color = "#667eea";
        labelSql.style.color = "#2196F3";
        labelNoSql.style.color = "#aaa";
        stopPolling();
    }
}

function toggleCrudSource() {
    crudUseNoSQL = document.getElementById('crudToggle').checked;
    localStorage.setItem('crud_useNoSQL', crudUseNoSQL);
    updateCrudUI();
    currentPage = 1;
    loadData();
}

function switchTab(tab) {
    currentTab = tab;
    localStorage.setItem('crud_currentTab', tab);
    currentPage = 1;
    lastDataSnapshot = ""; // Reset snapshot
    
    // Update Buttons
    document.querySelectorAll('.tab-btn').forEach(btn => btn.classList.remove('active'));
    const btn = document.querySelector(`button[onclick="switchTab('${tab}')"]`);
    if(btn) btn.classList.add('active');
    
    // Update Content
    document.querySelectorAll('.tab-content').forEach(content => content.classList.remove('active'));
    const content = document.getElementById(`${tab}-tab`);
    if(content) content.classList.add('active');
    
    loadData();
}

// ==========================================
// DATA LOADING
// ==========================================
async function loadData() {
    const prefix = crudUseNoSQL ? '/api/nosql' : '/api';
    const endpoint = `${prefix}/${currentTab}`;
    
    // Get search inputs specific to the tab
    const searchInput = document.getElementById(`${currentTab}-search`);
    const colInput = document.getElementById(`${currentTab}-search-col`);
    
    const params = new URLSearchParams({
        page: currentPage,
        per_page: 20,
        search: searchInput ? searchInput.value : '',
        column: colInput ? colInput.value : '' 
    });

    try {
        const response = await fetch(`${endpoint}?${params}`);
        const data = await response.json();
        
        // Handle rendering based on tab
        const list = data[currentTab] || []; // Assumes API returns key matching tab name
        
        if (currentTab === 'flights') renderFlights(list);
        else if (currentTab === 'aircraft') renderAircraft(list);
        else if (currentTab === 'airports') renderAirports(list);
        else if (currentTab === 'bookings') renderBookings(list);
        else if (currentTab === 'tickets') renderTickets(list);
        else if (currentTab === 'ticket_flights') renderTicketFlights(list);
        else if (currentTab === 'seats') renderSeats(list);
        else if (currentTab === 'boarding_passes') renderBoardingPasses(list);
        
        renderPagination(data);
        
        if (crudUseNoSQL) lastDataSnapshot = JSON.stringify(list);

    } catch (error) {
        console.error(error);
        showMessage('Error loading data');
    }
}

function searchData(tab) {
    currentPage = 1;
    loadData();
}

// ==========================================
// RENDER FUNCTIONS (Columns match Relational Schema)
// ==========================================

function renderFlights(list) {
    const tbody = document.getElementById('flights-tbody');
    // Updated to show Dep/Arr separately, plus Actual times and Scheduled Arrival
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.flight_id}</td>
            <td>${d.flight_no}</td>
            <td>${d.departure_airport}</td>
            <td>${d.arrival_airport}</td>
            <td>${formatDate(d.scheduled_departure)}</td>
            <td>${formatDate(d.scheduled_arrival)}</td>
            <td><span class="badge badge-info">${d.status}</span></td>
            <td>${d.aircraft_code}</td>
            <td class="small text-muted">${formatDate(d.actual_departure)}</td>
            <td class="small text-muted">${formatDate(d.actual_arrival)}</td>
            <td>${renderActions(d.flight_id)}</td>
        </tr>`).join('') || noData(11);
}

function renderAircraft(list) {
    const tbody = document.getElementById('aircraft-tbody');
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.aircraft_code}</td><td>${d.model}</td><td>${d.range}</td>
            <td>${renderActions(d.aircraft_code)}</td>
        </tr>`).join('') || noData(4);
}

function renderAirports(list) {
    const tbody = document.getElementById('airports-tbody');
    // Added Coordinates column
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.airport_code}</td>
            <td>${d.airport_name}</td>
            <td>${d.city}</td>
            <td class="small">${d.coordinates || ''}</td>
            <td>${d.timezone}</td>
            <td>${renderActions(d.airport_code)}</td>
        </tr>`).join('') || noData(6);
}

function renderBookings(list) {
    const tbody = document.getElementById('bookings-tbody');
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.book_ref}</td><td>${formatDate(d.book_date)}</td><td>${d.total_amount}</td>
            <td>${renderActions(d.book_ref)}</td>
        </tr>`).join('') || noData(4);
}

function renderTickets(list) {
    const tbody = document.getElementById('tickets-tbody');
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.ticket_no}</td><td>${d.book_ref}</td><td>${d.passenger_id}</td>
            <td>${renderActions(d.ticket_no)}</td>
        </tr>`).join('') || noData(4);
}

function renderTicketFlights(list) {
    const tbody = document.getElementById('ticket_flights-tbody');
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.ticket_no}</td><td>${d.flight_id}</td><td>${d.fare_conditions}</td><td>${d.amount}</td>
            <td>${renderActions(d.ticket_no + '|' + d.flight_id)}</td> </tr>`).join('') || noData(5);
}

function renderSeats(list) {
    const tbody = document.getElementById('seats-tbody');
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.aircraft_code}</td><td>${d.seat_no}</td><td>${d.fare_conditions}</td>
            <td>${renderActions(d.aircraft_code + '|' + d.seat_no)}</td>
        </tr>`).join('') || noData(4);
}

function renderBoardingPasses(list) {
    const tbody = document.getElementById('boarding_passes-tbody');
    tbody.innerHTML = list.map(d => `
        <tr>
            <td>${d.ticket_no}</td><td>${d.flight_id}</td><td>${d.boarding_no}</td><td>${d.seat_no}</td>
            <td>${renderActions(d.ticket_no + '|' + d.flight_id)}</td>
        </tr>`).join('') || noData(5);
}

// Helper for actions (Edit/Delete)
function renderActions(id) {
    return `<div class="action-buttons">
        <button class="btn btn-warning btn-sm" onclick="openEditModal('${id}')">✏️</button>
        <button class="btn btn-danger btn-sm" onclick="deleteRecord('${id}')">🗑️</button>
    </div>`;
}

function noData(cols) { return `<tr><td colspan="${cols}" class="loading">No data found</td></tr>`; }

function renderPagination(data) {
    const div = document.getElementById(`${currentTab}-pagination`);
    if(div) div.innerHTML = `
        <button onclick="currentPage--; loadData()" ${data.page <= 1 ? 'disabled' : ''}>« Prev</button>
        <span>Page ${data.page} of ${data.total_pages}</span>
        <button onclick="currentPage++; loadData()" ${data.page >= data.total_pages ? 'disabled' : ''}>Next »</button>
    `;
}

// ==========================================
// MODAL & FORMS
// ==========================================
function openCreateModal() {
    currentMode = 'create';
    currentRecordId = null;
    showModal('Create Record', getFormHtml({}));
}

async function openEditModal(id) {
    currentMode = 'edit';
    currentRecordId = id;
    
    // For composite keys (seat, ticket_flight), we split by '|'
    const prefix = crudUseNoSQL ? '/api/nosql' : '/api';
    
    // Simple fetch logic
    let url = `${prefix}/${currentTab}/${id}`;
    
    try {
        const res = await fetch(url);
        if(!res.ok) throw new Error("Failed");
        const data = await res.json();
        showModal('Edit Record', getFormHtml(data));
    } catch(e) {
        alert("Error fetching record details");
    }
}

function showModal(title, html) {
    document.getElementById('modal-title').textContent = title;
    document.getElementById('modal-form').innerHTML = html;
    document.getElementById('recordModal').classList.add('active');
}

function closeModal() { document.getElementById('recordModal').classList.remove('active'); }

// GENERIC SUBMIT
async function submitForm() {
    const formData = new FormData(document.getElementById('record-form'));
    const data = Object.fromEntries(formData.entries());
    const prefix = crudUseNoSQL ? '/api/nosql' : '/api';
    
    let url = `${prefix}/${currentTab}`;
    let method = 'POST';
    
    if (currentMode === 'edit') {
        url += `/${currentRecordId}`;
        method = 'PUT';
    }
    
    try {
        const res = await fetch(url, {
            method: method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data)
        });
        
        if (res.ok) {
            closeModal();
            loadData();
            showMessage("Saved successfully", "success");
        } else if (res.status === 409) {
            alert("Conflict: Record modified by another user.");
            loadData();
        } else {
            alert("Error saving record");
        }
    } catch(e) { console.error(e); }
}

async function deleteRecord(id) {
    if(!confirm("Are you sure?")) return;
    const prefix = crudUseNoSQL ? '/api/nosql' : '/api';
    try {
        const res = await fetch(`${prefix}/${currentTab}/${id}`, { method: 'DELETE' });
        if(res.ok) { loadData(); showMessage("Deleted", "success"); }
        else alert("Failed to delete");
    } catch(e) { console.error(e); }
}

// FORM GENERATORS (Matching Schema)
function getFormHtml(d) {
    const v = (val) => val !== undefined && val !== null ? val : '';
    const ver = d.version ? `<input type="hidden" name="version" value="${d.version}">` : '';
    // Primary keys should be read-only in 'edit' mode to prevent breaking the ID reference
    const ro = currentMode === 'edit' ? 'readonly' : ''; 

    // --- 1. FLIGHTS ---
    if (currentTab === 'flights') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>ID (Auto)</label><input name="flight_id" value="${v(d.flight_id)}" readonly placeholder="Auto-generated"></div>
            <div class="row">
                <div class="col"><div class="form-group"><label>Flight No</label><input name="flight_no" value="${v(d.flight_no)}" required></div></div>
                <div class="col"><div class="form-group"><label>Aircraft</label><input name="aircraft_code" value="${v(d.aircraft_code)}" maxlength="3" required></div></div>
            </div>
            <div class="row">
                <div class="col"><div class="form-group"><label>Dep Airport</label><input name="departure_airport" value="${v(d.departure_airport)}" maxlength="3" required></div></div>
                <div class="col"><div class="form-group"><label>Arr Airport</label><input name="arrival_airport" value="${v(d.arrival_airport)}" maxlength="3" required></div></div>
            </div>
            <div class="row">
                <div class="col"><div class="form-group"><label>Sched Dep</label><input type="datetime-local" name="scheduled_departure" value="${v(d.scheduled_departure)}" required></div></div>
                <div class="col"><div class="form-group"><label>Sched Arr</label><input type="datetime-local" name="scheduled_arrival" value="${v(d.scheduled_arrival)}" required></div></div>
            </div>
             <div class="row">
                <div class="col"><div class="form-group"><label>Act Dep</label><input type="datetime-local" name="actual_departure" value="${v(d.actual_departure)}"></div></div>
                <div class="col"><div class="form-group"><label>Act Arr</label><input type="datetime-local" name="actual_arrival" value="${v(d.actual_arrival)}"></div></div>
            </div>
            <div class="form-group"><label>Status</label>
                <select name="status">
                    <option value="Scheduled" ${d.status==='Scheduled'?'selected':''}>Scheduled</option>
                    <option value="On Time" ${d.status==='On Time'?'selected':''}>On Time</option>
                    <option value="Delayed" ${d.status==='Delayed'?'selected':''}>Delayed</option>
                    <option value="Departed" ${d.status==='Departed'?'selected':''}>Departed</option>
                    <option value="Arrived" ${d.status==='Arrived'?'selected':''}>Arrived</option>
                    <option value="Cancelled" ${d.status==='Cancelled'?'selected':''}>Cancelled</option>
                </select>
            </div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    // --- 2. AIRPORTS ---
    if (currentTab === 'airports') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>Code</label><input name="airport_code" value="${v(d.airport_code)}" ${ro} maxlength="3" required></div>
            <div class="form-group"><label>Name</label><input name="airport_name" value="${v(d.airport_name)}" required></div>
            <div class="form-group"><label>City</label><input name="city" value="${v(d.city)}" required></div>
            <div class="form-group"><label>Coordinates</label><input name="coordinates" value="${v(d.coordinates)}" placeholder="(lon, lat)"></div>
            <div class="form-group"><label>Timezone</label><input name="timezone" value="${v(d.timezone)}"></div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    // --- 3. AIRCRAFT ---
    if (currentTab === 'aircraft') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>Code</label><input name="aircraft_code" value="${v(d.aircraft_code)}" ${ro} maxlength="3" required></div>
            <div class="form-group"><label>Model</label><input name="model" value="${v(d.model)}" required></div>
            <div class="form-group"><label>Range</label><input type="number" name="range" value="${v(d.range)}" required></div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    // --- 4. BOOKINGS ---
    if (currentTab === 'bookings') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>Book Ref</label><input name="book_ref" value="${v(d.book_ref)}" ${ro} maxlength="6" required></div>
            <div class="form-group"><label>Date</label><input type="datetime-local" name="book_date" value="${v(d.book_date)}" required></div>
            <div class="form-group"><label>Amount</label><input type="number" name="total_amount" value="${v(d.total_amount)}" required></div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    // --- 5. TICKETS ---
    if (currentTab === 'tickets') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>Ticket No</label><input name="ticket_no" value="${v(d.ticket_no)}" ${ro} required></div>
            <div class="form-group"><label>Book Ref</label><input name="book_ref" value="${v(d.book_ref)}" required></div>
            <div class="form-group"><label>Passenger ID</label><input name="passenger_id" value="${v(d.passenger_id)}" required></div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    // --- 6. TICKET FLIGHTS ---
    if (currentTab === 'ticket_flights') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>Ticket No</label><input name="ticket_no" value="${v(d.ticket_no)}" ${ro} required></div>
            <div class="form-group"><label>Flight ID</label><input name="flight_id" value="${v(d.flight_id)}" ${ro} required></div>
            <div class="form-group"><label>Fare Conditions</label>
                <select name="fare_conditions">
                    <option value="Economy" ${d.fare_conditions==='Economy'?'selected':''}>Economy</option>
                    <option value="Business" ${d.fare_conditions==='Business'?'selected':''}>Business</option>
                    <option value="Comfort" ${d.fare_conditions==='Comfort'?'selected':''}>Comfort</option>
                </select>
            </div>
            <div class="form-group"><label>Amount</label><input type="number" name="amount" value="${v(d.amount)}" required></div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    // --- 7. SEATS ---
    if (currentTab === 'seats') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>Aircraft Code</label><input name="aircraft_code" value="${v(d.aircraft_code)}" ${ro} required></div>
            <div class="form-group"><label>Seat No</label><input name="seat_no" value="${v(d.seat_no)}" ${ro} required></div>
            <div class="form-group"><label>Fare Conditions</label>
                <select name="fare_conditions">
                    <option value="Economy" ${d.fare_conditions==='Economy'?'selected':''}>Economy</option>
                    <option value="Business" ${d.fare_conditions==='Business'?'selected':''}>Business</option>
                    <option value="Comfort" ${d.fare_conditions==='Comfort'?'selected':''}>Comfort</option>
                </select>
            </div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    // --- 8. BOARDING PASSES  ---
    if (currentTab === 'boarding_passes') return `
        <form id="record-form" onsubmit="event.preventDefault(); submitForm()">
            ${ver}
            <div class="form-group"><label>Ticket No</label><input name="ticket_no" value="${v(d.ticket_no)}" ${ro} required></div>
            <div class="form-group"><label>Flight ID</label><input name="flight_id" value="${v(d.flight_id)}" ${ro} required></div>
            <div class="form-group"><label>Boarding No</label><input type="number" name="boarding_no" value="${v(d.boarding_no)}" required></div>
            <div class="form-group"><label>Seat No</label><input name="seat_no" value="${v(d.seat_no)}" required></div>
            <div class="form-actions"><button class="btn btn-primary">Save</button></div>
        </form>`;

    return `<div style="padding:20px;">Form not implemented for this table yet.</div>`;
}

// UTILS
function showMessage(msg) { const d=document.getElementById(`${currentTab}-message`); if(d){d.textContent=msg; setTimeout(()=>d.textContent='', 3000);} }
function formatDate(d) { if(!d) return ''; return new Date(d).toLocaleString(); }
function startPolling() { pollingInterval = setInterval(loadData, 5000); }
function stopPolling() { clearInterval(pollingInterval); }