async function apiRequest(path, options = {}) {
    const response = await fetch(path, options);
    let payload = {};

    try {
        payload = await response.json();
    } catch (_) {}

    if (!response.ok || payload.status !== 'success') {
        throw new Error(payload?.error?.message || `Request failed: ${response.status}`);
    }

    return payload;
}

async function apiGet(path) {
    return apiRequest(path);
}

async function apiPost(path, body) {
    return apiRequest(path, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
    });
}

const csvState = {
    columns: [],
    rows: [],
    page: 1,
    pageSize: 100,
    primaryFilters: [],
    filterDefinitions: {},
    dataset: {
        row_count: 0,
        column_count: 0,
        last_modified: null,
    },
    pagination: {
        page: 1,
        page_size: 100,
        total_rows: 0,
        total_pages: 1,
        has_previous: false,
        has_next: false,
        start_row: 0,
        end_row: 0,
    },
};

const ALL_FILTER_OPTION_VALUE = '__ALL_FILTER_OPTION__';
let searchDebounceHandle = null;

function showProgrammePlansError(error, action = 'load the Programme Plans dataset') {
    console.error(error);
    const message = `Failed to ${action}: ${error.message}`;
    if (window.appUI?.notify) {
        window.appUI.notify(message, {
            title: 'Programme Plans',
            tone: 'error',
            timeoutMs: 7000,
        });
        return;
    }
    alert(message);
}

function captureUiSelections() {
    const columnFilters = {};
    document.querySelectorAll('.column-filter').forEach(selectEl => {
        const value = selectEl.value;
        const columnName = selectEl.dataset.columnName;
        if (!(columnName in columnFilters)) {
            columnFilters[columnName] = value;
        }
    });

    return {
        query: document.getElementById('csvSearchInput')?.value || '',
        pageSize: document.getElementById('csvPageSize')?.value || '100',
        filterFinder: document.getElementById('filterSearchInput')?.value || '',
        columnFilters,
    };
}

function restoreUiSelections(savedState) {
    if (!savedState) {
        return;
    }

    document.getElementById('csvSearchInput').value = savedState.query || '';
    document.getElementById('csvPageSize').value = savedState.pageSize || String(csvState.pageSize);

    Object.entries(savedState.columnFilters || {}).forEach(([columnName, value]) => {
        setColumnFilterValue(columnName, value);
    });

    document.getElementById('filterSearchInput').value = savedState.filterFinder || '';
    filterAdvancedFilters();
}

function getFilterDefinition(columnName) {
    return csvState.filterDefinitions[columnName] || { options: [], unique_count: 0 };
}

function createFilterCard(columnName, mode) {
    const wrapper = document.createElement('div');
    const isPrimary = mode === 'primary';
    wrapper.className = isPrimary ? 'col-12 col-md-6 col-xl-4' : 'col-12';

    const card = document.createElement('div');
    card.className = isPrimary ? 'programme-quick-filter h-100' : 'programme-advanced-filter h-100';
    card.dataset.filterCard = 'true';
    card.dataset.columnName = String(columnName || '').toLowerCase();

    const label = document.createElement('label');
    label.className = 'form-label mb-1 fw-semibold';
    label.setAttribute('for', `${mode}ColumnFilter-${columnName}`);
    label.textContent = columnName;

    const select = document.createElement('select');
    select.id = `${mode}ColumnFilter-${columnName}`;
    select.className = `form-select ${isPrimary ? '' : 'form-select-sm '}column-filter`;
    select.dataset.columnName = String(columnName);
    select.dataset.surface = mode;

    const allOption = document.createElement('option');
    allOption.value = ALL_FILTER_OPTION_VALUE;
    allOption.textContent = 'All values';
    select.appendChild(allOption);

    const definition = getFilterDefinition(columnName);
    (definition.options || []).forEach(value => {
        const option = document.createElement('option');
        option.value = value;
        option.textContent = value === '' ? '(blank)' : value;
        select.appendChild(option);
    });

    const hint = document.createElement('div');
    hint.className = 'form-text mt-2';
    hint.textContent = `${Number(definition.unique_count || 0).toLocaleString()} unique values`;

    card.appendChild(label);
    card.appendChild(select);
    card.appendChild(hint);
    wrapper.appendChild(card);

    return wrapper;
}

function renderPrimaryFilters() {
    const section = document.getElementById('primaryFiltersSection');
    const container = document.getElementById('primaryFiltersContainer');
    container.innerHTML = '';

    csvState.primaryFilters.forEach(columnName => {
        container.appendChild(createFilterCard(columnName, 'primary'));
    });

    section.hidden = csvState.primaryFilters.length === 0;
}

function renderAdvancedFilters() {
    const container = document.getElementById('advancedFiltersContainer');
    container.innerHTML = '';

    csvState.columns.forEach(columnName => {
        container.appendChild(createFilterCard(columnName, 'advanced'));
    });
}

function renderCsvHeader() {
    const header = document.getElementById('csvHeaderRow');
    header.innerHTML = '';

    const rowNumHeader = document.createElement('th');
    rowNumHeader.className = 'text-end';
    rowNumHeader.textContent = '#';
    rowNumHeader.style.color = '#0f172a';
    rowNumHeader.style.background = '#f8fafc';
    rowNumHeader.style.borderColor = 'rgba(15, 23, 42, 0.08)';
    header.appendChild(rowNumHeader);

    csvState.columns.forEach(columnName => {
        const th = document.createElement('th');
        th.textContent = columnName;
        th.style.color = '#0f172a';
        th.style.background = '#f8fafc';
        th.style.borderColor = 'rgba(15, 23, 42, 0.08)';
        header.appendChild(th);
    });
}

function getActiveColumnFilters() {
    const filters = new Map();
    document.querySelectorAll('.column-filter').forEach(selectEl => {
        const selectedValue = selectEl.value;
        const columnName = selectEl.dataset.columnName;
        if (selectedValue !== ALL_FILTER_OPTION_VALUE && !filters.has(columnName)) {
            filters.set(columnName, selectedValue);
        }
    });
    return filters;
}

function setColumnFilterValue(columnName, value) {
    document.querySelectorAll('.column-filter').forEach(selectEl => {
        if (selectEl.dataset.columnName !== String(columnName)) {
            return;
        }

        const hasOption = Array.from(selectEl.options).some(option => option.value === value);
        selectEl.value = hasOption ? value : ALL_FILTER_OPTION_VALUE;
    });
}

function renderCsvPage() {
    const body = document.getElementById('csvBody');
    const pageInfo = document.getElementById('csvPageInfo');
    body.innerHTML = '';

    const rows = csvState.rows || [];
    const pagination = csvState.pagination || {};

    if (rows.length === 0) {
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = csvState.columns.length + 1;
        td.className = 'text-center py-5 text-muted';
        td.innerHTML = '<div class="fw-semibold mb-1">No rows match the current filters</div><div class="small">Clear a chip or broaden the search to see more rows.</div>';
        tr.appendChild(td);
        body.appendChild(tr);
    } else {
        rows.forEach((row, index) => {
            const tr = document.createElement('tr');

            const rowNumCell = document.createElement('td');
            rowNumCell.className = 'text-end text-muted';
            rowNumCell.textContent = String((pagination.start_row || 1) + index);
            tr.appendChild(rowNumCell);

            row.forEach(value => {
                const td = document.createElement('td');
                td.textContent = value ?? '';
                tr.appendChild(td);
            });

            body.appendChild(tr);
        });
    }

    if (!pagination.total_rows) {
        pageInfo.textContent = '0 matches';
    } else {
        pageInfo.textContent = `Showing ${pagination.start_row}-${pagination.end_row} of ${Number(pagination.total_rows).toLocaleString()} rows`;
    }

    document.getElementById('csvPrevBtn').disabled = !pagination.has_previous;
    document.getElementById('csvNextBtn').disabled = !pagination.has_next;
}

function renderActiveFilterChips(activeFilters, query) {
    const container = document.getElementById('activeFilterChips');
    container.innerHTML = '';

    if (query) {
        const chip = document.createElement('div');
        chip.className = 'programme-chip';
        chip.dataset.chipType = 'query';

        const label = document.createElement('span');
        label.textContent = `Search: ${query}`;

        const button = document.createElement('button');
        button.type = 'button';
        button.setAttribute('aria-label', 'Remove search filter');
        button.innerHTML = '&times;';

        chip.appendChild(label);
        chip.appendChild(button);
        container.appendChild(chip);
    }

    Array.from(activeFilters.entries())
        .sort((a, b) => a[0].localeCompare(b[0]))
        .forEach(([columnName, value]) => {
            const chip = document.createElement('div');
            chip.className = 'programme-chip';
            chip.dataset.chipType = 'column';
            chip.dataset.columnName = columnName;

            const label = document.createElement('span');
            label.textContent = `${columnName}: ${value === '' ? '(blank)' : value}`;

            const button = document.createElement('button');
            button.type = 'button';
            button.setAttribute('aria-label', `Remove filter for ${columnName}`);
            button.innerHTML = '&times;';

            chip.appendChild(label);
            chip.appendChild(button);
            container.appendChild(chip);
        });

    if (!query && activeFilters.size === 0) {
        const empty = document.createElement('div');
        empty.className = 'programme-chip-empty';
        empty.textContent = 'No filters applied.';
        container.appendChild(empty);
    }
}

function updateSummaryPanels(activeFilters, query) {
    const filterCount = activeFilters.size + (query ? 1 : 0);
    document.getElementById('csvTotalRows').textContent = Number(csvState.dataset.row_count || 0).toLocaleString();
    document.getElementById('csvVisibleRows').textContent = Number(csvState.pagination.total_rows || 0).toLocaleString();
    document.getElementById('csvColumnCount').textContent = Number(csvState.columns.length || 0).toLocaleString();
    document.getElementById('csvFilterCount').textContent = filterCount.toLocaleString();
    document.getElementById('advancedFilterCount').textContent = activeFilters.size.toLocaleString();
}

function updateActiveFilterInfo(activeFilters, query) {
    const info = document.getElementById('activeFilterInfo');
    const searchLabel = query ? 'Search active' : 'No global search';
    info.textContent = `${activeFilters.size} column filters | ${searchLabel}`;
}

function filterAdvancedFilters() {
    const query = document.getElementById('filterSearchInput').value.trim().toLowerCase();
    const cards = Array.from(document.querySelectorAll('.programme-advanced-filter'));
    let visibleCount = 0;

    cards.forEach(card => {
        const matches = !query || card.dataset.columnName.includes(query);
        card.classList.toggle('filter-card-hidden', !matches);
        if (matches) {
            visibleCount += 1;
        }
    });

    const info = document.getElementById('filterSearchInfo');
    info.textContent = query
        ? `Showing ${visibleCount} matching filters`
        : `Showing all ${cards.length} filters`;
}

function bindDynamicFilterEvents() {
    document.querySelectorAll('.column-filter').forEach(selectEl => {
        selectEl.addEventListener('change', async event => {
            const columnName = event.target.dataset.columnName;
            const value = event.target.value;
            setColumnFilterValue(columnName, value);
            try {
                await requestCsvPage(1);
            } catch (error) {
                showProgrammePlansError(error, 'filter the Programme Plans dataset');
            }
        });
    });
}

function updateDatasetInfo() {
    const infoEl = document.getElementById('csvInfo');
    const updatedAt = csvState.dataset.last_modified || '';
    const rowCount = Number(csvState.dataset.row_count || 0);
    infoEl.textContent = `${rowCount.toLocaleString()} rows available${updatedAt ? ` | Updated ${updatedAt}` : ''}`;
}

async function loadFilterMetadata(options = {}) {
    const savedState = options.preserveSelections === false ? null : captureUiSelections();
    const payload = await apiGet('/api/programme-plans/filters');
    const data = payload.data || {};

    csvState.columns = data.columns || [];
    csvState.primaryFilters = data.quick_filters || [];
    csvState.dataset = data.dataset || csvState.dataset;
    csvState.filterDefinitions = {};

    (data.column_filters || []).forEach(definition => {
        csvState.filterDefinitions[definition.name] = {
            options: definition.options || [],
            unique_count: definition.unique_count || 0,
        };
    });

    const pageSizeSelect = document.getElementById('csvPageSize');
    const pageSizeOptions = Array.isArray(data.page_size_options) && data.page_size_options.length > 0
        ? data.page_size_options
        : [50, 100, 250, 500];
    pageSizeSelect.innerHTML = pageSizeOptions
        .map(size => `<option value="${size}">${size}</option>`)
        .join('');

    renderCsvHeader();
    renderPrimaryFilters();
    renderAdvancedFilters();
    bindDynamicFilterEvents();
    restoreUiSelections(savedState);

    const selected = parseInt(document.getElementById('csvPageSize').value, 10);
    csvState.pageSize = Number.isFinite(selected) && selected > 0 ? selected : 100;

    filterAdvancedFilters();
    updateDatasetInfo();
}

async function requestCsvPage(page = 1) {
    const query = document.getElementById('csvSearchInput').value.trim();
    const activeFilters = getActiveColumnFilters();
    const filters = Object.fromEntries(activeFilters);

    const payload = await apiPost('/api/programme-plans/data', {
        page,
        page_size: csvState.pageSize,
        search: query,
        filters,
    });

    const data = payload.data || {};
    const meta = payload.meta || {};

    csvState.rows = data.rows || [];
    csvState.columns = data.columns || csvState.columns;
    csvState.pagination = meta.pagination || csvState.pagination;
    csvState.dataset = meta.dataset || csvState.dataset;
    csvState.page = csvState.pagination.page || 1;

    renderCsvPage();
    renderActiveFilterChips(activeFilters, query);
    updateSummaryPanels(activeFilters, query);
    updateActiveFilterInfo(activeFilters, query);
    updateDatasetInfo();
}

async function loadCsvData(options = {}) {
    await loadFilterMetadata(options);
    await requestCsvPage(1);
}

async function clearAllFilters() {
    document.getElementById('csvSearchInput').value = '';
    document.getElementById('filterSearchInput').value = '';
    document.querySelectorAll('.column-filter').forEach(selectEl => {
        selectEl.value = ALL_FILTER_OPTION_VALUE;
    });
    filterAdvancedFilters();
    await requestCsvPage(1);
}

async function updateCsvPageSize() {
    const selected = parseInt(document.getElementById('csvPageSize').value, 10);
    csvState.pageSize = Number.isFinite(selected) && selected > 0 ? selected : 100;
    await requestCsvPage(1);
}

async function goCsvPrev() {
    if (csvState.pagination.has_previous) {
        await requestCsvPage((csvState.pagination.page || 1) - 1);
    }
}

async function goCsvNext() {
    if (csvState.pagination.has_next) {
        await requestCsvPage((csvState.pagination.page || 1) + 1);
    }
}

document.addEventListener('DOMContentLoaded', async () => {
    try {
        await loadCsvData({ preserveSelections: false });
    } catch (error) {
        showProgrammePlansError(error);
    }

    document.getElementById('csvClearBtn').addEventListener('click', () => {
        clearAllFilters().catch(error => showProgrammePlansError(error, 'clear Programme Plans filters'));
    });
    document.getElementById('clearDrawerFiltersBtn').addEventListener('click', () => {
        clearAllFilters().catch(error => showProgrammePlansError(error, 'clear Programme Plans filters'));
    });
    document.getElementById('csvReloadBtn').addEventListener('click', () => {
        loadCsvData().catch(error => showProgrammePlansError(error, 'reload the Programme Plans dataset'));
    });
    document.getElementById('csvPageSize').addEventListener('change', () => {
        updateCsvPageSize().catch(error => showProgrammePlansError(error, 'change the Programme Plans page size'));
    });
    document.getElementById('csvPrevBtn').addEventListener('click', () => {
        goCsvPrev().catch(error => showProgrammePlansError(error, 'load the previous Programme Plans page'));
    });
    document.getElementById('csvNextBtn').addEventListener('click', () => {
        goCsvNext().catch(error => showProgrammePlansError(error, 'load the next Programme Plans page'));
    });

    document.getElementById('csvSearchInput').addEventListener('input', () => {
        window.clearTimeout(searchDebounceHandle);
        searchDebounceHandle = window.setTimeout(() => {
            requestCsvPage(1).catch(error => {
                showProgrammePlansError(error, 'filter the Programme Plans dataset');
            });
        }, 220);
    });

    document.getElementById('filterSearchInput').addEventListener('input', filterAdvancedFilters);

    document.getElementById('activeFilterChips').addEventListener('click', event => {
        const button = event.target.closest('button');
        if (!button) {
            return;
        }

        const chip = button.closest('.programme-chip');
        if (!chip) {
            return;
        }

        if (chip.dataset.chipType === 'query') {
            document.getElementById('csvSearchInput').value = '';
        }

        if (chip.dataset.chipType === 'column') {
            setColumnFilterValue(chip.dataset.columnName, ALL_FILTER_OPTION_VALUE);
        }

        requestCsvPage(1).catch(error => {
            showProgrammePlansError(error, 'filter the Programme Plans dataset');
        });
    });
});
