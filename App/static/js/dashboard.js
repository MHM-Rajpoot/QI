/**
 * FE Enrolment Dashboard JavaScript
 */

// API Base URL
const API_BASE = '/api';

if (window.Chart) {
    Chart.defaults.color = '#4b5d79';
    Chart.defaults.borderColor = 'rgba(148, 163, 184, 0.2)';
    Chart.defaults.font.family = '"Plus Jakarta Sans", "Segoe UI", sans-serif';
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
    Chart.defaults.plugins.legend.labels.boxWidth = 10;
    Chart.defaults.plugins.legend.labels.boxHeight = 10;
    Chart.defaults.plugins.tooltip.backgroundColor = 'rgba(15, 23, 42, 0.92)';
    Chart.defaults.plugins.tooltip.titleFont = { family: '"Space Grotesk", "Plus Jakarta Sans", sans-serif', weight: '700' };
    Chart.defaults.plugins.tooltip.bodyFont = { family: '"Plus Jakarta Sans", "Segoe UI", sans-serif' };
}

// Utility Functions
const utils = {
    formatNumber: (num) => {
        return num ? num.toLocaleString() : '0';
    },
    
    formatPercent: (num) => {
        return num ? num.toFixed(1) + '%' : '0%';
    },
    
    showLoading: () => {
        const overlay = document.createElement('div');
        overlay.className = 'spinner-overlay';
        overlay.id = 'loadingOverlay';
        overlay.innerHTML = `
            <div class="spinner-border text-primary" role="status">
                <span class="visually-hidden">Loading...</span>
            </div>
        `;
        document.body.appendChild(overlay);
    },
    
    hideLoading: () => {
        const overlay = document.getElementById('loadingOverlay');
        if (overlay) overlay.remove();
    }
};

// API Functions
const api = {
    parseEnvelope: async (response) => {
        const payload = await response.json();
        if (!response.ok || payload.status !== 'success') {
            throw new Error(payload?.error?.message || `HTTP error! status: ${response.status}`);
        }
        return payload;
    },

    get: async (endpoint) => {
        try {
            const response = await fetch(`${API_BASE}${endpoint}`);
            const payload = await api.parseEnvelope(response);
            return payload.data;
        } catch (error) {
            console.error('API Error:', error);
            window.appUI?.notify(error.message || 'A data request failed.', {
                title: 'API Error',
                tone: 'error',
                timeoutMs: 7000
            });
            throw error;
        }
    },
    
    getSummary: () => api.get('/summary'),
    getEnrolmentTrends: () => api.get('/enrolment/trends'),
    getEnrolmentByProvider: (providerId) => api.get(`/enrolment/by-provider${providerId ? '?provider_id=' + providerId : ''}`),
    getEnrolmentBySSA: () => api.get('/enrolment/by-ssa'),
    getEnrolmentByAge: () => api.get('/enrolment/by-age'),
    getEnrolmentByLevel: () => api.get('/enrolment/by-level'),
    getProviders: () => api.get('/providers'),
    getSSAList: () => api.get('/ssa'),
    getForecast: (model) => api.get(`/forecast?model=${model}`),
    getForecastCombined: (model, providerId) => api.get(`/forecast/combined?model=${model}${providerId ? '&provider_id=' + providerId : ''}`),
    compareForecast: (providerId) => api.get(`/forecast/compare${providerId ? '?provider_id=' + providerId : ''}`),
    getModelAccuracy: (model) => api.get(`/forecast/accuracy?model=${model}`)
};

// Chart Helper Functions
const chartHelpers = {
    colors: {
        primary: 'rgb(13, 110, 253)',
        success: 'rgb(25, 135, 84)',
        warning: 'rgb(255, 193, 7)',
        danger: 'rgb(220, 53, 69)',
        info: 'rgb(13, 202, 240)',
        secondary: 'rgb(108, 117, 125)',
        palette: [
            'rgb(255, 99, 132)',
            'rgb(54, 162, 235)',
            'rgb(255, 206, 86)',
            'rgb(75, 192, 192)',
            'rgb(153, 102, 255)',
            'rgb(255, 159, 64)',
            'rgb(199, 199, 199)',
            'rgb(83, 102, 255)'
        ]
    },
    
    getColor: (index) => {
        return chartHelpers.colors.palette[index % chartHelpers.colors.palette.length];
    },
    
    defaultLineOptions: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { position: 'top' }
        },
        scales: {
            y: {
                beginAtZero: false,
                ticks: {
                    callback: (value) => value.toLocaleString()
                }
            }
        }
    },
    
    defaultBarOptions: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
            legend: { display: false }
        },
        scales: {
            y: {
                beginAtZero: true,
                ticks: {
                    callback: (value) => value.toLocaleString()
                }
            }
        }
    }
};

// Global Model Selector Handler
document.addEventListener('DOMContentLoaded', () => {
    const modelSelector = document.getElementById('modelSelector');
    
    if (modelSelector) {
        modelSelector.addEventListener('change', function() {
            const model = this.value;
            
            // Update URL with new model parameter
            const url = new URL(window.location);
            url.searchParams.set('model', model);
            
            // Reload page with new model
            window.location.href = url.toString();
        });
    }
    
    console.log('FE Enrolment Dashboard loaded');
});

// Export for use in templates
window.dashboardApi = api;
window.dashboardUtils = utils;
window.dashboardCharts = chartHelpers;
