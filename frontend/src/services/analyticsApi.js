import api from './api';

export const analyticsApi = {
  // Real-time metrics
  async getRealtimeMetrics() {
    const response = await api.get('/api/v1/analytics/realtime');
    return response.data;
  },

  // Historical data
  async getHistoricalData(days = 30) {
    const response = await api.get(`/api/v1/analytics/historical?days=${days}`);
    return response.data;
  },

  // Dashboard summary
  async getDashboardSummary() {
    const response = await api.get('/api/v1/analytics/dashboard');
    return response.data;
  },

  // Channel performance
  async getChannelPerformance() {
    const response = await api.get('/api/v1/analytics/channels');
    return response.data;
  },

  // Geographic data
  async getGeographicData() {
    const response = await api.get('/api/v1/analytics/geographic');
    return response.data;
  },

  // Top products
  async getTopProducts(limit = 10) {
    const response = await api.get(`/api/v1/analytics/products/top?limit=${limit}`);
    return response.data;
  },

  // Top customers
  async getTopCustomers(limit = 10) {
    const response = await api.get(`/api/v1/analytics/customers/top?limit=${limit}`);
    return response.data;
  },

  // Revenue trends
  async getRevenueTrends(period = 'daily') {
    const response = await api.get(`/api/v1/analytics/revenue/trends?period=${period}`);
    return response.data;
  },

  // Customer behavior
  async getCustomerBehavior() {
    const response = await api.get('/api/v1/analytics/customer-behavior');
    return response.data;
  },

  // Conversion funnel
  async getConversionFunnel() {
    const response = await api.get('/api/v1/analytics/conversion-funnel');
    return response.data;
  },

  // Spark job status (for monitoring data processing)
  async getSparkJobs() {
    const response = await api.get('/api/v1/analytics/spark/jobs');
    return response.data;
  },

  async getSparkJob(jobId) {
    const response = await api.get(`/api/v1/analytics/spark/jobs/${jobId}`);
    return response.data;
  },

  // Data processing pipeline status
  async getPipelineStatus() {
    const response = await api.get('/api/v1/analytics/pipeline/status');
    return response.data;
  },

  // New Elasticsearch-based aggregation endpoints
  async getCustomerActionsAggregations() {
    const response = await api.get('/api/v1/analytics/aggregations/customer-actions');
    return response.data;
  },

  async getRevenueHourlyAggregations() {
    const response = await api.get('/api/v1/analytics/aggregations/revenue-hourly');
    return response.data;
  },

  async getSystemMetricsAggregations() {
    const response = await api.get('/api/v1/analytics/aggregations/system-metrics');
    return response.data;
  },

  async getMetricsTimeseries() {
    const response = await api.get('/api/v1/analytics/aggregations/metrics-timeseries');
    return response.data;
  },

  async getPipelineStatusAggregations() {
    const response = await api.get('/api/v1/analytics/aggregations/pipeline-status');
    return response.data;
  },

  async refreshAnalyticsCache() {
    const response = await api.post('/api/v1/analytics/refresh-cache');
    return response.data;
  }
};

export default analyticsApi;