import axios from 'axios';

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:4000';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token to requests (temporarily disabled for testing)
// api.interceptors.request.use((config) => {
//   const token = localStorage.getItem('access_token');
//   if (token) {
//     config.headers.Authorization = `Bearer ${token}`;
//   }
//   return config;
// });

// Handle response errors
api.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config;

    // If error is 401 and we haven't retried yet
    if (error.response?.status === 401 && !originalRequest._retry) {
      originalRequest._retry = true;

      try {
        // Try to refresh the token
        const refreshToken = localStorage.getItem('refresh_token');
        if (refreshToken) {
          const response = await axios.post(`${API_BASE_URL}/api/v1/auth/refresh`, {
            refresh_token: refreshToken,
          });

          const { access_token, refresh_token: newRefreshToken } = response.data;
          localStorage.setItem('access_token', access_token);
          localStorage.setItem('refresh_token', newRefreshToken);

          // Retry original request with new token
          originalRequest.headers.Authorization = `Bearer ${access_token}`;
          return api(originalRequest);
        }
      } catch (refreshError) {
        // Refresh failed, logout user
        localStorage.removeItem('access_token');
        localStorage.removeItem('refresh_token');
        localStorage.removeItem('user');
        window.location.href = '/login';
        return Promise.reject(refreshError);
      }
    }

    // If still 401 or other error, logout
    if (error.response?.status === 401) {
      localStorage.removeItem('access_token');
      localStorage.removeItem('refresh_token');
      localStorage.removeItem('user');
      window.location.href = '/login';
    }

    return Promise.reject(error);
  }
);

export default api;

// API endpoints
export const authAPI = {
  login: async (email, password, rememberMe = false) => {
    const response = await api.post('/api/v1/auth/login', {
      email,
      password,
      remember_me: rememberMe,
    });
    const { access_token, refresh_token, user } = response.data;
    localStorage.setItem('access_token', access_token);
    localStorage.setItem('refresh_token', refresh_token);
    localStorage.setItem('user', JSON.stringify(user));
    return response.data;
  },
  register: async (email, password, fullName) => {
    const response = await api.post('/api/v1/auth/register', {
      email,
      password,
      full_name: fullName,
    });
    const { access_token, refresh_token, user } = response.data;
    localStorage.setItem('access_token', access_token);
    localStorage.setItem('refresh_token', refresh_token);
    localStorage.setItem('user', JSON.stringify(user));
    return response.data;
  },
  logout: () => {
    localStorage.removeItem('access_token');
    localStorage.removeItem('refresh_token');
    localStorage.removeItem('user');
    return api.post('/api/v1/auth/logout');
  },
  getCurrentUser: () => api.get('/api/v1/auth/me'),
  refreshToken: async () => {
    const refreshToken = localStorage.getItem('refresh_token');
    if (!refreshToken) throw new Error('No refresh token');
    const response = await api.post('/api/v1/auth/refresh', {
      refresh_token: refreshToken,
    });
    const { access_token, refresh_token: newRefreshToken } = response.data;
    localStorage.setItem('access_token', access_token);
    localStorage.setItem('refresh_token', newRefreshToken);
    return response.data;
  },
};


export const customersAPI = {
  getAll: (params) => api.get('/api/v1/customers', { params }),
  getStats: () => api.get('/api/v1/customers/stats'),
  getById: (id) => api.get(`/api/v1/customers/${id}`),
  create: (data) => api.post('/api/v1/customers', data),
  update: (id, data) => api.put(`/api/v1/customers/${id}`, data),
  delete: (id) => api.delete(`/api/v1/customers/${id}`),
};

export const analyticsAPI = {
  getRealtime: () => api.get('/api/v1/analytics/realtime'),
  getHistorical: (params) => api.get('/api/v1/analytics/historical', { params }),
  getDashboard: () => api.get('/api/v1/analytics/dashboard'),
  getChannels: () => api.get('/api/v1/analytics/channels'),
  getGeography: () => api.get('/api/v1/analytics/geography'),
  getTrends: (params) => api.get('/api/v1/analytics/trends', { params }),
};

export const campaignsAPI = {
  getAll: (params) => api.get('/api/v1/campaigns', { params }),
  getStats: () => api.get('/api/v1/campaigns/stats'),
  getById: (id) => api.get(`/api/v1/campaigns/${id}`),
  create: (data) => api.post('/api/v1/campaigns', data),
  update: (id, data) => api.put(`/api/v1/campaigns/${id}`, data),
  delete: (id) => api.delete(`/api/v1/campaigns/${id}`),
  activate: (id) => api.post(`/api/v1/campaigns/${id}/activate`),
  pause: (id) => api.post(`/api/v1/campaigns/${id}/pause`),
  getPerformance: (id, params) => api.get(`/api/v1/campaigns/${id}/performance`, { params }),
};

export const mlAPI = {
  getModels: (params) => api.get('/api/v1/ml/models', { params }),
  getModel: (id) => api.get(`/api/v1/ml/models/${id}`),
  createModel: (data) => api.post('/api/v1/ml/models', data),
  predict: (data) => api.post('/api/v1/ml/predict', data),
  predictChurn: (data) => api.post('/api/v1/ml/churn/predict', data),
  segment: (data) => api.post('/api/v1/ml/segment', data),
  recommend: (data) => api.post('/api/v1/ml/recommend', data),
  getModelPerformance: (id, params) => api.get(`/api/v1/ml/models/${id}/performance`, { params }),
};

export const adminAPI = {
  getUsers: (params) => api.get('/api/v1/admin/users', { params }),
  getUser: (id) => api.get(`/api/v1/admin/users/${id}`),
  createUser: (data) => api.post('/api/v1/admin/users', data),
  updateUser: (id, data) => api.put(`/api/v1/admin/users/${id}`, data),
  deleteUser: (id) => api.delete(`/api/v1/admin/users/${id}`),
  getSystemStats: () => api.get('/api/v1/admin/system/stats'),
  getAuditLogs: (params) => api.get('/api/v1/admin/audit-logs', { params }),
  getConfig: () => api.get('/api/v1/admin/config'),
  updateConfig: (key, value) => api.put(`/api/v1/admin/config/${key}`, { value }),
  getDashboardMetrics: () => api.get('/api/v1/admin/dashboard-metrics'),
};

export const healthAPI = {
  check: () => api.get('/health'),
  ready: () => api.get('/ready'),
  live: () => api.get('/live'),
};
