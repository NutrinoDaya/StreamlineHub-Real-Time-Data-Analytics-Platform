import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { 
  BarChart3, 
  Users, 
  Activity, 
  Clock, 
  Zap, 
  Database,
  RefreshCw,
  Play,
  Pause,
  CheckCircle,
  DollarSign,
} from 'lucide-react';
import { analyticsApi } from '../services/analyticsApi';

const Analytics = () => {
  const [activeTab, setActiveTab] = useState('overview');
  const [realtimeData, setRealtimeData] = useState(null);
  const [dashboardData, setDashboardData] = useState(null);
  const [sparkJobs, setSparkJobs] = useState([]);
  const [pipelineStatus, setPipelineStatus] = useState(null);
  const [loading, setLoading] = useState(true);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const [refreshInterval, setRefreshInterval] = useState(null);
  const [customerBehavior, setCustomerBehavior] = useState(null);
  const [transactionSummary, setTransactionSummary] = useState(null);
  const [pipelineHealth, setPipelineHealth] = useState(null);
  const [historicalData, setHistoricalData] = useState([]);

  useEffect(() => {
    loadData();
    
    if (autoRefresh) {
      const interval = setInterval(loadRealtimeData, 30000); // Refresh every 30 seconds
      setRefreshInterval(interval);
    }

    return () => {
      if (refreshInterval) {
        clearInterval(refreshInterval);
      }
    };
  }, [autoRefresh]);

  const loadData = async () => {
    try {
      setLoading(true);
      
      const [realtime, dashboard, historical, customerBehaviorData, transactionData, healthData] = await Promise.allSettled([
        analyticsApi.getRealtimeMetrics(),
        analyticsApi.getDashboardSummary().catch(() => null),
        analyticsApi.getHistoricalData(7).catch(() => []),
        fetch('/api/v1/analytics/customer-behavior').then(res => res.json()).catch(() => null),
        fetch('/api/v1/analytics/transaction-summary').then(res => res.json()).catch(() => null),
        fetch('/api/v1/analytics/pipeline-health').then(res => res.json()).catch(() => null)
      ]);

      if (realtime.status === 'fulfilled') {
        setRealtimeData(realtime.value);
      }
      if (dashboard.status === 'fulfilled') {
        setDashboardData(dashboard.value);
      }
      if (historical.status === 'fulfilled') {
        setHistoricalData(historical.value || []);
      }
      if (customerBehaviorData.status === 'fulfilled') {
        setCustomerBehavior(customerBehaviorData.value?.data || null);
      }
      if (transactionData.status === 'fulfilled') {
        setTransactionSummary(transactionData.value?.data || null);
      }
      if (healthData.status === 'fulfilled') {
        setPipelineHealth(healthData.value?.data || null);
        // Update pipeline status from health data
        if (healthData.value?.data) {
          setPipelineStatus({
            status: healthData.value.data.overall_status,
            lastRun: healthData.value.data.last_checked,
            components: healthData.value.data.components
          });
        }
      }
      
    } catch (error) {
      console.error('Error loading analytics data:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadRealtimeData = async () => {
    try {
      const realtime = await analyticsApi.getRealtimeMetrics();
      setRealtimeData(realtime);
    } catch (error) {
      console.error('Error loading realtime data:', error);
    }
  };

  const tabs = [
    { id: 'overview', name: 'Overview', icon: BarChart3 },
    { id: 'customers', name: 'Customer Behavior', icon: Users },
    { id: 'transactions', name: 'Transactions', icon: DollarSign },
    { id: 'pipeline', name: 'Pipeline Health', icon: Zap }
  ];

  if (loading && !realtimeData) {
    return (
      <div className="p-6">
        <div className="flex items-center justify-center h-64">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="p-6">
      {/* Header */}
      <div className="mb-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Advanced Analytics</h1>
            <p className="text-gray-600 dark:text-gray-400 mt-2">
              Real-time insights and data processing pipeline monitoring
            </p>
          </div>
          <div className="flex items-center space-x-3">
            <button
              onClick={() => setAutoRefresh(!autoRefresh)}
              className={`inline-flex items-center px-3 py-2 border rounded-md text-sm font-medium ${
                autoRefresh
                  ? 'border-green-300 text-green-700 bg-green-50 dark:bg-green-900/20 dark:text-green-400'
                  : 'border-gray-300 text-gray-700 bg-white dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600'
              }`}
            >
              {autoRefresh ? <Pause className="w-4 h-4 mr-2" /> : <Play className="w-4 h-4 mr-2" />}
              Auto Refresh
            </button>
            <button
              onClick={loadData}
              className="inline-flex items-center px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md text-sm font-medium text-gray-700 dark:text-gray-300 bg-white dark:bg-gray-800 hover:bg-gray-50 dark:hover:bg-gray-700"
            >
              <RefreshCw className="w-4 h-4 mr-2" />
              Refresh
            </button>
          </div>
        </div>
      </div>

      {/* Real-time Metrics Bar */}
      {realtimeData && (
        <div className="mb-6 bg-white dark:bg-gray-800 rounded-lg shadow p-4">
          <div className="flex items-center justify-between mb-3">
            <h3 className="text-sm font-medium text-gray-700 dark:text-gray-300">Real-time Metrics</h3>
            <span className="text-xs text-gray-500 dark:text-gray-400">
              Last updated: {new Date(realtimeData.timestamp).toLocaleTimeString()}
            </span>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
            <div className="text-center">
              <div className="text-2xl font-bold text-blue-600 dark:text-blue-400">{realtimeData.active_users.toLocaleString()}</div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Active Users</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-green-600 dark:text-green-400">{realtimeData.events_per_second}</div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Events/sec</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-purple-600 dark:text-purple-400">${realtimeData.revenue_per_minute.toFixed(0)}</div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Revenue/min</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-orange-600 dark:text-orange-400">{realtimeData.conversion_rate}%</div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Conversion</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-indigo-600 dark:text-indigo-400">{Math.floor(realtimeData.avg_session_duration / 60)}m</div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Avg Session</div>
            </div>
            <div className="text-center">
              <div className="text-2xl font-bold text-red-600 dark:text-red-400">{realtimeData.bounce_rate}%</div>
              <div className="text-xs text-gray-500 dark:text-gray-400">Bounce Rate</div>
            </div>
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="mb-6">
        <div className="border-b border-gray-200 dark:border-gray-700">
          <nav className="-mb-px flex space-x-8">
            {tabs.map((tab) => {
              const Icon = tab.icon;
              return (
                <button
                  key={tab.id}
                  onClick={() => setActiveTab(tab.id)}
                  className={`group inline-flex items-center py-4 px-1 border-b-2 font-medium text-sm ${
                    activeTab === tab.id
                      ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                      : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300 dark:text-gray-400 dark:hover:text-gray-300'
                  }`}
                >
                  <Icon className="w-5 h-5 mr-2" />
                  {tab.name}
                </button>
              );
            })}
          </nav>
        </div>
      </div>

      {/* Overview Tab */}
      {activeTab === 'overview' && (
        <div className="space-y-6">
          {/* Pipeline Status Overview */}
          <div className="bg-gradient-to-r from-green-50 to-blue-50 dark:from-green-900/20 dark:to-blue-900/20 border border-green-200 dark:border-green-800 rounded-lg p-6">
            <div className="flex items-start">
              <CheckCircle className="w-6 h-6 text-green-600 dark:text-green-400 mt-0.5 mr-3 flex-shrink-0" />
              <div>
                <h3 className="text-lg font-medium text-green-900 dark:text-green-100 mb-2">
                  Analytics Pipeline Active
                </h3>
                <p className="text-green-800 dark:text-green-200 mb-4">
                  Complete data pipeline operational: Kafka → Delta Lake → Elasticsearch → Analytics API
                </p>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                  <div className="flex items-center">
                    <CheckCircle className="w-4 h-4 mr-2 text-green-600 dark:text-green-400" />
                    <span>Kafka Events</span>
                  </div>
                  <div className="flex items-center">
                    <CheckCircle className="w-4 h-4 mr-2 text-green-600 dark:text-green-400" />
                    <span>Delta Lake Storage</span>
                  </div>
                  <div className="flex items-center">
                    <CheckCircle className="w-4 h-4 mr-2 text-green-600 dark:text-green-400" />
                    <span>Spark Aggregations</span>
                  </div>
                  <div className="flex items-center">
                    <CheckCircle className="w-4 h-4 mr-2 text-green-600 dark:text-green-400" />
                    <span>Analytics API</span>
                  </div>
                </div>
              </div>
            </div>
          </div>

          {/* Key Metrics Grid */}
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
            {dashboardData && (
              <>
                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
                  <div className="flex items-center">
                    <div className="p-2 bg-blue-100 dark:bg-blue-900/50 rounded-lg">
                      <BarChart3 className="w-6 h-6 text-blue-600 dark:text-blue-400" />
                    </div>
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Today's Revenue</p>
                      <p className="text-2xl font-bold text-gray-900 dark:text-white">
                        ${dashboardData.today_revenue?.toLocaleString() || '0'}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
                  <div className="flex items-center">
                    <div className="p-2 bg-green-100 dark:bg-green-900/50 rounded-lg">
                      <Users className="w-6 h-6 text-green-600 dark:text-green-400" />
                    </div>
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Active Users</p>
                      <p className="text-2xl font-bold text-gray-900 dark:text-white">
                        {realtimeData?.active_users?.toLocaleString() || '0'}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
                  <div className="flex items-center">
                    <div className="p-2 bg-purple-100 dark:bg-purple-900/50 rounded-lg">
                      <Activity className="w-6 h-6 text-purple-600 dark:text-purple-400" />
                    </div>
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Events/Second</p>
                      <p className="text-2xl font-bold text-gray-900 dark:text-white">
                        {realtimeData?.events_per_second || '0'}
                      </p>
                    </div>
                  </div>
                </div>

                <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
                  <div className="flex items-center">
                    <div className="p-2 bg-yellow-100 dark:bg-yellow-900/50 rounded-lg">
                      <Zap className="w-6 h-6 text-yellow-600 dark:text-yellow-400" />
                    </div>
                    <div className="ml-4">
                      <p className="text-sm font-medium text-gray-600 dark:text-gray-400">Conversion Rate</p>
                      <p className="text-2xl font-bold text-gray-900 dark:text-white">
                        {realtimeData?.conversion_rate?.toFixed(1) || '0'}%
                      </p>
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>

          {/* Charts */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-4">Historical Revenue Trends</h3>
              <div className="h-64">
                {historicalData.length > 0 ? (
                  <div className="h-full flex items-center justify-center text-gray-500">
                    Revenue trend chart would display here with {historicalData.length} data points
                  </div>
                ) : (
                  <div className="h-full flex items-center justify-center border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg">
                    <div className="text-center">
                      <BarChart3 className="w-12 h-12 mx-auto text-gray-400 mb-2" />
                      <p className="text-gray-500 dark:text-gray-400">Loading historical data...</p>
                      <p className="text-sm text-gray-400 dark:text-gray-500">Powered by Delta Lake aggregations</p>
                    </div>
                  </div>
                )}
              </div>
            </div>

            <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white mb-4">Customer Engagement</h3>
              <div className="h-64">
                {customerBehavior ? (
                  <div className="h-full flex items-center justify-center text-gray-500">
                    Customer behavior visualization with {customerBehavior.summary?.total_events || 0} events
                  </div>
                ) : (
                  <div className="h-full flex items-center justify-center border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg">
                    <div className="text-center">
                      <Users className="w-12 h-12 mx-auto text-gray-400 mb-2" />
                      <p className="text-gray-500 dark:text-gray-400">Loading customer data...</p>
                      <p className="text-sm text-gray-400 dark:text-gray-500">Real-time behavioral analytics</p>
                    </div>
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Customer Behavior Tab */}
      {activeTab === 'customers' && (
        <div className="space-y-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">Customer Behavior Analytics</h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">Aggregated customer engagement and behavioral insights</p>
            </div>
            <div className="p-6">
              {customerBehavior ? (
                <div className="space-y-6">
                  {/* Summary Cards */}
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <Users className="w-8 h-8 text-blue-600 dark:text-blue-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-blue-900 dark:text-blue-100">
                            {customerBehavior.summary?.total_events?.toLocaleString() || '0'}
                          </div>
                          <div className="text-sm text-blue-700 dark:text-blue-300">Total Events</div>
                        </div>
                      </div>
                    </div>
                    <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <Activity className="w-8 h-8 text-green-600 dark:text-green-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-green-900 dark:text-green-100">
                            {customerBehavior.summary?.unique_customers?.toLocaleString() || '0'}
                          </div>
                          <div className="text-sm text-green-700 dark:text-green-300">Unique Customers</div>
                        </div>
                      </div>
                    </div>
                    <div className="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <BarChart3 className="w-8 h-8 text-purple-600 dark:text-purple-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-purple-900 dark:text-purple-100">
                            {customerBehavior.summary?.avg_engagement?.toFixed(1) || '0'}
                          </div>
                          <div className="text-sm text-purple-700 dark:text-purple-300">Avg Engagement</div>
                        </div>
                      </div>
                    </div>
                    <div className="bg-orange-50 dark:bg-orange-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <DollarSign className="w-8 h-8 text-orange-600 dark:text-orange-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-orange-900 dark:text-orange-100">
                            {customerBehavior.summary?.top_actions?.length || '0'}
                          </div>
                          <div className="text-sm text-orange-700 dark:text-orange-300">Action Types</div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Top Actions */}
                  <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-6">
                    <h4 className="text-md font-medium text-gray-900 dark:text-white mb-4">Top Customer Actions</h4>
                    <div className="space-y-2">
                      {customerBehavior.summary?.top_actions?.map((action, index) => (
                        <div key={index} className="flex items-center justify-between p-3 bg-white dark:bg-gray-800 rounded-md">
                          <span className="font-medium text-gray-900 dark:text-white">{action}</span>
                          <span className="text-sm text-gray-500 dark:text-gray-400">#{index + 1}</span>
                        </div>
                      )) || (
                        <p className="text-gray-500 dark:text-gray-400 text-center py-4">No action data available</p>
                      )}
                    </div>
                  </div>

                  {/* Device Types */}
                  <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-6">
                    <h4 className="text-md font-medium text-gray-900 dark:text-white mb-4">Device Distribution</h4>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      {customerBehavior.summary?.top_devices?.map((device, index) => (
                        <div key={index} className="bg-white dark:bg-gray-800 rounded-lg p-4 text-center">
                          <div className="text-lg font-semibold text-gray-900 dark:text-white capitalize">{device}</div>
                          <div className="text-sm text-gray-500 dark:text-gray-400">Device Type</div>
                        </div>
                      )) || (
                        <p className="text-gray-500 dark:text-gray-400 text-center py-4 col-span-3">No device data available</p>
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="h-64 flex items-center justify-center">
                  <div className="text-center">
                    <Users className="w-16 h-16 mx-auto text-gray-400 mb-4" />
                    <h4 className="text-xl font-medium text-gray-900 dark:text-white mb-2">Loading Customer Analytics</h4>
                    <p className="text-gray-500 dark:text-gray-400">Fetching behavioral insights from Delta Lake aggregations...</p>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Transactions Tab */}
      {activeTab === 'transactions' && (
        <div className="space-y-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">Transaction Analytics</h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">Revenue insights and transaction performance metrics</p>
            </div>
            <div className="p-6">
              {transactionSummary ? (
                <div className="space-y-6">
                  {/* Summary Cards */}
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
                    <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <DollarSign className="w-8 h-8 text-green-600 dark:text-green-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-green-900 dark:text-green-100">
                            ${transactionSummary.summary?.total_revenue?.toLocaleString() || '0'}
                          </div>
                          <div className="text-sm text-green-700 dark:text-green-300">Total Revenue</div>
                        </div>
                      </div>
                    </div>
                    <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <BarChart3 className="w-8 h-8 text-blue-600 dark:text-blue-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-blue-900 dark:text-blue-100">
                            {transactionSummary.summary?.total_transactions?.toLocaleString() || '0'}
                          </div>
                          <div className="text-sm text-blue-700 dark:text-blue-300">Total Transactions</div>
                        </div>
                      </div>
                    </div>
                    <div className="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <Activity className="w-8 h-8 text-purple-600 dark:text-purple-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-purple-900 dark:text-purple-100">
                            ${transactionSummary.summary?.avg_transaction_value?.toFixed(2) || '0'}
                          </div>
                          <div className="text-sm text-purple-700 dark:text-purple-300">Avg Transaction</div>
                        </div>
                      </div>
                    </div>
                    <div className="bg-yellow-50 dark:bg-yellow-900/20 rounded-lg p-4">
                      <div className="flex items-center">
                        <CheckCircle className="w-8 h-8 text-yellow-600 dark:text-yellow-400 mr-3" />
                        <div>
                          <div className="text-lg font-semibold text-yellow-900 dark:text-yellow-100">
                            {transactionSummary.summary?.success_rate?.toFixed(1) || '0'}%
                          </div>
                          <div className="text-sm text-yellow-700 dark:text-yellow-300">Success Rate</div>
                        </div>
                      </div>
                    </div>
                  </div>

                  {/* Status Breakdown */}
                  <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-6">
                    <h4 className="text-md font-medium text-gray-900 dark:text-white mb-4">Transaction Status</h4>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      {transactionSummary.summary?.status_breakdown ? Object.entries(transactionSummary.summary.status_breakdown).map(([status, count]) => (
                        <div key={status} className="bg-white dark:bg-gray-800 rounded-lg p-4 text-center">
                          <div className="text-lg font-semibold text-gray-900 dark:text-white">{count.toLocaleString()}</div>
                          <div className="text-sm text-gray-500 dark:text-gray-400 capitalize">{status}</div>
                        </div>
                      )) : (
                        <p className="text-gray-500 dark:text-gray-400 text-center py-4 col-span-3">No status data available</p>
                      )}
                    </div>
                  </div>

                  {/* Hourly Revenue */}
                  <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-6">
                    <h4 className="text-md font-medium text-gray-900 dark:text-white mb-4">Recent Hourly Revenue</h4>
                    <div className="space-y-3">
                      {transactionSummary.hourly_revenue?.slice(0, 5).map((hour, index) => (
                        <div key={index} className="flex items-center justify-between p-3 bg-white dark:bg-gray-800 rounded-md">
                          <div className="flex items-center">
                            <Clock className="w-4 h-4 mr-3 text-gray-500" />
                            <span className="font-medium text-gray-900 dark:text-white">
                              {new Date(hour.hour).toLocaleTimeString()}
                            </span>
                          </div>
                          <div className="text-right">
                            <div className="font-semibold text-green-600 dark:text-green-400">${hour.revenue.toFixed(2)}</div>
                            <div className="text-xs text-gray-500">{hour.transactions} transactions</div>
                          </div>
                        </div>
                      )) || (
                        <p className="text-gray-500 dark:text-gray-400 text-center py-4">No hourly data available</p>
                      )}
                    </div>
                  </div>
                </div>
              ) : (
                <div className="h-64 flex items-center justify-center">
                  <div className="text-center">
                    <DollarSign className="w-16 h-16 mx-auto text-gray-400 mb-4" />
                    <h4 className="text-xl font-medium text-gray-900 dark:text-white mb-2">Loading Transaction Analytics</h4>
                    <p className="text-gray-500 dark:text-gray-400">Fetching revenue insights from aggregated data...</p>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Real-time Tab */}
      {activeTab === 'realtime' && (
        <div className="space-y-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <div className="flex items-center justify-between">
                <h3 className="text-lg font-medium text-gray-900 dark:text-white">Live Data Stream</h3>
                <div className="flex items-center space-x-2">
                  <div className="w-2 h-2 bg-green-500 rounded-full animate-pulse"></div>
                  <span className="text-sm text-green-600 dark:text-green-400">Live</span>
                </div>
              </div>
            </div>
            <div className="p-6">
              <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-6 mb-4">
                <div className="flex items-start">
                  <Activity className="w-6 h-6 text-blue-600 dark:text-blue-400 mt-0.5 mr-3 flex-shrink-0" />
                  <div>
                    <h4 className="text-lg font-medium text-blue-900 dark:text-blue-100 mb-2">
                      Real-Time Dashboard Available
                    </h4>
                    <p className="text-blue-800 dark:text-blue-200 mb-3">
                      Live event streaming and real-time metrics are now available on the <strong>High Throughput Dashboard</strong> page with WebSocket-powered updates.
                    </p>
                    <a 
                      href="/dashboard" 
                      className="inline-flex items-center px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-sm font-medium transition-colors"
                    >
                      Go to Live Dashboard →
                    </a>
                  </div>
                </div>
              </div>
              
              <div className="h-64 flex items-center justify-center border-2 border-dashed border-gray-300 dark:border-gray-600 rounded-lg">
                <div className="text-center">
                  <Activity className="w-16 h-16 mx-auto text-blue-500 mb-4" />
                  <h4 className="text-xl font-medium text-gray-900 dark:text-white mb-2">Real-time Event Stream</h4>
                  <p className="text-gray-500 dark:text-gray-400 mb-4">Live Kafka event processing and visualization</p>
                  <div className="flex items-center justify-center space-x-4 text-sm">
                    <div className="flex items-center">
                      <div className="w-3 h-3 bg-blue-500 rounded-full mr-2"></div>
                      <span className="text-gray-600 dark:text-gray-400">Customer Events</span>
                    </div>
                    <div className="flex items-center">
                      <div className="w-3 h-3 bg-green-500 rounded-full mr-2"></div>
                      <span className="text-gray-600 dark:text-gray-400">Analytics Events</span>
                    </div>
                    <div className="flex items-center">
                      <div className="w-3 h-3 bg-purple-500 rounded-full mr-2"></div>
                      <span className="text-gray-600 dark:text-gray-400">Campaign Events</span>
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Spark Data Processing Tab */}
      {activeTab === 'spark' && (
        <div className="space-y-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">Apache Spark Processing Jobs</h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">Spark streaming jobs processing Kafka topics</p>
            </div>
            <div className="p-6">
              <div className="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800 rounded-lg p-6 mb-6">
                <div className="flex items-start">
                  <Database className="w-6 h-6 text-blue-600 dark:text-blue-400 mt-0.5 mr-3 flex-shrink-0" />
                  <div>
                    <h4 className="text-lg font-medium text-blue-900 dark:text-blue-100 mb-2">
                      Spark Cluster Active
                    </h4>
                    <p className="text-blue-800 dark:text-blue-200 mb-3">
                      Apache Spark cluster is running and processing Kafka streams. View job details in the Spark UI.
                    </p>
                    <div className="flex space-x-3">
                      <a 
                        href="http://localhost:7080" 
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-md text-sm font-medium transition-colors"
                      >
                        Open Spark UI →
                      </a>
                      <a 
                        href="http://localhost:9095" 
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center px-4 py-2 bg-purple-600 hover:bg-purple-700 text-white rounded-md text-sm font-medium transition-colors"
                      >
                        Open Kafka UI →
                      </a>
                    </div>
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                <div className="bg-green-50 dark:bg-green-900/20 rounded-lg p-4">
                  <div className="flex items-center">
                    <CheckCircle className="w-8 h-8 text-green-600 dark:text-green-400 mr-3" />
                    <div>
                      <div className="text-lg font-semibold text-green-900 dark:text-green-100">Healthy</div>
                      <div className="text-sm text-green-700 dark:text-green-300">Cluster Status</div>
                    </div>
                  </div>
                </div>
                <div className="bg-blue-50 dark:bg-blue-900/20 rounded-lg p-4">
                  <div className="flex items-center">
                    <Activity className="w-8 h-8 text-blue-600 dark:text-blue-400 mr-3" />
                    <div>
                      <div className="text-lg font-semibold text-blue-900 dark:text-blue-100">3 Topics</div>
                      <div className="text-sm text-blue-700 dark:text-blue-300">Processing</div>
                    </div>
                  </div>
                </div>
                <div className="bg-purple-50 dark:bg-purple-900/20 rounded-lg p-4">
                  <div className="flex items-center">
                    <Zap className="w-8 h-8 text-purple-600 dark:text-purple-400 mr-3" />
                    <div>
                      <div className="text-lg font-semibold text-purple-900 dark:text-purple-100">Real-time</div>
                      <div className="text-sm text-purple-700 dark:text-purple-300">Stream Mode</div>
                    </div>
                  </div>
                </div>
              </div>

              {/* Active Kafka Topics */}
              <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-6">
                <h4 className="text-md font-medium text-gray-900 dark:text-white mb-4">Active Kafka Topics</h4>
                <div className="space-y-3">
                  <div className="flex items-center justify-between p-3 bg-white dark:bg-gray-800 rounded-md">
                    <div className="flex items-center">
                      <div className="w-2 h-2 bg-green-500 rounded-full mr-3 animate-pulse"></div>
                      <span className="font-medium text-gray-900 dark:text-white">customer-events</span>
                    </div>
                    <span className="text-sm text-gray-500 dark:text-gray-400">Customer behavior & interactions</span>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-white dark:bg-gray-800 rounded-md">
                    <div className="flex items-center">
                      <div className="w-2 h-2 bg-green-500 rounded-full mr-3 animate-pulse"></div>
                      <span className="font-medium text-gray-900 dark:text-white">analytics-data</span>
                    </div>
                    <span className="text-sm text-gray-500 dark:text-gray-400">Metrics & KPI aggregations</span>
                  </div>
                  <div className="flex items-center justify-between p-3 bg-white dark:bg-gray-800 rounded-md">
                    <div className="flex items-center">
                      <div className="w-2 h-2 bg-green-500 rounded-full mr-3 animate-pulse"></div>
                      <span className="font-medium text-gray-900 dark:text-white">campaign-events</span>
                    </div>
                    <span className="text-sm text-gray-500 dark:text-gray-400">Campaign performance tracking</span>
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>
      )}

      {/* Pipeline Health Tab */}
      {activeTab === 'pipeline' && (
        <div className="space-y-6">
          <div className="bg-white dark:bg-gray-800 rounded-lg shadow">
            <div className="p-6 border-b border-gray-200 dark:border-gray-700">
              <h3 className="text-lg font-medium text-gray-900 dark:text-white">Pipeline Health Status</h3>
              <p className="text-sm text-gray-500 dark:text-gray-400">Real-time monitoring of data processing components</p>
            </div>
            <div className="p-6">
              {pipelineHealth ? (
                <div className="space-y-6">
                  {/* Overall Status */}
                  <div className={`border rounded-lg p-6 ${
                    pipelineHealth.overall_status === 'healthy' 
                      ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800'
                      : pipelineHealth.overall_status === 'warning'
                      ? 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800'
                      : 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800'
                  }`}>
                    <div className="flex items-center">
                      {pipelineHealth.overall_status === 'healthy' ? (
                        <CheckCircle className="w-8 h-8 text-green-600 dark:text-green-400 mr-3" />
                      ) : (
                        <Zap className="w-8 h-8 text-red-600 dark:text-red-400 mr-3" />
                      )}
                      <div>
                        <h4 className="text-lg font-medium capitalize">
                          Pipeline Status: {pipelineHealth.overall_status}
                        </h4>
                        <p className="text-sm text-gray-600 dark:text-gray-400">
                          Last checked: {new Date(pipelineHealth.last_checked).toLocaleString()}
                        </p>
                      </div>
                    </div>
                  </div>

                  {/* Component Status */}
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                    {Object.entries(pipelineHealth.components).map(([component, status]) => (
                      <div key={component} className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-6">
                        <div className="flex items-start justify-between">
                          <div>
                            <h5 className="font-medium text-gray-900 dark:text-white capitalize mb-2">
                              {component.replace('_', ' ')}
                            </h5>
                            <div className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                              status.status === 'healthy'
                                ? 'bg-green-100 text-green-800 dark:bg-green-900/50 dark:text-green-200'
                                : status.status === 'warning'
                                ? 'bg-yellow-100 text-yellow-800 dark:bg-yellow-900/50 dark:text-yellow-200'
                                : 'bg-red-100 text-red-800 dark:bg-red-900/50 dark:text-red-200'
                            }`}>
                              {status.status}
                            </div>
                          </div>
                          {status.status === 'healthy' ? (
                            <CheckCircle className="w-6 h-6 text-green-500" />
                          ) : (
                            <Activity className="w-6 h-6 text-red-500" />
                          )}
                        </div>
                        <p className="text-sm text-gray-600 dark:text-gray-400 mt-3">
                          {status.message}
                        </p>
                      </div>
                    ))}
                  </div>

                  {/* External Links */}
                  <div className="bg-gray-50 dark:bg-gray-900/50 rounded-lg p-6">
                    <h4 className="text-md font-medium text-gray-900 dark:text-white mb-4">Monitoring Dashboards</h4>
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                      <a 
                        href="http://localhost:8080" 
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center justify-center p-4 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition-colors"
                      >
                        <Database className="w-5 h-5 mr-2" />
                        Airflow UI
                      </a>
                      <a 
                        href="http://localhost:7080" 
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center justify-center p-4 bg-orange-600 hover:bg-orange-700 text-white rounded-lg transition-colors"
                      >
                        <Zap className="w-5 h-5 mr-2" />
                        Spark UI
                      </a>
                      <a 
                        href="http://localhost:5601" 
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center justify-center p-4 bg-purple-600 hover:bg-purple-700 text-white rounded-lg transition-colors"
                      >
                        <BarChart3 className="w-5 h-5 mr-2" />
                        Kibana
                      </a>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="h-64 flex items-center justify-center">
                  <div className="text-center">
                    <Zap className="w-16 h-16 mx-auto text-gray-400 mb-4" />
                    <h4 className="text-xl font-medium text-gray-900 dark:text-white mb-2">Loading Pipeline Health</h4>
                    <p className="text-gray-500 dark:text-gray-400">Checking component status...</p>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default Analytics;
