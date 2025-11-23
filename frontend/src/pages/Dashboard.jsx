import React, { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { 
  Users, 
  DollarSign, 
  ShoppingCart, 
  TrendingUp,
  Activity,
  Clock
} from 'lucide-react';
import { 
  LineChart, 
  Line, 
  AreaChart,
  Area,
  BarChart, 
  Bar, 
  PieChart,
  Pie,
  Cell,
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  Legend,
  ResponsiveContainer 
} from 'recharts';
import StatCard from '../components/StatCard';
import Table from '../components/Table';
import { analyticsAPI, customersAPI } from '../services/api';

const Dashboard = () => {
  const [loading, setLoading] = useState(true);
  const [dashboardData, setDashboardData] = useState(null);
  const [realtimeMetrics, setRealtimeMetrics] = useState(null);
  const [revenueData, setRevenueData] = useState([]);
  const [channelData, setChannelData] = useState([]);

  useEffect(() => {
    fetchDashboardData();
    const interval = setInterval(fetchRealtimeMetrics, 5000);
    return () => clearInterval(interval);
  }, []);

  const fetchDashboardData = async () => {
    try {
      const [dashboard, realtime, historical, channels] = await Promise.all([
        analyticsAPI.getDashboard(),
        analyticsAPI.getRealtime(),
        analyticsAPI.getHistorical({ days: 7 }).catch(() => ({ data: [] })),
        analyticsAPI.getChannels().catch(() => ({ data: [] }))
      ]);
      
      setDashboardData(dashboard.data);
      setRealtimeMetrics(realtime.data);
      
      // Transform historical data for revenue chart
      if (historical.data && historical.data.length > 0) {
        const transformedRevenue = historical.data.map(item => ({
          name: new Date(item.date).toLocaleDateString('en-US', { weekday: 'short' }),
          revenue: item.revenue || 0
        }));
        setRevenueData(transformedRevenue);
      }
      
      // Transform channel data
      if (channels.data && channels.data.length > 0) {
        const colors = ['#3b82f6', '#8b5cf6', '#10b981', '#f59e0b', '#ef4444'];
        const transformedChannels = channels.data.map((channel, index) => ({
          name: channel.channel_name || channel.name,
          value: Math.round(channel.percentage || channel.value || 0),
          color: colors[index % colors.length]
        }));
        setChannelData(transformedChannels);
      }
      
      setLoading(false);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
      setLoading(false);
    }
  };

  const fetchRealtimeMetrics = async () => {
    try {
      const response = await analyticsAPI.getRealtime();
      setRealtimeMetrics(response.data);
    } catch (error) {
      console.error('Error fetching realtime metrics:', error);
    }
  };

  const productColumns = [
    { key: 'product_name', label: 'Product' },
    { 
      key: 'sales', 
      label: 'Sales',
      render: (row) => row.sales.toLocaleString()
    },
    { 
      key: 'revenue', 
      label: 'Revenue',
      render: (row) => `$${row.revenue.toLocaleString()}`
    },
    { 
      key: 'growth_rate', 
      label: 'Growth',
      render: (row) => (
        <span className={row.growth_rate >= 0 ? 'text-green-600' : 'text-red-600'}>
          {row.growth_rate >= 0 ? '+' : ''}{row.growth_rate}%
        </span>
      )
    },
  ];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen">
        <div className="animate-spin rounded-full h-32 w-32 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  return (
    <div className="p-6 space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold text-gray-900 dark:text-white">Business Intelligence Dashboard</h1>
        <p className="text-gray-600 dark:text-gray-400 mt-1">
          Welcome back! Here's your business performance overview.
        </p>
      </div>

      {/* Real-time Metrics Banner */}
      {realtimeMetrics && (
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          className="bg-gradient-to-r from-blue-500 to-purple-600 rounded-xl p-6 text-white"
        >
          <div className="flex items-center gap-2 mb-4">
            <Activity className="w-5 h-5" />
            <span className="font-medium">Live Metrics</span>
            <div className="flex items-center gap-1 ml-auto">
              <div className="w-2 h-2 bg-green-400 rounded-full animate-pulse"></div>
              <span className="text-sm">Updated {new Date(realtimeMetrics.timestamp).toLocaleTimeString()}</span>
            </div>
          </div>
          <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
            <div>
              <p className="text-sm opacity-90">Active Users</p>
              <p className="text-2xl font-bold">{realtimeMetrics.active_users.toLocaleString()}</p>
            </div>
            <div>
              <p className="text-sm opacity-90">Events/Sec</p>
              <p className="text-2xl font-bold">{realtimeMetrics.events_per_second.toFixed(1)}</p>
            </div>
            <div>
              <p className="text-sm opacity-90">Revenue/Min</p>
              <p className="text-2xl font-bold">${realtimeMetrics.revenue_per_minute.toFixed(0)}</p>
            </div>
            <div>
              <p className="text-sm opacity-90">Conversion Rate</p>
              <p className="text-2xl font-bold">{realtimeMetrics.conversion_rate.toFixed(1)}%</p>
            </div>
          </div>
        </motion.div>
      )}

      {/* Stats Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
        <StatCard
          title="Today's Revenue"
          value={`$${dashboardData?.today_revenue?.toLocaleString() || '0'}`}
          change={dashboardData?.revenue_change_percent}
          changeType="positive"
          icon={DollarSign}
          color="green"
        />
        <StatCard
          title="Total Orders"
          value={dashboardData?.today_orders?.toLocaleString() || '0'}
          change={dashboardData?.orders_change_percent}
          changeType="positive"
          icon={ShoppingCart}
          color="blue"
        />
        <StatCard
          title="New Customers"
          value={dashboardData?.today_customers?.toLocaleString() || '0'}
          change={dashboardData?.customers_change_percent}
          changeType="positive"
          icon={Users}
          color="purple"
        />
        <StatCard
          title="Avg Session"
          value={`${realtimeMetrics?.avg_session_duration?.toFixed(0) || '0'}s`}
          change={-2.3}
          changeType="negative"
          icon={Clock}
          color="orange"
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Revenue Trend */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-6">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            Revenue Trend
          </h3>
          {revenueData.length > 0 ? (
            <ResponsiveContainer width="100%" height={300}>
              <AreaChart data={revenueData}>
                <defs>
                  <linearGradient id="colorRevenue" x1="0" y1="0" x2="0" y2="1">
                    <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.8}/>
                    <stop offset="95%" stopColor="#3b82f6" stopOpacity={0}/>
                  </linearGradient>
                </defs>
                <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                <XAxis dataKey="name" stroke="#6b7280" />
                <YAxis stroke="#6b7280" />
                <Tooltip />
                <Area 
                  type="monotone" 
                  dataKey="revenue" 
                  stroke="#3b82f6" 
                  fillOpacity={1} 
                  fill="url(#colorRevenue)" 
                />
              </AreaChart>
            </ResponsiveContainer>
          ) : (
            <div className="h-[300px] flex items-center justify-center text-gray-400 dark:text-gray-500">
              <div className="text-center">
                <TrendingUp className="w-12 h-12 mx-auto mb-2 opacity-50" />
                <p>Loading revenue data...</p>
              </div>
            </div>
          )}
        </div>

        {/* Traffic Sources */}
        <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-6">
          <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
            Traffic Sources
          </h3>
          {channelData.length > 0 ? (
            <div className="flex items-center justify-between">
              <ResponsiveContainer width="60%" height={300}>
                <PieChart>
                  <Pie
                    data={channelData}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={100}
                    paddingAngle={5}
                    dataKey="value"
                  >
                    {channelData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip />
                </PieChart>
              </ResponsiveContainer>
              <div className="space-y-2">
                {channelData.map((item) => (
                  <div key={item.name} className="flex items-center gap-2">
                    <div 
                      className="w-3 h-3 rounded-full" 
                      style={{ backgroundColor: item.color }}
                    ></div>
                    <span className="text-sm text-gray-600 dark:text-gray-400">{item.name}</span>
                    <span className="text-sm font-medium text-gray-900 dark:text-white ml-auto">
                      {item.value}%
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ) : (
            <div className="h-[300px] flex items-center justify-center text-gray-400 dark:text-gray-500">
              <div className="text-center">
                <Activity className="w-12 h-12 mx-auto mb-2 opacity-50" />
                <p>Loading traffic sources...</p>
              </div>
            </div>
          )}
        </div>
      </div>

      {/* Top Products Table */}
      <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-6">
        <h3 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">
          Top Products
        </h3>
        <Table
          columns={productColumns}
          data={dashboardData?.top_products || []}
          isLoading={loading}
        />
      </div>
    </div>
  );
};

export default Dashboard;
