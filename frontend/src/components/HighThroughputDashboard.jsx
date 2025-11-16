import React, { useState, useEffect, useRef } from 'react';
import { Line, Bar } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
} from 'chart.js';

// WebSocket base URL - use environment variable or fallback to localhost:4000
const WS_BASE_URL = (import.meta.env.VITE_API_URL || 'http://localhost:4000').replace('http', 'ws');

// Register Chart.js components
ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  Title,
  Tooltip,
  Legend,
  Filler
);

const HighThroughputDashboard = () => {
  // State for real-time data
  const [stats, setStats] = useState({
    total_processed: 0,
    events_per_second: 0,
    last_updated: null
  });
  
  const [recentEvents, setRecentEvents] = useState([]);
  const [eventHistory, setEventHistory] = useState([]);
  const [isConnected, setIsConnected] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState('Disconnecting...');
  
  // Refs for WebSocket connections
  const statsWs = useRef(null);
  const eventsWs = useRef(null);
  const maxEvents = 50; // Max events to keep in memory
  const maxHistory = 60; // Max history points for charts
  
  // Chart data state
  const [chartData, setChartData] = useState({
    labels: [],
    datasets: [{
      label: 'Events/Second',
      data: [],
      borderColor: 'rgb(59, 130, 246)',
      backgroundColor: 'rgba(59, 130, 246, 0.1)',
      fill: true,
      tension: 0.4
    }]
  });
  
  const [eventTypeData, setEventTypeData] = useState({
    labels: ['Customer', 'Transaction', 'Analytics'],
    datasets: [{
      label: 'Event Types',
      data: [0, 0, 0],
      backgroundColor: [
        'rgba(59, 130, 246, 0.8)',
        'rgba(16, 185, 129, 0.8)',
        'rgba(245, 158, 11, 0.8)'
      ],
      borderColor: [
        'rgba(59, 130, 246, 1)',
        'rgba(16, 185, 129, 1)',
        'rgba(245, 158, 11, 1)'
      ],
      borderWidth: 2
    }]
  });

  // Chart options
  const lineChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    interaction: {
      intersect: false,
    },
    scales: {
      x: {
        display: true,
        title: {
          display: true,
          text: 'Time'
        }
      },
      y: {
        display: true,
        title: {
          display: true,
          text: 'Events/Second'
        },
        beginAtZero: true
      }
    },
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: true,
        text: 'Real-time Event Throughput'
      }
    },
    animation: {
      duration: 200
    }
  };

  const barChartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
      },
      title: {
        display: true,
        text: 'Event Type Distribution'
      }
    },
    animation: {
      duration: 200
    }
  };

  // Connect to WebSocket endpoints
  useEffect(() => {
    // Connect to real-time dashboard WebSocket
    const connectWebSocket = () => {
      try {
        const ws = new WebSocket(`${WS_BASE_URL}/ws/dashboard`);
        statsWs.current = ws;
        
        ws.onopen = () => {
          console.log('✅ Dashboard WebSocket connected');
          setConnectionStatus('Connected - Live Data 🟢');
          setIsConnected(true);
          
          // Send ping every 30 seconds to keep connection alive
          const pingInterval = setInterval(() => {
            if (ws.readyState === WebSocket.OPEN) {
              ws.send(JSON.stringify({ type: 'ping' }));
            }
          }, 30000);
          
          ws.pingInterval = pingInterval;
        };
        
        ws.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            
            if (message.type === 'initial_data' || message.type === 'dashboard_update') {
              const data = message.data;
              console.log("kafka message data : ", data)
              // Update stats with correct Kafka metrics
              setStats({
                total_processed: data.kafka_monitoring?.consumer_consumed || 0,
                events_per_second: data.kafka_monitoring?.real_time_events_per_second || 0,
                last_updated: data.timestamp
              });
              
              // Update chart data
              const now = new Date().toLocaleTimeString();
              setChartData(prev => {
                const newLabels = [...prev.labels, now].slice(-maxHistory);
                const newData = [...prev.datasets[0].data, data.kafka_monitoring?.real_time_events_per_second || 0].slice(-maxHistory);
                
                return {
                  ...prev,
                  labels: newLabels,
                  datasets: [{
                    ...prev.datasets[0],
                    data: newData
                  }]
                };
              });
              
              // Update event type distribution based on actual recent events
              const recentEvents = data.recent_events || [];
              const eventTypes = { 'Customer': 0, 'Transaction': 0, 'Analytics': 0 };
              
              recentEvents.forEach(event => {
                const type = event.event_type || 'Analytics';
                if (type.includes('customer') || type === 'customer_behavior' || type === 'customer_registration' || type === 'customer_profile_update') {
                  eventTypes['Customer']++;
                } else if (type.includes('transaction') || type === 'transaction_completed') {
                  eventTypes['Transaction']++;
                } else if (type === 'system_metric' || type === 'analytics_metric' || type.includes('metric')) {
                  eventTypes['Analytics']++;
                } else {
                  eventTypes['Analytics']++; // Default to Analytics for unknown types
                }
              });
              
              setEventTypeData(prev => ({
                ...prev,
                labels: ['Customer', 'Transaction', 'Analytics'],
                datasets: [{
                  ...prev.datasets[0],
                  data: [eventTypes['Customer'], eventTypes['Transaction'], eventTypes['Analytics']]
                }]
              }));
              
              // Update recent events
              if (data.recent_events && data.recent_events.length > 0) {
                setRecentEvents(prev => {
                  const newEvents = [...data.recent_events, ...prev].slice(0, maxEvents);
                  return newEvents;
                });
              }
            } else if (message.type === 'pong') {
              console.log('Received pong from server');
            }
          } catch (e) {
            console.error('Error parsing WebSocket message:', e);
          }
        };
        
        ws.onerror = (error) => {
          console.error('WebSocket error:', error);
          setConnectionStatus('Connection Error ❌');
        };
        
        ws.onclose = () => {
          console.log('❌ Dashboard WebSocket disconnected');
          setConnectionStatus('Disconnected - Reconnecting... 🔄');
          setIsConnected(false);
          
          // Clear ping interval
          if (ws.pingInterval) {
            clearInterval(ws.pingInterval);
          }
          
          // Reconnect after 3 seconds
          setTimeout(connectWebSocket, 3000);
        };
      } catch (error) {
        console.error('Failed to connect WebSocket:', error);
        setConnectionStatus('Connection Failed ❌');
        setIsConnected(false);
        
        // Retry connection after 5 seconds
        setTimeout(connectWebSocket, 5000);
      }
    };
    
    connectWebSocket();
    
    // Cleanup on unmount
    return () => {
      if (statsWs.current) {
        if (statsWs.current.pingInterval) {
          clearInterval(statsWs.current.pingInterval);
        }
        statsWs.current.close();
      }
      if (eventsWs.current) {
        eventsWs.current.close();
      }
    };
  }, []);

  // Format numbers for display
  const formatNumber = (num) => {
    if (num >= 1000000) {
      return (num / 1000000).toFixed(1) + 'M';
    } else if (num >= 1000) {
      return (num / 1000).toFixed(1) + 'K';
    }
    return num.toLocaleString();
  };

  // Get event icon based on type
  const getEventIcon = (event) => {
    switch (event.event_type) {
      case 'customer_behavior':
      case 'customer_registration':
      case 'customer_profile_update':
        return '👤';
      case 'transaction':
      case 'transaction_completed':
        return event.status === 'completed' || event.transaction_status === 'completed' ? '✅' : 
               event.status === 'failed' || event.transaction_status === 'failed' ? '❌' : '⏳';
      case 'analytics_metric':
      case 'system_metric':
        return '📊';
      default:
        return '📡';
    }
  };

  // Get event description
  const getEventDescription = (event) => {
    switch (event.event_type) {
      case 'customer_behavior':
        return `Customer ${event.customer_id}: ${event.action}${event.value ? ` ($${event.value})` : ''}`;
      case 'customer_registration':
        return `New customer: ${event.full_name || event.customer_id} (${event.segment || 'standard'})`;
      case 'customer_profile_update':
        return `Profile update for customer ${event.customer_id} (${event.updated_fields?.length || 0} fields)`;
      case 'transaction':
      case 'transaction_completed':
        return `${event.transaction_id}: $${event.amount || event.total_amount} (${event.status || event.transaction_status || 'completed'})`;
      case 'analytics_metric':
      case 'system_metric':
        return `${event.metric_name}: ${event.value} ${event.unit || ''}`;
      default:
        return `${event.event_type || 'Unknown event'}: ${JSON.stringify(event).substring(0, 50)}...`;
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white p-6">
      {/* Header */}
      <div className="mb-8">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-4xl font-bold bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
              StreamLineHub Analytics
            </h1>
            <p className="text-gray-400 text-lg">High-Throughput Real-time Event Dashboard</p>
          </div>
          <div className="flex items-center space-x-4">
            <div className={`flex items-center space-x-2 px-4 py-2 rounded-full ${
              isConnected ? 'bg-green-900 text-green-300' : 'bg-red-900 text-red-300'
            }`}>
              <div className={`w-3 h-3 rounded-full ${
                isConnected ? 'bg-green-500 animate-pulse' : 'bg-red-500'
              }`}></div>
              <span className="text-sm font-medium">{connectionStatus}</span>
            </div>
          </div>
        </div>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
        <div className="bg-gradient-to-br from-blue-600 to-blue-800 p-6 rounded-xl shadow-xl">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-blue-200 text-sm font-medium">Total Events</p>
              <p className="text-3xl font-bold text-white">{formatNumber(stats.total_processed)}</p>
            </div>
            <div className="text-blue-200 text-3xl">📊</div>
          </div>
        </div>
        
        <div className="bg-gradient-to-br from-green-600 to-green-800 p-6 rounded-xl shadow-xl">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-green-200 text-sm font-medium">Events/Second</p>
              <p className="text-3xl font-bold text-white">{stats.events_per_second.toLocaleString()}</p>
            </div>
            <div className="text-green-200 text-3xl">⚡</div>
          </div>
        </div>
      </div>

      {/* Charts */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
        <div className="bg-gray-800 p-6 rounded-xl shadow-xl">
          <div className="h-64">
            <Line data={chartData} options={lineChartOptions} />
          </div>
        </div>
        
        <div className="bg-gray-800 p-6 rounded-xl shadow-xl">
          <div className="h-64">
            <Bar data={eventTypeData} options={barChartOptions} />
          </div>
        </div>
      </div>

      {/* Live Event Stream */}
      <div className="bg-gray-800 rounded-xl shadow-xl overflow-hidden">
        <div className="p-6 border-b border-gray-700">
          <h3 className="text-xl font-bold text-white mb-2">Live Event Stream</h3>
          <p className="text-gray-400">Real-time events as they're processed ({recentEvents.length} recent)</p>
        </div>
        
        <div className="max-h-96 overflow-y-auto">
          {recentEvents.length === 0 ? (
            <div className="p-8 text-center text-gray-500">
              <div className="text-4xl mb-4">🔄</div>
              <p>Waiting for events...</p>
            </div>
          ) : (
            <div className="divide-y divide-gray-700">
              {recentEvents.map((event, index) => (
                <div key={index} className="p-4 hover:bg-gray-700 transition-colors">
                  <div className="flex items-center space-x-3">
                    <span className="text-2xl">{getEventIcon(event)}</span>
                    <div className="flex-1">
                      <div className="flex items-center justify-between">
                        <p className="text-white font-medium">{getEventDescription(event)}</p>
                        <span className="text-xs text-gray-400">
                          {event.processed_at ? new Date(event.processed_at).toLocaleTimeString() : 'Now'}
                        </span>
                      </div>
                      <div className="flex items-center space-x-4 mt-1">
                        <span className="text-xs text-blue-400 bg-blue-900 px-2 py-1 rounded">
                          {event.event_type}
                        </span>
                        {event.batch_size && (
                          <span className="text-xs text-green-400">
                            Batch: {event.batch_size}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
};

export default HighThroughputDashboard;