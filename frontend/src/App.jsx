import React from 'react';
import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import Sidebar from './components/Sidebar';
import Dashboard from './pages/Dashboard';
import Analytics from './pages/Analytics';
import HighThroughputDashboard from './components/HighThroughputDashboard';
import Login from './pages/Login';
import Register from './pages/Register';

// Error Boundary Component
class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null };
  }

  static getDerivedStateFromError(error) {
    return { hasError: true, error };
  }

  componentDidCatch(error, errorInfo) {
    console.error('React Error Boundary caught an error:', error, errorInfo);
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50">
          <div className="text-center">
            <h1 className="text-2xl font-bold text-red-600 mb-4">Something went wrong</h1>
            <p className="text-gray-600 mb-4">
              {this.state.error?.message || 'An unexpected error occurred'}
            </p>
            <button
              onClick={() => window.location.reload()}
              className="bg-blue-500 text-white px-4 py-2 rounded hover:bg-blue-600"
            >
              Reload Page
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

// Protected Route wrapper (temporarily disabled for testing)
const ProtectedRoute = ({ children }) => {
  // For testing purposes, bypass authentication
  return children;
  
  // Uncomment below for normal authentication
  // const token = localStorage.getItem('access_token');
  // return token ? children : <Navigate to="/login" replace />;
};

// Layout with Sidebar
const Layout = ({ children }) => {
  return (
    <div className="flex min-h-screen bg-gray-50 dark:bg-gray-900">
      <Sidebar />
      <main className="flex-1 lg:ml-64 overflow-auto">
        {children}
      </main>
    </div>
  );
};

function App() {
  return (
    <ErrorBoundary>
      <BrowserRouter>
        <Routes>
          <Route path="/login" element={<Login />} />
          <Route path="/register" element={<Register />} />
          
          {/* Direct route to high-throughput dashboard without layout for testing */}
          <Route path="/live" element={<HighThroughputDashboard />} />
          
          {/* Default route with layout */}
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <Layout>
                  <HighThroughputDashboard />
                </Layout>
              </ProtectedRoute>
            }
          />
          
          <Route
            path="/dashboard"
            element={
              <ProtectedRoute>
                <Layout>
                  <Dashboard />
                </Layout>
              </ProtectedRoute>
            }
          />
          
          <Route
            path="/analytics"
            element={
              <ProtectedRoute>
                <Layout>
                  <Analytics />
                </Layout>
              </ProtectedRoute>
            }
          />


          
          {/* Fallback route for testing */}
          <Route
            path="*"
            element={
              <div className="min-h-screen flex items-center justify-center bg-gray-50">
                <div className="text-center">
                  <h1 className="text-2xl font-bold mb-4">StreamLineHub Analytics</h1>
                  <p className="mb-4">Route not found. Available routes:</p>
                  <ul className="text-left">
                    <li>/ - Main Dashboard</li>
                    <li>/live - High Throughput Dashboard</li>
                    <li>/dashboard - Standard Dashboard</li>
                  </ul>
                </div>
              </div>
            }
          />
        </Routes>
      </BrowserRouter>
    </ErrorBoundary>
  );
}

export default App;
