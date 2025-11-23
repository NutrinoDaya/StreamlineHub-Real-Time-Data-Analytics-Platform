import React, { useState, useEffect } from 'react';
import api from '../services/api';
import { 
  Table, 
  RefreshCw, 
  Database,
  FileSearch 
} from 'lucide-react';

const DataDiscovery = () => {
  const [tables, setTables] = useState([]);
  const [selectedTable, setSelectedTable] = useState(null);
  const [tableDetails, setTableDetails] = useState(null);
  const [loading, setLoading] = useState(false);
  const [detailsLoading, setDetailsLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchTables = async () => {
    setLoading(true);
    try {
      const response = await api.get('/api/v1/discovery/tables');
      setTables(response.data);
    } catch (err) {
      setError('Failed to load tables');
      console.error(err);
    } finally {
      setLoading(false);
    }
  };

  const fetchTableDetails = async (tableName) => {
    setDetailsLoading(true);
    try {
      const response = await api.get(`/api/v1/discovery/tables/${tableName}`);
      setTableDetails(response.data);
    } catch (err) {
      console.error(err);
    } finally {
      setDetailsLoading(false);
    }
  };

  useEffect(() => {
    fetchTables();
  }, []);

  useEffect(() => {
    if (selectedTable) {
      fetchTableDetails(selectedTable);
    }
  }, [selectedTable]);

  const handleRefresh = () => {
    if (selectedTable) {
      fetchTableDetails(selectedTable);
    }
    fetchTables();
  };

  const formatBytes = (bytes, decimals = 2) => {
    if (!+bytes) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
  };

  return (
    <div className="flex h-screen bg-gray-900 text-white">
      {/* Sidebar - Table List */}
      <div className="w-1/4 border-r border-gray-700 flex flex-col bg-gray-900">
        <div className="p-4 border-b border-gray-700 flex justify-between items-center">
          <h2 className="text-xl font-bold flex items-center gap-2">
            <Database className="h-6 w-6 text-blue-400" />
            Bronze Tables
          </h2>
          <button onClick={fetchTables} className="p-1 hover:bg-gray-800 rounded transition-colors">
            <RefreshCw className={`h-5 w-5 ${loading ? 'animate-spin' : ''}`} />
          </button>
        </div>
        <div className="flex-1 overflow-y-auto">
          {tables.map((table) => (
            <div
              key={table.name}
              onClick={() => setSelectedTable(table.name)}
              className={`p-4 cursor-pointer hover:bg-gray-800 border-b border-gray-800 transition-colors ${
                selectedTable === table.name ? 'bg-blue-900/30 border-l-4 border-l-blue-500' : ''
              }`}
            >
              <div className="font-medium text-blue-300">{table.name}</div>
              <div className="text-xs text-gray-400 mt-1 flex justify-between">
                <span>{table.record_count.toLocaleString()} records</span>
                <span>{formatBytes(table.size_bytes)}</span>
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden bg-gray-900">
        {selectedTable && tableDetails ? (
          <>
            {/* Header */}
            <div className="p-6 border-b border-gray-700 bg-gray-800/50 flex justify-between items-center">
              <div>
                <h1 className="text-2xl font-bold flex items-center gap-3">
                  <Table className="h-8 w-8 text-green-400" />
                  {tableDetails.name}
                </h1>
                <div className="text-sm text-gray-400 mt-1">
                  Last updated: {new Date().toLocaleTimeString()}
                </div>
              </div>
              <button 
                onClick={handleRefresh}
                className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 rounded-lg transition-colors text-white font-medium shadow-lg shadow-blue-900/20"
              >
                <RefreshCw className={`h-5 w-5 ${detailsLoading ? 'animate-spin' : ''}`} />
                Refresh Data
              </button>
            </div>

            {/* Content Scroll Area */}
            <div className="flex-1 overflow-y-auto p-6 space-y-8">
              
              {/* Stats Cards */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 shadow-lg">
                  <div className="text-gray-400 text-sm mb-1">Total Records</div>
                  <div className="text-3xl font-bold text-white">
                    {tableDetails.record_count.toLocaleString()}
                  </div>
                </div>
                <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 shadow-lg">
                  <div className="text-gray-400 text-sm mb-1">Total Size</div>
                  <div className="text-3xl font-bold text-white">
                    {formatBytes(tableDetails.size_bytes)}
                  </div>
                </div>
                <div className="bg-gray-800 p-6 rounded-xl border border-gray-700 shadow-lg">
                  <div className="text-gray-400 text-sm mb-1">Format</div>
                  <div className="text-3xl font-bold text-blue-400">Delta Lake</div>
                </div>
              </div>

              {/* Schema Section */}
              <div className="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden shadow-lg">
                <div className="p-4 border-b border-gray-700 bg-gray-800/80">
                  <h3 className="text-lg font-semibold flex items-center gap-2">
                    <FileSearch className="h-5 w-5 text-purple-400" />
                    Schema
                  </h3>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-left">
                    <thead className="bg-gray-900/50 text-gray-400 text-sm uppercase">
                      <tr>
                        <th className="px-6 py-3">Field Name</th>
                        <th className="px-6 py-3">Type</th>
                        <th className="px-6 py-3">Nullable</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700">
                      {tableDetails.schema.fields.map((field) => (
                        <tr key={field.name} className="hover:bg-gray-700/50 transition-colors">
                          <td className="px-6 py-3 font-mono text-blue-300">{field.name}</td>
                          <td className="px-6 py-3 text-yellow-300">{field.type}</td>
                          <td className="px-6 py-3 text-gray-400">{field.nullable.toString()}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>

              {/* Data Preview Section */}
              <div className="bg-gray-800 rounded-xl border border-gray-700 overflow-hidden shadow-lg">
                <div className="p-4 border-b border-gray-700 bg-gray-800/80">
                  <h3 className="text-lg font-semibold flex items-center gap-2">
                    <Table className="h-5 w-5 text-green-400" />
                    Data Preview (10 records)
                  </h3>
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-left whitespace-nowrap">
                    <thead className="bg-gray-900/50 text-gray-400 text-sm uppercase">
                      <tr>
                        {tableDetails.preview.length > 0 && 
                          Object.keys(tableDetails.preview[0]).map((key) => (
                            <th key={key} className="px-6 py-3">{key}</th>
                          ))
                        }
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-700">
                      {tableDetails.preview.map((row, idx) => (
                        <tr key={idx} className="hover:bg-gray-700/50 transition-colors">
                          {Object.values(row).map((val, i) => (
                            <td key={i} className="px-6 py-3 text-sm text-gray-300">
                              {typeof val === 'object' ? JSON.stringify(val) : String(val)}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                  {tableDetails.preview.length === 0 && (
                    <div className="p-8 text-center text-gray-500">
                      No data available in this table
                    </div>
                  )}
                </div>
              </div>

            </div>
          </>
        ) : (
          <div className="flex-1 flex items-center justify-center text-gray-500 flex-col gap-4">
            <Table className="h-16 w-16 opacity-20" />
            <p className="text-lg">Select a table from the sidebar to view details</p>
          </div>
        )}
      </div>
    </div>
  );
};

export default DataDiscovery;
