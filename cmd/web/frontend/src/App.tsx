import { useState, useEffect } from 'react';
import Editor from '@monaco-editor/react';
import './App.css';

interface Plugin {
  name: string;
  description: string;
  createdAt: number;
  compiledAt: number;
  isCompiled: boolean;
  compiledPath: string;
}

interface Event {
  id: string;
  state: string;
  payload: string;
  result?: string;
  attempts: number;
  max_attempts: number;
  created_at: string;
  updated_at: string;
  completed_at?: string;
  error?: string;
  delegation_type: string;
  dead: boolean;
  priority: number;
}

type TaskStateEvent = Record<string, unknown>;

function App() {
  const [code, setCode] = useState('');
  const [pluginName, setPluginName] = useState('');
  const [description, setDescription] = useState('');
  const [plugins, setPlugins] = useState<Plugin[]>([]);
  const [selectedPlugin, setSelectedPlugin] = useState<string | null>(null);
  const [eventPayload, setEventPayload] = useState('{}');
  const [message, setMessage] = useState('');
  const [loading, setLoading] = useState(false);
  const [events, setEvents] = useState<Event[]>([]);
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);
  const [eventsPage, setEventsPage] = useState(1);
  const [eventsTotal, setEventsTotal] = useState(0);
  const [eventsFilter, setEventsFilter] = useState('');
  const [taskStates, setTaskStates] = useState<TaskStateEvent[]>([]);
  const [streamConnected, setStreamConnected] = useState(false);
  const examplePluginDescription =
    'Simple example plugin that repeats a message. Use this as a starter template for your own plugin.';
  const examplePluginCode = `package main

import (
    "context"
    "encoding/json"
    "fmt"
    "strings"

    "grain/nexus"
)

type ExamplePlugin struct{}

type ExampleArgs struct {
    Message string \`json:"message"\`
    Count   int    \`json:"count"\`
}

func (p *ExamplePlugin) Meta() nexus.PluginMeta {
    return nexus.PluginMeta{
        Name:        "example.WebPlugin",
        Description: "Example plugin created via web UI",
        Version:     1,
        ArgsSchemaJSON: json.RawMessage(\`{
            "type": "object",
            "properties": {
                "message": {"type": "string"},
                "count": {"type": "integer", "default": 1}
            },
            "required": ["message"]
        }\`),
    }
}

func (p *ExamplePlugin) Execute(ctx context.Context, args ExampleArgs) (string, error) {
    if args.Message == "" {
        return "", fmt.Errorf("message is required")
    }
    if args.Count <= 0 {
        args.Count = 1
    }

    var output strings.Builder
    for index := 0; index < args.Count; index++ {
        output.WriteString(fmt.Sprintf("[%d] %s\\n", index+1, args.Message))
    }
    return output.String(), nil
}

func Plugin(_ string) (nexus.Plugin, error) {
    return &ExamplePlugin{}, nil
}
`;

  const loadExamplePlugin = () => {
    setPluginName('example.WebPlugin');
    setDescription(examplePluginDescription);
    setCode(examplePluginCode);
    setEventPayload(
      JSON.stringify(
        {
          message: 'Hello from the web UI',
          count: 2,
        },
        null,
        2,
      ),
    );
    setMessage('Example plugin loaded. You can edit it before creating.');
  };

  useEffect(() => {
    loadPlugins();
    loadEvents();
  }, []);

  useEffect(() => {
    loadEvents();
  }, [eventsPage, eventsFilter]);


  useEffect(() => {
    const es = new EventSource("/api/task-stream");

    es.onopen = () => setStreamConnected(true);

    es.addEventListener("task_state", (event: MessageEvent) => {
      try {
        const parsed = JSON.parse(event.data) as TaskStateEvent;
        setTaskStates((prev) => [parsed, ...prev].slice(0, 100));
      } catch {
        console.warn("Failed to parse task state event:", event.data);  
      }
    });

    es.onerror = () => {
      setStreamConnected(false);
    };

    return () => {
      es.close();
      setStreamConnected(false);
    };
  }, []);

  const parseApiResponse = async (res: Response): Promise<any> => {
    const raw = await res.text();
    if (!raw) {
      return {};
    }

    try {
      return JSON.parse(raw);
    } catch {
      return { error: raw || `HTTP ${res.status}` };
    }
  };

  const formatError = (err: any): string => {
    if (!err) return 'Unknown error';
    if (typeof err === 'string') return err;
    if (err instanceof Error) return err.message || 'Unknown error';
    if (err.error && typeof err.error === 'string') return err.error;
    return String(err) || 'Unknown error';
  };

  const loadPlugins = async () => {
    try {
      const res = await fetch('/api/plugins');
      const data = await parseApiResponse(res);
      if (!res.ok) {
        const errorMsg = data.error || data.message || `HTTP ${res.status}`;
        setMessage(`Error: ${errorMsg}`);
        return;
      }
      setPlugins(data.plugins || []);
    } catch (err) {
      setMessage(`Error loading plugins: ${formatError(err)}`);
    }
  };

  const loadEvents = async () => {
    try {
      const params = new URLSearchParams({
        page: eventsPage.toString(),
        limit: '10',
      });
      if (eventsFilter) {
        params.append('state', eventsFilter);
      }
      const res = await fetch(`/api/events?${params}`);
      const data = await parseApiResponse(res);
      if (!res.ok) {
        const errorMsg = data.error || data.message || `HTTP ${res.status}`;
        console.error(`Error loading events: ${errorMsg}`);
        return;
      }
      setEvents(data.events || []);
      setEventsTotal(data.total || 0);
    } catch (err) {
      console.error(`Error loading events: ${formatError(err)}`);
    }
  };

  const loadEventDetails = async (eventId: string) => {
    try {
      const res = await fetch(`/api/events/${eventId}`);
      const data = await parseApiResponse(res);
      if (!res.ok) {
        const errorMsg = data.error || data.message || `HTTP ${res.status}`;
        setMessage(`Error: ${errorMsg}`);
        return;
      }
      setSelectedEvent(data.event);
    } catch (err) {
      setMessage(`Error loading event: ${formatError(err)}`);
    }
  };

  const handleCreatePlugin = async () => {
    if (!pluginName.trim() || !code.trim()) {
      setMessage('Plugin name and code are required');
      return;
    }

    setLoading(true);
    try {
      const res = await fetch('/api/plugins/create', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: pluginName,
          code: code,
          description: description,
        }),
      });

      const data = await parseApiResponse(res);
      if (res.ok) {
        setMessage(`Plugin "${pluginName}" created successfully`);
        setPluginName('');
        setDescription('');
        setCode('');
        loadPlugins();
      } else {
        const errorMsg = data.error || data.message || `HTTP ${res.status}`;
        setMessage(`Error: ${errorMsg}`);
      }
    } catch (err) {
      setMessage(`Error creating plugin: ${formatError(err)}`);
    } finally {
      setLoading(false);
    }
  };

  const handleCompilePlugin = async (name: string) => {
    setLoading(true);
    try {
      const res = await fetch('/api/plugins/compile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      });

      const data = await parseApiResponse(res);
      if (res.ok) {
        setMessage(`Plugin compiled successfully: ${data.path}`);
        loadPlugins();
      } else {
        const errorMsg = data.error || data.message || `HTTP ${res.status}`;
        setMessage(`Compilation error: ${errorMsg}`);
      }
    } catch (err) {
      setMessage(`Error compiling plugin: ${formatError(err)}`);
    } finally {
      setLoading(false);
    }
  };

  const formatDate = (dateStr: string) => {
    if (!dateStr) return 'N/A';
    return new Date(dateStr).toLocaleString();
  };

  const getStateColor = (state: string) => {
    const colors: { [key: string]: string } = {
      COMPLETED: '#10b981',
      FAILED: '#ef4444',
      PROCESSING: '#3b82f6',
      PENDING: '#f59e0b',
      SUMBITTED: '#8b5cf6',
      RETRIAL: '#f97316',
    };
    return colors[state] || '#6b7280';
  };

  const handleSubmitEvent = async () => {
    if (!selectedPlugin) {
      setMessage('Please select a plugin');
      return;
    }

    setLoading(true);
    try {
      const res = await fetch('/api/events/submit', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          delegationType: selectedPlugin,
          payload: eventPayload,
          maxAttempts: 3,
          priority: 0,
        }),
      });

      const data = await parseApiResponse(res);
      if (res.ok) {
        setMessage(`Event submitted! ID: ${data.eventID}`);
        setEventPayload('{}');
      } else {
        const errorMsg = data.error || data.message || `HTTP ${res.status}`;
        setMessage(`Error: ${errorMsg}`);
      }
    } catch (err) {
      setMessage(`Error submitting event: ${formatError(err)}`);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <header className="header">
        <h1>Nexus Plugin Editor</h1>
        <p>Create, compile, and test plugins</p>
      </header>

      <div className="container">
        <div className="section create-section">
          <h2>Create Plugin</h2>

          <div className="plugin-guide">
            <h3>Starter Example</h3>
            <p>Use a working template to learn plugin structure quickly.</p>
            <ul>
              <li>Implements Meta and Execute methods</li>
              <li>Shows args schema in JSON</li>
              <li>Includes exported Plugin symbol</li>
            </ul>
            <button
              type="button"
              onClick={loadExamplePlugin}
              disabled={loading}
              className="btn btn-secondary btn-example"
            >
              Use Example Plugin
            </button>
          </div>

          <input
            type="text"
            placeholder="Plugin name (e.g., example.WebPlugin)"
            value={pluginName}
            onChange={(e) => setPluginName(e.target.value)}
            disabled={loading}
            className="input"
          />
          <p className="field-help">Use a namespaced name so delegationType is clear.</p>

          <input
            type="text"
            placeholder="Description (what this plugin does)"
            value={description}
            onChange={(e) => setDescription(e.target.value)}
            className="input"
          />

          <div className="editor-container">
            <Editor
              height="520px"
              defaultLanguage="go"
              value={code}
              onChange={(value: any) => setCode(value || '')}
              theme="vs-dark"
              options={{
                minimap: { enabled: false },
                fontSize: 15,
                lineNumbers: 'on',
              }}
            />
          </div>

          <button onClick={handleCreatePlugin} disabled={loading} className="btn btn-primary">
            {loading ? 'Creating...' : 'Create Plugin'}
          </button>
        </div>

        <div className="section">
          <h2>Plugins</h2>
          {plugins.length === 0 ? (
            <p>No plugins created yet</p>
          ) : (
            <div className="plugin-list">
              {plugins.map((plugin) => (
                <div key={plugin.name} className="plugin-card">
                  <div className="plugin-info">
                    <h3>{plugin.name}</h3>
                    <p>{plugin.description}</p>
                    <span className={`status ${plugin.isCompiled ? 'compiled' : 'pending'}`}>
                      {plugin.isCompiled ? '✓ Compiled' : 'Pending Compile'}
                    </span>
                  </div>
                  <div className="plugin-actions">
                    <button
                      onClick={() => handleCompilePlugin(plugin.name)}
                      disabled={loading || plugin.isCompiled}
                      className="btn btn-secondary"
                    >
                      Compile
                    </button>
                    <button
                      onClick={() => setSelectedPlugin(plugin.name)}
                      disabled={!plugin.isCompiled}
                      className={`btn ${selectedPlugin === plugin.name ? 'btn-active' : 'btn-secondary'}`}
                    >
                      {selectedPlugin === plugin.name ? '✓ Selected' : 'Select'}
                    </button>
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="section">
          <h2>Submit Event</h2>
          {selectedPlugin ? (
            <>
              <div className="selected-plugin-info">
                Selected Plugin: <strong>{selectedPlugin}</strong>
              </div>
              <label>Event Payload (JSON):</label>
              <textarea
                value={eventPayload}
                onChange={(e) => setEventPayload(e.target.value)}
                className="textarea"
                rows={10}
              />
              <button onClick={handleSubmitEvent} disabled={loading} className="btn btn-primary">
                {loading ? 'Submitting...' : 'Submit Event'}
              </button>
            </>
          ) : (
            <div className="empty-state">
              Select a compiled plugin to submit events
            </div>
          )}
        </div>

        <div className="section events-section">
          <h2>Events</h2>
          <div className="events-controls">
            <select
              value={eventsFilter}
              onChange={(e) => {
                setEventsFilter(e.target.value);
                setEventsPage(1);
              }}
              className="input"
            >
              <option value="">All States</option>
              <option value="SUMBITTED">Submitted</option>
              <option value="PENDING">Pending</option>
              <option value="PROCESSING">Processing</option>
              <option value="COMPLETED">Completed</option>
              <option value="FAILED">Failed</option>
              <option value="RETRIAL">Retrial</option>
            </select>
            <button onClick={loadEvents} className="btn btn-secondary">
              Refresh
            </button>
          </div>

          {events.length === 0 ? (
            <p>No events found</p>
          ) : (
            <>
              <div className="events-list">
                {events.map((event) => (
                  <div
                    key={event.id}
                    className={`event-card ${selectedEvent?.id === event.id ? 'active' : ''}`}
                    onClick={() => loadEventDetails(event.id)}
                  >
                    <div className="event-header">
                      <span
                        className="event-state"
                        style={{ backgroundColor: getStateColor(event.state) }}
                      >
                        {event.state}
                      </span>
                      <span className="event-type">{event.delegation_type}</span>
                    </div>
                    <div className="event-meta">
                      <span>ID: {event.id.slice(-8)}</span>
                      <span>
                        Attempts: {event.attempts}/{event.max_attempts}
                      </span>
                      {event.dead && <span className="event-dead">💀 Dead</span>}
                    </div>
                    <div className="event-time">{formatDate(event.created_at)}</div>
                  </div>
                ))}
              </div>

              <div className="pagination">
                <button
                  onClick={() => setEventsPage((p) => Math.max(1, p - 1))}
                  disabled={eventsPage === 1}
                  className="btn btn-secondary"
                >
                  Previous
                </button>
                <span>
                  Page {eventsPage} of {Math.ceil(eventsTotal / 10)}
                </span>
                <button
                  onClick={() => setEventsPage((p) => p + 1)}
                  disabled={eventsPage >= Math.ceil(eventsTotal / 10)}
                  className="btn btn-secondary"
                >
                  Next
                </button>
              </div>
            </>
          )}

          {selectedEvent && (
            <div className="event-details">
              <div className="event-details-header">
                <h3>Event Details</h3>
                <button onClick={() => setSelectedEvent(null)} className="btn btn-secondary">
                  Close
                </button>
              </div>

              <div className="detail-row">
                <strong>ID:</strong> <code>{selectedEvent.id}</code>
              </div>
              <div className="detail-row">
                <strong>State:</strong>{' '}
                <span
                  className="event-state"
                  style={{ backgroundColor: getStateColor(selectedEvent.state) }}
                >
                  {selectedEvent.state}
                </span>
              </div>
              <div className="detail-row">
                <strong>Delegation Type:</strong> {selectedEvent.delegation_type}
              </div>
              <div className="detail-row">
                <strong>Attempts:</strong> {selectedEvent.attempts}/{selectedEvent.max_attempts}
              </div>
              <div className="detail-row">
                <strong>Priority:</strong> {selectedEvent.priority}
              </div>
              <div className="detail-row">
                <strong>Created:</strong> {formatDate(selectedEvent.created_at)}
              </div>
              <div className="detail-row">
                <strong>Updated:</strong> {formatDate(selectedEvent.updated_at)}
              </div>
              {selectedEvent.completed_at && (
                <div className="detail-row">
                  <strong>Completed:</strong> {formatDate(selectedEvent.completed_at)}
                </div>
              )}

              <div className="detail-section">
                <h4>Payload</h4>
                <pre className="code-block">{selectedEvent.payload || 'N/A'}</pre>
              </div>

              {selectedEvent.result && (
                <div className="detail-section">
                  <h4>Result</h4>
                  <pre className="code-block">{selectedEvent.result}</pre>
                </div>
              )}

              {selectedEvent.error && (
                <div className="detail-section error-section">
                  <h4>Error</h4>
                  <pre className="code-block error">{selectedEvent.error}</pre>
                </div>
              )}
            </div>
          )}
        </div>

        <div className="section task-states-section">
          <div className="task-states-header">
            <h2>Task State Stream</h2>
            <div className="stream-status">
              <span className={`status-indicator ${streamConnected ? 'connected' : 'disconnected'}`}>
                {streamConnected ? '🍀 Connected' : '🐦‍🔥 Disconnected'}
              </span>
              <button 
                onClick={() => setTaskStates([])} 
                className="btn btn-secondary btn-small"
                disabled={taskStates.length === 0}
              >
                Clear
              </button>
            </div>
          </div>

          {taskStates.length === 0 ? (
            <div className="empty-state">
              {streamConnected ? 'Waiting for task states...' : 'No task states yet'}
            </div>
          ) : (
            <div className="task-states-list">
              {taskStates.map((taskState, index) => {
                const state = (taskState as any).state;
                const eventId = (taskState as any).event_id;
                return (
                <div key={index} className="task-state-card">
                  <div className="task-state-header">
                    <span className="task-state-index">#{taskStates.length - index}</span>
                    {state && (
                      <span 
                        className="task-state-badge"
                        style={{ backgroundColor: getStateColor(String(state)) }}
                      >
                        {String(state)}
                      </span>
                    )}
                    {eventId && (
                      <span className="task-state-id">
                        Event: {String(eventId).slice(-8)}
                      </span>
                    )}
                  </div>
                  <pre className="task-state-content">
                    {JSON.stringify(taskState, null, 2)}
                  </pre>
                </div>
              );
              })}
            </div>
          )}
        </div>

        {message && (
          <div className={`message ${message.includes('Error') ? 'error' : 'success'}`}>
            {message}
          </div>
        )}

      </div>
    </div>
  );
}

export default App;