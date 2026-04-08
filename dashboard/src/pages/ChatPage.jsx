import { useState, useRef, useEffect, useCallback } from 'react'
import { useChatContext } from '../hooks/useChatContext'

const THEME = {
  bg:       '#09090b',
  surface:  '#18181b',
  border:   'rgba(255,255,255,0.08)',
  text:     '#fafafa',
  muted:    '#a1a1aa',
  red:      '#e10600',
  inputBg:  '#27272a',
  userBg:   '#1c1c20',
}

const API_KEY_STORAGE = 'f1_chat_api_key'
const MODEL           = 'claude-sonnet-4-6'
// Dev: Vite proxies /anthropic/* → https://api.anthropic.com/* (no CORS)
// Production: set VITE_ANTHROPIC_PROXY_URL to your Cloudflare Worker base URL
const ANTHROPIC_BASE  = import.meta.env.VITE_ANTHROPIC_PROXY_URL ?? '/anthropic'
const CURRENT_YEAR    = new Date().getFullYear()
const YEARS           = [CURRENT_YEAR, CURRENT_YEAR - 1, CURRENT_YEAR - 2].filter(y => y >= 2023)

const EXAMPLE_QUESTIONS = [
  'Who had the fastest S3 time in the Chinese GP qualifying?',
  'How many laps has Mercedes led this season?',
  'Which driver has the highest average peak braking G-force?',
  'How many DNFs has Red Bull had this year?',
  'Who is leading the championship, and by how many points?',
]

function makeSelectStyle(isMobile) {
  return {
    background: THEME.inputBg,
    color: THEME.text,
    border: `1px solid ${THEME.border}`,
    borderRadius: '6px',
    padding: '0.3rem 0.5rem',
    fontSize: isMobile ? '16px' : '0.875rem',
    cursor: 'pointer',
    outline: 'none',
  }
}

// Minimal markdown → React: handles **bold**, *italic*, `code`, and leading - list items
function renderMarkdown(text) {
  const lines = text.split('\n')
  const elements = []
  let i = 0

  while (i < lines.length) {
    const line = lines[i]

    // Heading
    if (line.startsWith('### ')) {
      elements.push(<p key={i} style={{ margin: '0.75rem 0 0.25rem', fontWeight: 'bold', color: THEME.text }}>{inlineMarkdown(line.slice(4))}</p>)
    } else if (line.startsWith('## ')) {
      elements.push(<p key={i} style={{ margin: '0.75rem 0 0.25rem', fontWeight: 'bold', color: THEME.text }}>{inlineMarkdown(line.slice(3))}</p>)
    // List item
    } else if (line.match(/^[-*] /)) {
      elements.push(
        <div key={i} style={{ display: 'flex', gap: '0.5rem', margin: '0.15rem 0' }}>
          <span style={{ color: THEME.muted, flexShrink: 0 }}>•</span>
          <span>{inlineMarkdown(line.slice(2))}</span>
        </div>
      )
    // Blank line
    } else if (line.trim() === '') {
      elements.push(<div key={i} style={{ height: '0.5rem' }} />)
    // Normal line
    } else {
      elements.push(<p key={i} style={{ margin: '0.15rem 0' }}>{inlineMarkdown(line)}</p>)
    }
    i++
  }
  return elements
}

function inlineMarkdown(text) {
  // Split on **bold**, *italic*, `code`
  const parts = []
  const regex = /(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`)/g
  let last = 0
  let match

  while ((match = regex.exec(text)) !== null) {
    if (match.index > last) parts.push(text.slice(last, match.index))
    const raw = match[0]
    if (raw.startsWith('**')) {
      parts.push(<strong key={match.index}>{raw.slice(2, -2)}</strong>)
    } else if (raw.startsWith('*')) {
      parts.push(<em key={match.index}>{raw.slice(1, -1)}</em>)
    } else if (raw.startsWith('`')) {
      parts.push(
        <code key={match.index} style={{ background: THEME.inputBg, padding: '0.1em 0.3em', borderRadius: '3px', fontSize: '0.85em' }}>
          {raw.slice(1, -1)}
        </code>
      )
    }
    last = match.index + raw.length
  }
  if (last < text.length) parts.push(text.slice(last))
  return parts.length === 1 && typeof parts[0] === 'string' ? parts[0] : parts
}

function ApiKeySetup({ onSave }) {
  const [keyInput, setKeyInput] = useState('')
  const [show, setShow]         = useState(false)

  function handleSave() {
    const trimmed = keyInput.trim()
    if (!trimmed.startsWith('sk-')) return
    onSave(trimmed)
  }

  return (
    <div style={{
      maxWidth: 480,
      margin: '3rem auto',
      background: THEME.surface,
      border: `1px solid ${THEME.border}`,
      borderRadius: '10px',
      padding: '1.75rem',
      display: 'flex',
      flexDirection: 'column',
      gap: '1rem',
    }}>
      <div>
        <p style={{ margin: 0, fontWeight: 'bold', fontSize: '1rem', color: THEME.text }}>Set up your Anthropic API key</p>
        <p style={{ margin: '0.5rem 0 0', fontSize: '0.8rem', color: THEME.muted, lineHeight: 1.6 }}>
          Your key is stored only in your browser and sent directly to Anthropic — it never passes through any server we control.
          Get a key at <span style={{ color: THEME.text }}>console.anthropic.com</span>. Consider setting a spending limit in your Anthropic account.
        </p>
      </div>

      <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
        <div style={{ position: 'relative', flex: 1 }}>
          <input
            type={show ? 'text' : 'password'}
            placeholder="sk-ant-..."
            value={keyInput}
            onChange={e => setKeyInput(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && handleSave()}
            style={{
              width: '100%',
              background: THEME.inputBg,
              border: `1px solid ${THEME.border}`,
              borderRadius: '6px',
              color: THEME.text,
              padding: '0.45rem 2.5rem 0.45rem 0.6rem',
              fontSize: '0.875rem',
              outline: 'none',
              boxSizing: 'border-box',
            }}
          />
          <button
            onClick={() => setShow(s => !s)}
            style={{
              position: 'absolute',
              right: 6,
              top: '50%',
              transform: 'translateY(-50%)',
              background: 'none',
              border: 'none',
              color: THEME.muted,
              cursor: 'pointer',
              fontSize: '0.75rem',
              padding: 0,
            }}
          >
            {show ? 'hide' : 'show'}
          </button>
        </div>
        <button
          onClick={handleSave}
          disabled={!keyInput.trim().startsWith('sk-')}
          style={{
            background: THEME.red,
            color: '#fff',
            border: 'none',
            borderRadius: '6px',
            padding: '0.45rem 1rem',
            fontSize: '0.875rem',
            cursor: keyInput.trim().startsWith('sk-') ? 'pointer' : 'not-allowed',
            opacity: keyInput.trim().startsWith('sk-') ? 1 : 0.4,
            whiteSpace: 'nowrap',
          }}
        >
          Save key
        </button>
      </div>
    </div>
  )
}

function MessageBubble({ msg }) {
  const isUser = msg.role === 'user'

  return (
    <div style={{
      display: 'flex',
      justifyContent: isUser ? 'flex-end' : 'flex-start',
      marginBottom: '0.75rem',
    }}>
      <div style={{
        maxWidth: '80%',
        background: isUser ? THEME.userBg : THEME.surface,
        border: `1px solid ${THEME.border}`,
        borderRadius: isUser ? '12px 12px 2px 12px' : '12px 12px 12px 2px',
        padding: '0.65rem 0.9rem',
        fontSize: '0.875rem',
        color: THEME.text,
        lineHeight: 1.6,
      }}>
        {isUser
          ? <span style={{ whiteSpace: 'pre-wrap' }}>{msg.content}</span>
          : <div>{msg.content ? renderMarkdown(msg.content) : <span style={{ color: THEME.muted }}>…</span>}</div>
        }
      </div>
    </div>
  )
}

function ThinkingDots() {
  return (
    <div style={{ display: 'flex', justifyContent: 'flex-start', marginBottom: '0.75rem' }}>
      <div style={{
        background: THEME.surface,
        border: `1px solid ${THEME.border}`,
        borderRadius: '12px 12px 12px 2px',
        padding: '0.65rem 1rem',
        color: THEME.muted,
        fontSize: '0.875rem',
      }}>
        Thinking…
      </div>
    </div>
  )
}

export default function ChatPage({ isMobile }) {
  const [year, setYear]         = useState(CURRENT_YEAR)
  const [apiKey, setApiKey]     = useState(() => localStorage.getItem(API_KEY_STORAGE) ?? '')
  const [messages, setMessages] = useState([])
  const [input, setInput]       = useState('')
  const [sending, setSending]   = useState(false)
  const [apiError, setApiError] = useState(null)
  const messagesEndRef           = useRef(null)
  const textareaRef              = useRef(null)

  const { context, loading: contextLoading, error: contextError } = useChatContext(year)

  // Clear conversation when year changes
  useEffect(() => { setMessages([]) }, [year])

  // Scroll to bottom when messages update
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  function saveApiKey(key) {
    localStorage.setItem(API_KEY_STORAGE, key)
    setApiKey(key)
  }

  function clearApiKey() {
    localStorage.removeItem(API_KEY_STORAGE)
    setApiKey('')
    setMessages([])
    setApiError(null)
  }

  const sendMessage = useCallback(async (text) => {
    const trimmed = (text ?? input).trim()
    if (!trimmed || sending || !apiKey || !context) return

    setInput('')
    setApiError(null)
    setSending(true)

    const userMsg    = { role: 'user', content: trimmed }
    const newHistory = [...messages, userMsg]
    setMessages(newHistory)

    try {
      const response = await fetch(`${ANTHROPIC_BASE}/v1/messages`, {
        method: 'POST',
        headers: {
          'x-api-key':         apiKey,
          'anthropic-version': '2023-06-01',
          'content-type':      'application/json',
        },
        body: JSON.stringify({
          model:      MODEL,
          max_tokens: 1024,
          stream:     true,
          system:     context,
          messages:   newHistory,
        }),
      })

      if (!response.ok) {
        const err = await response.json().catch(() => ({}))
        const msg = err.error?.message ?? `API error ${response.status}`
        if (response.status === 401) throw new Error('Invalid API key — check your key and try again.')
        if (response.status === 429) throw new Error('Rate limit reached. Wait a moment and try again.')
        throw new Error(msg)
      }

      // Add an empty assistant message to stream into
      setMessages(prev => [...prev, { role: 'assistant', content: '' }])

      const reader  = response.body.getReader()
      const decoder = new TextDecoder()
      let buffer      = ''
      let accumulated = ''

      while (true) {
        const { done, value } = await reader.read()
        if (done) break

        buffer += decoder.decode(value, { stream: true })
        const lines = buffer.split('\n')
        buffer = lines.pop() ?? ''

        for (const line of lines) {
          if (!line.startsWith('data: ')) continue
          const data = line.slice(6).trim()
          if (!data) continue
          try {
            const parsed = JSON.parse(data)
            if (parsed.type === 'content_block_delta' && parsed.delta?.type === 'text_delta') {
              accumulated += parsed.delta.text
              const snapshot = accumulated
              setMessages(prev => {
                const updated = [...prev]
                updated[updated.length - 1] = { role: 'assistant', content: snapshot }
                return updated
              })
            }
          } catch { /* ignore malformed SSE lines */ }
        }
      }
    } catch (err) {
      setApiError(err.message)
      // Remove the empty assistant placeholder if we errored before any content arrived
      setMessages(prev => {
        const last = prev[prev.length - 1]
        return (last?.role === 'assistant' && !last.content) ? prev.slice(0, -1) : prev
      })
    } finally {
      setSending(false)
      textareaRef.current?.focus()
    }
  }, [input, sending, apiKey, context, messages])

  function handleKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const canSend = !!apiKey && !!context && !sending && !!input.trim()
  const showSetup = !apiKey

  return (
    <div style={{
      display: 'flex',
      flexDirection: 'column',
      height: 'calc(100vh - 57px)', // subtract top nav height
      maxWidth: 820,
      margin: '0 auto',
      padding: isMobile ? '0 0.25rem' : '0',
    }}>

      {/* Sub-header: year selector + context status + key management */}
      <div style={{
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        flexWrap: 'wrap',
        gap: '0.5rem',
        padding: '0.6rem 0.75rem',
        borderBottom: `1px solid ${THEME.border}`,
        background: THEME.surface,
        flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
          <label style={{ display: 'flex', alignItems: 'center', gap: '0.4rem', color: THEME.muted, fontSize: '0.75rem' }}>
            <span>SEASON</span>
            <select
              value={year}
              onChange={e => setYear(Number(e.target.value))}
              style={makeSelectStyle(isMobile)}
            >
              {YEARS.map(y => <option key={y} value={y}>{y}</option>)}
            </select>
          </label>

          <span style={{ fontSize: '0.75rem', color: contextLoading ? THEME.muted : (contextError ? '#ef4444' : '#22c55e') }}>
            {contextLoading ? '⟳ Loading data…' : contextError ? '✕ Data error' : '✓ Data ready'}
          </span>
        </div>

        {apiKey && (
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.75rem', color: THEME.muted }}>
            <span>🔑 API key set</span>
            <button
              onClick={clearApiKey}
              style={{ background: 'none', border: 'none', color: THEME.muted, cursor: 'pointer', fontSize: '0.75rem', padding: 0, textDecoration: 'underline' }}
            >
              remove
            </button>
          </div>
        )}
      </div>

      {/* Message thread */}
      <div style={{
        flex: 1,
        overflowY: 'auto',
        padding: isMobile ? '0.75rem 0.5rem' : '1rem 0.75rem',
      }}>
        {showSetup ? (
          <ApiKeySetup onSave={saveApiKey} />
        ) : messages.length === 0 ? (
          /* Empty state */
          <div style={{ maxWidth: 480, margin: '2rem auto', color: THEME.muted }}>
            <p style={{ fontSize: '0.875rem', marginBottom: '1rem' }}>
              Ask anything about the {year} F1 season. Try:
            </p>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '0.4rem' }}>
              {EXAMPLE_QUESTIONS.map((q, i) => (
                <button
                  key={i}
                  onClick={() => sendMessage(q)}
                  disabled={!context || sending}
                  style={{
                    background: THEME.surface,
                    border: `1px solid ${THEME.border}`,
                    borderRadius: '6px',
                    color: context ? THEME.text : THEME.muted,
                    padding: '0.5rem 0.75rem',
                    fontSize: '0.8rem',
                    cursor: context && !sending ? 'pointer' : 'default',
                    textAlign: 'left',
                    opacity: context ? 1 : 0.5,
                  }}
                >
                  {q}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <>
            {messages.map((msg, i) => <MessageBubble key={i} msg={msg} />)}
            {sending && messages[messages.length - 1]?.role === 'user' && <ThinkingDots />}
          </>
        )}

        {apiError && (
          <div style={{
            background: 'rgba(239,68,68,0.1)',
            border: '1px solid rgba(239,68,68,0.3)',
            borderRadius: '6px',
            color: '#ef4444',
            padding: '0.6rem 0.9rem',
            fontSize: '0.8rem',
            margin: '0.5rem 0',
          }}>
            {apiError}
          </div>
        )}

        <div ref={messagesEndRef} />
      </div>

      {/* Input area */}
      {!showSetup && (
        <div style={{
          flexShrink: 0,
          borderTop: `1px solid ${THEME.border}`,
          padding: isMobile ? '0.5rem' : '0.75rem',
          background: THEME.bg,
          display: 'flex',
          gap: '0.5rem',
          alignItems: 'flex-end',
        }}>
          <textarea
            ref={textareaRef}
            rows={1}
            placeholder={context ? 'Ask a question… (Enter to send, Shift+Enter for newline)' : 'Loading data…'}
            value={input}
            onChange={e => {
              setInput(e.target.value)
              // Auto-grow up to ~5 rows
              e.target.style.height = 'auto'
              e.target.style.height = Math.min(e.target.scrollHeight, 120) + 'px'
            }}
            onKeyDown={handleKeyDown}
            disabled={!apiKey || !context || sending}
            style={{
              flex: 1,
              resize: 'none',
              background: THEME.inputBg,
              border: `1px solid ${THEME.border}`,
              borderRadius: '8px',
              color: THEME.text,
              padding: '0.55rem 0.75rem',
              fontSize: isMobile ? '16px' : '0.875rem',
              fontFamily: 'monospace',
              outline: 'none',
              lineHeight: 1.5,
              overflow: 'hidden',
            }}
          />
          <button
            onClick={() => sendMessage()}
            disabled={!canSend}
            style={{
              background: canSend ? THEME.red : THEME.inputBg,
              border: `1px solid ${canSend ? THEME.red : THEME.border}`,
              borderRadius: '8px',
              color: canSend ? '#fff' : THEME.muted,
              padding: '0.55rem 1.1rem',
              fontSize: '0.875rem',
              cursor: canSend ? 'pointer' : 'not-allowed',
              whiteSpace: 'nowrap',
              flexShrink: 0,
              fontFamily: 'monospace',
            }}
          >
            Send
          </button>
        </div>
      )}

      {/* Clear conversation link */}
      {!showSetup && messages.length > 0 && (
        <div style={{
          flexShrink: 0,
          textAlign: 'center',
          padding: '0.3rem',
          borderTop: `1px solid ${THEME.border}`,
          background: THEME.bg,
        }}>
          <button
            onClick={() => { setMessages([]); setApiError(null) }}
            style={{
              background: 'none',
              border: 'none',
              color: THEME.muted,
              cursor: 'pointer',
              fontSize: '0.72rem',
              textDecoration: 'underline',
            }}
          >
            Clear conversation
          </button>
        </div>
      )}
    </div>
  )
}
