import { useState, useRef, useEffect, useCallback, useMemo } from 'react'
import { useChatContext } from '../hooks/useChatContext'
import { formatToolsForProvider, toGeminiContents, appendToolCallToHistory, extractGeminiToolCall } from '../tools/definitions'
import { executeToolCall } from '../tools/executor'

const THEME = {
  bg:       '#09090b',
  surface:  '#18181b',
  border:   'rgba(255,255,255,0.08)',
  text:     '#fafafa',
  muted:    '#a1a1aa',
  red:      '#e10600',
  inputBg:  '#27272a',
  userBg:   '#1b2540',
  green:    '#22c55e',
}

// Provider configuration
const PROVIDERS = {
  gemini: {
    id:          'gemini',
    name:        'Google Gemini',
    badge:       'Free',
    model:       'gemini-2.5-flash',
    keyPrefix:   'AIza',
    placeholder: 'AIza...',
    storageKey:  'f1_chat_gemini_key',
    linkText:    'aistudio.google.com',
    description: 'Free tier via Google AI Studio (aistudio.google.com) — no credit card required. Must use an AI Studio key, not a Google Cloud key.',
    enabled:     true,
  },
  anthropic: {
    id:          'anthropic',
    name:        'Claude (Anthropic)',
    badge:       null,
    model:       'claude-sonnet-4-6',
    keyPrefix:   'sk-',
    placeholder: 'sk-ant-...',
    storageKey:  'f1_chat_api_key',
    linkText:    'console.anthropic.com',
    description: 'Claude Sonnet 4.6. Set a monthly spending limit in your Anthropic account.',
    enabled:     false,  // hidden until Cloudflare Worker proxy is deployed for production
  },
}

const PROVIDER_STORAGE = 'f1_chat_provider'
// Dev: Vite proxies /anthropic/* → https://api.anthropic.com/* (bypasses CORS)
// Production: set VITE_ANTHROPIC_PROXY_URL to your Cloudflare Worker base URL
const ANTHROPIC_BASE   = import.meta.env.VITE_ANTHROPIC_PROXY_URL ?? '/anthropic'
// Gemini supports CORS directly from browsers — no proxy needed
const GEMINI_BASE      = 'https://generativelanguage.googleapis.com/v1beta/models'

const CURRENT_YEAR = new Date().getFullYear()
const YEARS        = [CURRENT_YEAR, CURRENT_YEAR - 1, CURRENT_YEAR - 2].filter(y => y >= 2023)

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

function ApiKeySetup({ provider, onSave }) {
  const cfg            = PROVIDERS[provider]
  const [keyInput, setKeyInput] = useState('')
  const [show, setShow]         = useState(false)

  // Reset input when provider changes
  useEffect(() => { setKeyInput(''); setShow(false) }, [provider])

  const isValid = keyInput.trim().startsWith(cfg.keyPrefix)

  function handleSave() {
    if (!isValid) return
    onSave(keyInput.trim())
  }

  return (
    <div style={{
      maxWidth: 480,
      margin: '3rem auto',
      display: 'flex',
      flexDirection: 'column',
      gap: '1rem',
    }}>
      {/* Key entry */}
      <div style={{
        background: THEME.surface,
        border: `1px solid ${THEME.border}`,
        borderRadius: '10px',
        padding: '1.5rem',
        display: 'flex',
        flexDirection: 'column',
        gap: '1rem',
      }}>
        <div>
          <p style={{ margin: 0, fontWeight: 'bold', fontSize: '1rem', color: THEME.text }}>
            Set up your {cfg.name} API key
          </p>
          <p style={{ margin: '0.5rem 0 0', fontSize: '0.8rem', color: THEME.muted, lineHeight: 1.6 }}>
            {cfg.description}{' '}
            Your key is stored only in your browser — it never passes through any server we control.{' '}
            Get a key at <span style={{ color: THEME.text }}>{cfg.linkText}</span>.
          </p>
        </div>

        <div style={{ display: 'flex', gap: '0.5rem', alignItems: 'center' }}>
          <div style={{ position: 'relative', flex: 1 }}>
            <input
              type={show ? 'text' : 'password'}
              placeholder={cfg.placeholder}
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
            disabled={!isValid}
            style={{
              background: isValid ? THEME.red : THEME.inputBg,
              color: isValid ? '#fff' : THEME.muted,
              border: 'none',
              borderRadius: '6px',
              padding: '0.45rem 1rem',
              fontSize: '0.875rem',
              cursor: isValid ? 'pointer' : 'not-allowed',
              opacity: isValid ? 1 : 0.4,
              whiteSpace: 'nowrap',
            }}
          >
            Save key
          </button>
        </div>
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
        borderLeft: isUser ? `1px solid ${THEME.border}` : '2px solid rgba(225, 6, 0, 0.5)',
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

function ThinkingDots({ label = 'Thinking…' }) {
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
        {label}
      </div>
    </div>
  )
}

// Send via Anthropic Messages API (requires CORS proxy).
// Returns null when done (text was streamed via onChunk), or { id, name, args } when the model wants a tool call.
async function sendWithAnthropic(history, context, apiKey, tools, onChunk, onUsage) {
  const response = await fetch(`${ANTHROPIC_BASE}/v1/messages`, {
    method: 'POST',
    headers: {
      'x-api-key':         apiKey,
      'anthropic-version': '2023-06-01',
      'content-type':      'application/json',
    },
    body: JSON.stringify({
      model:      PROVIDERS.anthropic.model,
      max_tokens: 1024,
      stream:     true,
      system:     context,
      messages:   history,
      ...(tools?.length ? { tools, tool_choice: { type: 'auto' } } : {}),
    }),
  })

  if (!response.ok) {
    const err = await response.json().catch(() => ({}))
    const msg = err.error?.message ?? `API error ${response.status}`
    if (response.status === 401) throw new Error('Invalid API key — check your key and try again.')
    if (response.status === 429) throw new Error('Rate limit reached. Wait a moment and try again.')
    throw new Error(msg)
  }

  const reader  = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer     = ''
  let toolUse    = null // { id, name, input: '' } while accumulating a tool_use block
  let inputTokens  = 0
  let outputTokens = 0

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
        const p = JSON.parse(data)
        if (p.type === 'message_start') {
          inputTokens = p.message?.usage?.input_tokens ?? 0
        } else if (p.type === 'message_delta' && p.usage) {
          outputTokens = p.usage.output_tokens ?? 0
        } else if (p.type === 'content_block_start' && p.content_block?.type === 'tool_use') {
          toolUse = { id: p.content_block.id, name: p.content_block.name, input: '' }
        } else if (p.type === 'content_block_delta' && p.delta?.type === 'input_json_delta' && toolUse) {
          toolUse.input += p.delta.partial_json ?? ''
        } else if (p.type === 'message_delta' && p.delta?.stop_reason === 'tool_use' && toolUse) {
          onUsage?.(inputTokens, outputTokens)
          return { id: toolUse.id, name: toolUse.name, args: JSON.parse(toolUse.input || '{}') }
        } else if (p.type === 'content_block_delta' && p.delta?.type === 'text_delta' && !toolUse) {
          onChunk(p.delta.text)
        }
      } catch { /* ignore malformed SSE lines */ }
    }
  }
  onUsage?.(inputTokens, outputTokens)
  return null
}

// Send via Gemini streaming API (direct browser call — no proxy needed, CORS supported).
// Returns null when done (text was streamed via onChunk), or { id, name, args } when the model wants a tool call.
async function sendWithGemini(history, context, apiKey, tools, onChunk, onUsage) {
  const response = await fetch(
    `${GEMINI_BASE}/${PROVIDERS.gemini.model}:streamGenerateContent?key=${encodeURIComponent(apiKey)}&alt=sse`,
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        systemInstruction: { parts: [{ text: context }] },
        contents:          history,  // already Gemini-formatted by caller
        generationConfig:  { maxOutputTokens: 1024 },
        ...(tools ? { tools } : {}),
      }),
    }
  )

  if (!response.ok) {
    const err = await response.json().catch(() => ({}))
    const msg = err.error?.message ?? `API error ${response.status}`
    if (response.status === 401 || response.status === 403) throw new Error(`Invalid API key — ${msg}`)
    throw new Error(msg)
  }

  const reader  = response.body.getReader()
  const decoder = new TextDecoder()
  let buffer          = ''
  let toolCall        = null
  let promptTokens    = 0
  let candidateTokens = 0

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
        if (parsed.error) throw new Error(parsed.error.message ?? `Stream error ${parsed.error.code}`)
        if (parsed.usageMetadata) {
          promptTokens    = parsed.usageMetadata.promptTokenCount    ?? promptTokens
          candidateTokens = parsed.usageMetadata.candidatesTokenCount ?? candidateTokens
        }
        const tc = extractGeminiToolCall(parsed)
        if (tc) { toolCall = tc; continue }
        const text = parsed.candidates?.[0]?.content?.parts?.[0]?.text
        if (text) onChunk(text)
      } catch (e) { if (e.message) throw e }
    }
  }
  onUsage?.(promptTokens, candidateTokens)
  return toolCall
}

export default function ChatPage({ isMobile }) {
  const [year, setYear]           = useState(CURRENT_YEAR)
  const [provider, setProvider]   = useState(() => {
    const stored = localStorage.getItem(PROVIDER_STORAGE)
    // Fall back to 'gemini' if stored provider is no longer enabled
    return (stored && PROVIDERS[stored]?.enabled) ? stored : 'gemini'
  })
  const [geminiKey, setGeminiKey] = useState(() => localStorage.getItem(PROVIDERS.gemini.storageKey) ?? '')
  const [anthropicKey, setAnthropicKey] = useState(() => localStorage.getItem(PROVIDERS.anthropic.storageKey) ?? '')
  const [messages, setMessages]     = useState([])
  const [input, setInput]           = useState('')
  const [sending, setSending]       = useState(false)
  const [toolStatus, setToolStatus] = useState(null)
  const [apiError, setApiError]     = useState(null)
  const [tokenUsage, setTokenUsage] = useState({ input: 0, output: 0 })
  const messagesEndRef               = useRef(null)
  const textareaRef                  = useRef(null)

  const tools = useMemo(() => formatToolsForProvider(provider), [provider])

  const { context, loading: contextLoading, error: contextError } = useChatContext(year)

  const cfg    = PROVIDERS[provider]
  const apiKey = provider === 'gemini' ? geminiKey : anthropicKey

  // Clear conversation when year or provider changes
  useEffect(() => { setMessages([]); setTokenUsage({ input: 0, output: 0 }) }, [year, provider])

  // Scroll to bottom when messages update
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages])

  function saveProvider(id) {
    localStorage.setItem(PROVIDER_STORAGE, id)
    setProvider(id)
  }

  function saveApiKey(key) {
    localStorage.setItem(cfg.storageKey, key)
    if (provider === 'gemini') setGeminiKey(key)
    else setAnthropicKey(key)
  }

  function clearApiKey() {
    localStorage.removeItem(cfg.storageKey)
    if (provider === 'gemini') setGeminiKey('')
    else setAnthropicKey('')
    setMessages([])
    setApiError(null)
    setTokenUsage({ input: 0, output: 0 })
  }

  const sendMessage = useCallback(async (text) => {
    const trimmed = (text ?? input).trim()
    if (!trimmed || sending || !apiKey || !context) return

    setInput('')
    setApiError(null)
    setSending(true)
    setToolStatus(null)

    const userMsg    = { role: 'user', content: trimmed }
    const newHistory = [...messages, userMsg]
    setMessages(newHistory)

    // Provider-formatted request history (separate from display messages)
    let reqHistory = provider === 'gemini'
      ? toGeminiContents(newHistory)
      : newHistory

    const onChunk = (chunk) => {
      setMessages(prev => {
        const updated = [...prev]
        const last    = updated[updated.length - 1]
        updated[updated.length - 1] = { ...last, content: last.content + chunk }
        return updated
      })
    }

    const onUsage = (inp, out) => {
      setTokenUsage(prev => ({ input: prev.input + inp, output: prev.output + out }))
    }

    try {
      for (let i = 0; i < 5; i++) {  // max 5 tool calls per turn
        // Add empty assistant placeholder to stream into
        setMessages(prev => [...prev, { role: 'assistant', content: '' }])

        const toolCall = provider === 'gemini'
          ? await sendWithGemini(reqHistory, context, apiKey, tools, onChunk, onUsage)
          : await sendWithAnthropic(reqHistory, context, apiKey, tools, onChunk, onUsage)

        if (!toolCall) break  // text was streamed — done

        // Tool call: remove empty placeholder, execute tool, loop
        setMessages(prev => {
          const last = prev[prev.length - 1]
          return last?.role === 'assistant' && !last.content ? prev.slice(0, -1) : prev
        })

        setToolStatus('Looking up data…')
        const result = await executeToolCall(toolCall.name, toolCall.args, year)
        setToolStatus(null)

        reqHistory = appendToolCallToHistory(reqHistory, toolCall, result, provider)
      }
    } catch (err) {
      setApiError(err.message)
      setMessages(prev => {
        const last = prev[prev.length - 1]
        return last?.role === 'assistant' && !last.content ? prev.slice(0, -1) : prev
      })
    } finally {
      setSending(false)
      setToolStatus(null)
      textareaRef.current?.focus()
    }
  }, [input, sending, apiKey, context, messages, provider, tools, year])

  function handleKeyDown(e) {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault()
      sendMessage()
    }
  }

  const canSend  = !!apiKey && !!context && !sending && !!input.trim()
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

          <span style={{ fontSize: '0.75rem', color: contextLoading ? THEME.muted : (contextError ? '#ef4444' : THEME.green) }}>
            {contextLoading ? '⟳ Loading data…' : contextError ? '✕ Data error' : '✓ Data ready'}
          </span>
        </div>

        <div style={{ display: 'flex', alignItems: 'center', gap: '0.4rem' }}>
          {Object.values(PROVIDERS).filter(p => p.enabled).map(p => (
            <button
              key={p.id}
              onClick={() => saveProvider(p.id)}
              style={{
                background: provider === p.id ? THEME.red : 'transparent',
                border: `1px solid ${provider === p.id ? THEME.red : THEME.border}`,
                borderRadius: '4px',
                color: provider === p.id ? '#fff' : THEME.muted,
                padding: '0.2rem 0.45rem',
                fontSize: '0.7rem',
                cursor: 'pointer',
                lineHeight: 1.4,
              }}
            >
              {p.name}
              {p.badge && (
                <span style={{ marginLeft: '0.25rem', color: provider === p.id ? 'rgba(255,255,255,0.75)' : THEME.green }}>
                  ({p.badge})
                </span>
              )}
            </button>
          ))}
          {apiKey && (
            <button
              onClick={clearApiKey}
              style={{ background: 'none', border: 'none', color: THEME.muted, cursor: 'pointer', fontSize: '0.7rem', padding: '0 0.1rem', textDecoration: 'underline' }}
            >
              remove key
            </button>
          )}
        </div>
      </div>

      {/* Message thread */}
      <div style={{
        flex: 1,
        overflowY: 'auto',
        padding: isMobile ? '0.75rem 0.5rem' : '1rem 0.75rem',
      }}>
        {showSetup ? (
          <ApiKeySetup
            provider={provider}
            onSave={saveApiKey}
          />
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
            {sending && messages[messages.length - 1]?.role === 'user' && !toolStatus && <ThinkingDots />}
            {toolStatus && <ThinkingDots label={toolStatus} />}
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

      {/* Footer bar: clear + token usage */}
      {!showSetup && messages.length > 0 && (
        <div style={{
          flexShrink: 0,
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'space-between',
          padding: '0.25rem 0.75rem',
          borderTop: `1px solid ${THEME.border}`,
          background: THEME.bg,
        }}>
          <button
            onClick={() => { setMessages([]); setApiError(null); setTokenUsage({ input: 0, output: 0 }) }}
            style={{
              background: 'none',
              border: 'none',
              color: THEME.muted,
              cursor: 'pointer',
              fontSize: '0.72rem',
              textDecoration: 'underline',
              padding: 0,
            }}
          >
            Clear conversation
          </button>
          {(tokenUsage.input > 0 || tokenUsage.output > 0) && (
            <span style={{ fontSize: '0.7rem', color: THEME.muted, fontVariantNumeric: 'tabular-nums' }}>
              {tokenUsage.input.toLocaleString()} in · {tokenUsage.output.toLocaleString()} out · {(tokenUsage.input + tokenUsage.output).toLocaleString()} total tokens
            </span>
          )}
        </div>
      )}
    </div>
  )
}
