import { useInView } from '../hooks/useInView'

/**
 * Defers rendering children until the section scrolls near the viewport.
 * Reserves minHeight until content loads to prevent scroll position jumps.
 */
export default function LazySection({ children, minHeight = 300 }) {
  const [ref, inView] = useInView()
  return (
    <div ref={ref} style={{ minHeight: inView ? undefined : minHeight }}>
      {inView ? children : null}
    </div>
  )
}
