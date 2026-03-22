import { useEffect, useRef, useState } from 'react'

/**
 * Returns [ref, inView] where inView becomes true once the element enters
 * the viewport and stays true permanently (observer disconnects after first hit).
 * rootMargin pre-loads content before it's fully visible.
 */
export function useInView(rootMargin = '150px 0px') {
  const ref = useRef(null)
  const [inView, setInView] = useState(false)

  useEffect(() => {
    const el = ref.current
    if (!el) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setInView(true)
          observer.disconnect()
        }
      },
      { rootMargin },
    )
    observer.observe(el)
    return () => observer.disconnect()
  }, [rootMargin])

  return [ref, inView]
}
