package electric4k
internal actual fun platformRuntimeVisibility(): RuntimeVisibilityAdapter? {
    if (js("typeof document === 'undefined'") as Boolean) return null
    return object : RuntimeVisibilityAdapter {
        override fun isVisible(): Boolean = js("document.visibilityState !== 'hidden'") as Boolean
        override fun subscribe(callback: (Boolean) -> Unit): () -> Unit {
            val listener: (dynamic) -> Unit = { callback(isVisible()) }
            js("document.addEventListener('visibilitychange', listener)")
            return { js("document.removeEventListener('visibilitychange', listener)"); Unit }
        }
    }
}
