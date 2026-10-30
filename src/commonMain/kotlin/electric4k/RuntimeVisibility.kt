package electric4k

interface RuntimeVisibilityAdapter {
    fun isVisible(): Boolean = true
    fun subscribe(callback: (Boolean) -> Unit): () -> Unit
}
internal expect fun platformRuntimeVisibility(): RuntimeVisibilityAdapter?
