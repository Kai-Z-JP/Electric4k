package electric4k
internal actual fun platformProtocolCachePersistence(): ProtocolCachePersistence? {
    if (js("typeof globalThis.localStorage === 'undefined'") as Boolean) return null
    return object : ProtocolCachePersistence {
        override fun read(): String? = js("globalThis.localStorage.getItem('electric4k_protocol_cache')") as String?
        override fun write(value: String) { js("globalThis.localStorage.setItem('electric4k_protocol_cache', value)") }
    }
}
