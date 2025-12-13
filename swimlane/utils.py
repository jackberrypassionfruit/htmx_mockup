from threading import Lock

class DataFrameSessionStore:
    def __init__(self):
        self._store = {}
        self._lock = Lock()
    
    def get(self, session_key, df_name):
        with self._lock:
            return self._store.get(session_key)[df_name] 
    
    def set(self, session_key, df_name, dataframe):
        with self._lock:
            self._store[session_key][df_name] = dataframe
    
    def delete(self, session_key):
        with self._lock:
            self._store.pop(session_key, None)
            
            
# Global instance
df_store = DataFrameSessionStore()