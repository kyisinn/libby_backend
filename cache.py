# =============================================================================
# CACHE MODULE
# =============================================================================
# Handles caching and compute-intensive operations like similarity calculations

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from database import get_all_books_for_similarity

# =============================================================================
# IN-MEMORY CACHE
# =============================================================================

class BookCache:
    def __init__(self):
        self.similarity_cache = {}
        self.book_data_cache = None
        self.tfidf_matrix_cache = None
        self.book_df_cache = None
        
    def clear_cache(self):
        """Clear all cached data."""
        self.similarity_cache = {}
        self.book_data_cache = None
        self.tfidf_matrix_cache = None
        self.book_df_cache = None
        
    def get_cached_similarity(self, book_id):
        """Get cached similarity results for a book."""
        return self.similarity_cache.get(book_id)
        
    def set_cached_similarity(self, book_id, similar_books):
        """Cache similarity results for a book."""
        self.similarity_cache[book_id] = similar_books

# Global cache instance
book_cache = BookCache()

# =============================================================================
# SIMILARITY CALCULATIONS
# =============================================================================

def calculate_similar_books(book_id):
    """
    Calculate content-based recommendations using TF-IDF and Cosine Similarity.
    Uses caching to improve performance.
    """
    # Check if we have cached results
    cached_result = book_cache.get_cached_similarity(book_id)
    if cached_result:
        return cached_result
    
    # Get all books data (use cache if available)
    if book_cache.book_data_cache is None:
        all_books = get_all_books_for_similarity()
        if not all_books:
            return []
        book_cache.book_data_cache = all_books
    else:
        all_books = book_cache.book_data_cache

    # Convert to DataFrame (use cache if available)
    if book_cache.book_df_cache is None:
        df = pd.DataFrame(all_books)
        df['content'] = df['title'] + ' ' + df['genre']
        book_cache.book_df_cache = df
    else:
        df = book_cache.book_df_cache

    # Check if the target book_id exists
    if book_id not in df['book_id'].values:
        return None  # Book not found

    # Calculate TF-IDF matrix (use cache if available)
    if book_cache.tfidf_matrix_cache is None:
        # Note: stop_words='english' removes common words like 'the', 'a', etc.
        tfidf = TfidfVectorizer(stop_words='english')
        tfidf_matrix = tfidf.fit_transform(df['content'])
        book_cache.tfidf_matrix_cache = tfidf_matrix
    else:
        tfidf_matrix = book_cache.tfidf_matrix_cache

    book_index = df.index[df['book_id'] == book_id].tolist()[0]
    cosine_sim = cosine_similarity(tfidf_matrix[book_index], tfidf_matrix)
    
    sim_scores = list(enumerate(cosine_sim[0]))
    sim_scores = sorted(sim_scores, key=lambda x: x[1], reverse=True)
    sim_scores = sim_scores[1:11]  # Get top 10, excluding the book itself

    book_indices = [i[0] for i in sim_scores]
    similar_book_ids = df['book_id'].iloc[book_indices].tolist()
    
    # Cache the result
    book_cache.set_cached_similarity(book_id, similar_book_ids)
    
    return similar_book_ids

# =============================================================================
# CACHE MANAGEMENT
# =============================================================================

def refresh_similarity_cache():
    """Refresh the similarity calculation cache."""
    book_cache.clear_cache()
    return True

def get_cache_stats():
    """Get statistics about the current cache state."""
    return {
        'similarity_cache_size': len(book_cache.similarity_cache),
        'has_book_data_cache': book_cache.book_data_cache is not None,
        'has_tfidf_cache': book_cache.tfidf_matrix_cache is not None,
        'has_dataframe_cache': book_cache.book_df_cache is not None
    }

# =============================================================================
# SEARCH RESULT CACHING (Optional Enhancement)
# =============================================================================

class SearchCache:
    def __init__(self, max_size=100):
        self.cache = {}
        self.max_size = max_size
        self.access_order = []
    
    def get(self, key):
        """Get cached search results."""
        if key in self.cache:
            # Move to end (most recently used)
            self.access_order.remove(key)
            self.access_order.append(key)
            return self.cache[key]
        return None
    
    def set(self, key, value):
        """Cache search results with LRU eviction."""
        if key in self.cache:
            self.access_order.remove(key)
        elif len(self.cache) >= self.max_size:
            # Remove least recently used
            oldest = self.access_order.pop(0)
            del self.cache[oldest]
        
        self.cache[key] = value
        self.access_order.append(key)
    
    def clear(self):
        """Clear search cache."""
        self.cache = {}
        self.access_order = []

# Global search cache instance
search_cache = SearchCache()

def get_cached_search(query):
    """Get cached search results."""
    return search_cache.get(query.lower())

def cache_search_results(query, results):
    """Cache search results."""
    search_cache.set(query.lower(), results)