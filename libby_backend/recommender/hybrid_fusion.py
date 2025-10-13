# hybrid_fusion.py
from .content_based import get_content_scores
from .collaborative import get_collab_scores
from .author_based import get_author_recommendations
from .trending import get_trending_books
from .diversity import get_diverse_books
from .confidence_score import compute_confidence

def get_final_recommendations(user_id, top_n=10):
    # Step 1: Collect results from each algorithm
    cb_scores = get_content_scores(user_id)
    cf_scores = get_collab_scores(user_id)
    trend_scores = get_trending_books()
    author_scores = get_author_recommendations(user_id)
    diversity_scores = get_diverse_books(user_id)
    
    # Step 2: Normalize + weight
    final_scores = {}
    for book_id in set(cb_scores) | set(cf_scores) | set(trend_scores):
        final_scores[book_id] = (
            0.35*cb_scores.get(book_id,0) +
            0.25*cf_scores.get(book_id,0) +
            0.20*trend_scores.get(book_id,0) +
            0.15*author_scores.get(book_id,0) +
            0.05*diversity_scores.get(book_id,0)
        )
    
    # Step 3: Sort results
    ranked = sorted(final_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Step 4: Compute confidence for batch
    confidence = compute_confidence(R=0.9, D=0.8, C=0.95, Q=0.88)
    
    # Step 5: Return top N with confidence
    results = [
        {"book_id": b, "score": s, "confidence": confidence}
        for b, s in ranked[:top_n]
    ]
    return results