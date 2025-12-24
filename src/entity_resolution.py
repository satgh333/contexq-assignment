# src/entity_resolution.py
import re
import Levenshtein
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, monotonically_increasing_id
from pyspark.sql.types import StringType, IntegerType

def clean_corporate_name(name: str) -> str:
    """Clean corporate name for matching"""
    if not name:
        return ""
    # Remove punctuation, normalize case, remove common suffixes
    cleaned = re.sub(r'[^\w\s]', '', name.lower())
    cleaned = re.sub(r'\b(inc|corp|ltd|llc|company|co)\b', '', cleaned)
    return ' '.join(cleaned.split())

def simplify_address(address: str) -> str:
    """Extract city/state from address"""
    if not address:
        return ""
    # Simple extraction - in real scenario would use address parsing library
    parts = address.split(',')
    if len(parts) >= 2:
        return f"{parts[-2].strip()} {parts[-1].strip()}".lower()  # ✅ force lowercase
    return address.lower().strip()

def calculate_similarity(name1: str, addr1: str, name2: str, addr2: str) -> float:
    """Calculate similarity score between two corporate records"""
    name_sim = Levenshtein.ratio(clean_corporate_name(name1), clean_corporate_name(name2))
    addr_sim = Levenshtein.ratio(simplify_address(addr1), simplify_address(addr2))
    return (name_sim * 0.7) + (addr_sim * 0.3)  # Weight name more heavily

def assign_corporate_ids(df1: DataFrame, df2: DataFrame, threshold: float = 0.85) -> dict:
    """Assign unique corporate IDs based on entity resolution"""
    # Convert to Pandas for easier processing (in production, use distributed approach)
    pdf1 = df1.toPandas()
    pdf2 = df2.toPandas()
    
    corporate_id = 1
    id_mapping = {}
    
    # Process source 1
    for idx, row in pdf1.iterrows():
        key = f"s1_{idx}"
        id_mapping[key] = corporate_id
        corporate_id += 1
    
    # Process source 2 with matching
    for idx, row in pdf2.iterrows():
        best_match_id = None
        best_score = 0
        
        for s1_idx, s1_row in pdf1.iterrows():
            score = calculate_similarity(
                row['corporate_name_S2'], row.get('address', ''),
                s1_row['corporate_name_S1'], s1_row.get('address', '')
            )
            if score > threshold and score > best_score:
                best_score = score
                best_match_id = id_mapping[f"s1_{s1_idx}"]
        
        if best_match_id:
            id_mapping[f"s2_{idx}"] = best_match_id
        else:
            id_mapping[f"s2_{idx}"] = corporate_id
            corporate_id += 1
    
    return id_mapping
