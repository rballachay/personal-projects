from transformers import AutoTokenizer, AutoModel
import torch
from typing import List
import numpy as np

class EmbeddingTransformer:
    def __init__(self, model_name="sentence-transformers/all-MiniLM-L6-v2", device="cpu"):
        self.device = device or ("cuda" if torch.cuda.is_available() else "cpu")
        self.tokenizer = AutoTokenizer.from_pretrained(model_name)
        self.model = AutoModel.from_pretrained(model_name)
    
    def encode_text(self, text: str) -> np.ndarray:
        """Single text embedding (your current code)"""
        inputs = self.tokenizer(text, return_tensors="pt", truncation=True, padding=True)
        with torch.no_grad():
            model_output = self.model(**inputs)
        token_embeddings = model_output.last_hidden_state
        attention_mask = inputs['attention_mask']
        mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
        summed = torch.sum(token_embeddings * mask_expanded, 1)
        counts = torch.clamp(mask_expanded.sum(1), min=1e-9)
        mean_pooled = summed / counts
        normalized = torch.nn.functional.normalize(mean_pooled, p=2, dim=1)
        return normalized[0].cpu().numpy()

    def encode_texts(self, texts: List[str], batch_size: int = 32) -> List[np.ndarray]:
        """Batch encoding for multiple texts."""
        embeddings = []
        for i in range(0, len(texts), batch_size):
            batch_texts = texts[i:i+batch_size]
            inputs = self.tokenizer(batch_texts, return_tensors="pt", truncation=True, padding=True)
            with torch.no_grad():
                model_output = self.model(**inputs)
            token_embeddings = model_output.last_hidden_state
            attention_mask = inputs['attention_mask']
            mask_expanded = attention_mask.unsqueeze(-1).expand(token_embeddings.size()).float()
            summed = torch.sum(token_embeddings * mask_expanded, 1)
            counts = torch.clamp(mask_expanded.sum(1), min=1e-9)
            mean_pooled = summed / counts
            normalized = torch.nn.functional.normalize(mean_pooled, p=2, dim=1)
            embeddings.extend(normalized.cpu().numpy())
        return embeddings
