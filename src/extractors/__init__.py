from src.extractors.openalex import OpenAlexExtractor
from src.extractors.arxiv import ArxivExtractor
from src.extractors.pubmed import PubMedExtractor

EXTRACTORS = {
    "openalex": OpenAlexExtractor,
    "arxiv": ArxivExtractor,
    "pubmed": PubMedExtractor,
}
