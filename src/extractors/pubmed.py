import time
import xml.etree.ElementTree as ET
from datetime import date

import requests

from src.extractors.base import BaseExtractor


class PubMedExtractor(BaseExtractor):
    """Extract biomedical articles from PubMed via NCBI E-utilities (esearch + efetch)."""

    ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"

    def extract(self, start_date: date, end_date: date) -> list[dict]:
        cfg = self.config
        max_records = cfg.get("max_records", 50)
        query = cfg.get("query", "science OR technology")
        request_delay = cfg.get("request_delay", 0.4)

        fetch_ids = self._build_retry()(self._esearch)
        fetch_details = self._build_retry()(self._efetch)

        pmids = fetch_ids(query, start_date, end_date, max_records)
        if not pmids:
            self.logger.info("PubMed: no articles found for %s -> %s", start_date, end_date)
            return []

        self.logger.info("PubMed: found %d PMIDs, fetching details...", len(pmids))
        time.sleep(request_delay)

        batch_size = cfg.get("batch_size", 25)
        all_records: list[dict] = []

        for i in range(0, len(pmids), batch_size):
            batch = pmids[i:i + batch_size]
            records = fetch_details(batch)
            all_records.extend(records)
            self.logger.info("PubMed: fetched %d/%d articles", len(all_records), len(pmids))
            if i + batch_size < len(pmids):
                time.sleep(request_delay)

        self.logger.info("PubMed extract complete: %d records", len(all_records))
        return all_records

    def _esearch(self, query: str, start_date: date, end_date: date, max_records: int) -> list[str]:
        params = {
            "db": "pubmed",
            "term": f'("{start_date.strftime("%Y/%m/%d")}"[PDAT] : "{end_date.strftime("%Y/%m/%d")}"[PDAT]) AND ({query})',
            "retmax": max_records,
            "retmode": "json",
            "sort": "pub_date",
            "tool": self.config.get("tool_name", "etl-pipeline"),
            "email": self.config.get("email", "etl@example.com"),
        }
        self.logger.info("PubMed esearch: %s -> %s, query=%s", start_date, end_date, query)
        resp = requests.get(self.ESEARCH_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        data = resp.json()
        return data.get("esearchresult", {}).get("idlist", [])

    def _efetch(self, pmids: list[str]) -> list[dict]:
        params = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "xml",
            "tool": self.config.get("tool_name", "etl-pipeline"),
            "email": self.config.get("email", "etl@example.com"),
        }
        resp = requests.get(self.EFETCH_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return self._parse_xml(resp.text)

    def _parse_xml(self, xml_text: str) -> list[dict]:
        root = ET.fromstring(xml_text)
        records = []

        for article_node in root.findall(".//PubmedArticle"):
            citation = article_node.find("MedlineCitation")
            if citation is None:
                continue

            pmid_el = citation.find("PMID")
            pmid = pmid_el.text if pmid_el is not None else ""

            art = citation.find("Article")
            if art is None:
                continue

            title_el = art.find("ArticleTitle")
            title = self._get_text(title_el)

            abstract_parts = []
            abstract_node = art.find("Abstract")
            if abstract_node is not None:
                for ab_text in abstract_node.findall("AbstractText"):
                    label = ab_text.get("Label", "")
                    text = self._get_text(ab_text)
                    if label and text:
                        abstract_parts.append(f"{label}: {text}")
                    elif text:
                        abstract_parts.append(text)
            abstract = " ".join(abstract_parts) or None

            doi = None
            for eloc in art.findall(".//ELocationID"):
                if eloc.get("EIdType") == "doi":
                    doi = eloc.text
                    break

            pub_date = self._extract_date(art)

            authors = []
            author_list = art.find("AuthorList")
            if author_list is not None:
                for author_el in author_list.findall("Author"):
                    last = author_el.findtext("LastName", "")
                    fore = author_el.findtext("ForeName", "")
                    name = f"{fore} {last}".strip()
                    orcid = None
                    for ident in author_el.findall(".//Identifier"):
                        if ident.get("Source") == "ORCID":
                            orcid = (ident.text or "").rsplit("/", 1)[-1]
                            break
                    if name:
                        authors.append({"name": name, "orcid": orcid})

            tags = []
            mesh_list = citation.find("MeshHeadingList")
            if mesh_list is not None:
                for mesh in mesh_list.findall("MeshHeading"):
                    desc = mesh.find("DescriptorName")
                    if desc is not None and desc.text:
                        tags.append({"name": desc.text, "score": None})

            keyword_list = citation.find("KeywordList")
            if keyword_list is not None:
                for kw in keyword_list.findall("Keyword"):
                    if kw.text:
                        tags.append({"name": kw.text.strip(), "score": None})

            records.append({
                "external_id": pmid,
                "title": title,
                "abstract": abstract,
                "url": f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/",
                "doi": doi,
                "language": "en",
                "published_at": pub_date,
                "sentiment_score": None,
                "authors": authors,
                "tags": tags,
            })

        return records

    @staticmethod
    def _get_text(element) -> str:
        if element is None:
            return ""
        return "".join(element.itertext()).strip()

    @staticmethod
    def _extract_date(article_node) -> str | None:
        for path in ["Journal/JournalIssue/PubDate", "ArticleDate"]:
            date_el = article_node.find(path)
            if date_el is None:
                continue
            year = date_el.findtext("Year", "")
            month = date_el.findtext("Month", "01")
            day = date_el.findtext("Day", "01")
            if year:
                month_str = month.zfill(2) if month.isdigit() else "01"
                return f"{year}-{month_str}-{day.zfill(2)}"
        return None
