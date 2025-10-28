# src/extraction/extract.py
"""
Extractor (portado desde lambda.py) - módulo solo para scraping/CSV.
No realiza operaciones en BD ni usa AWS.
Interfaz:
    extract(output_csv_path: str, num_pages: int = 9, force_scrape: bool = False, verbose: bool = False) -> str
"""
from typing import List, Dict, Optional
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import os

# --- Config (mantener sincronizado con lambda.py si hace falta) ---
ENTITY_VALUE = 'Agencia Nacional de Infraestructura'
FIXED_CLASSIFICATION_ID = 13
URL_BASE = "https://www.ani.gov.co/informacion-de-la-ani/normatividad?field_tipos_de_normas__tid=12&title=&body_value=&field_fecha__value%5Bvalue%5D%5Byear%5D="

CLASSIFICATION_KEYWORDS = {
    'resolución': 15,
    'resolucion': 15,
    'decreto': 14,
}
DEFAULT_RTYPE_ID = 14

# --- Helpers ---
def clean_quotes(text: Optional[str]) -> Optional[str]:
    if not text:
        return text
    quotes_map = {
        '\u201C': '', '\u2018': '', '\u2019': '', '\u00AB': '', '\u00BB': '',
        '\u201E': '', '\u201A': '', '\u2039': '', '\u203A': '', '"': '',
        "'": '', '´': '', '`': '', '′': '', '″': '',
    }
    cleaned_text = text
    for quote_char, replacement in quotes_map.items():
        cleaned_text = cleaned_text.replace(quote_char, replacement)
    quotes_pattern = r'["\'\u201C\u201D\u2018\u2019\u00AB\u00BB\u201E\u201A\u2039\u203A\u2032\u2033]'
    cleaned_text = re.sub(quotes_pattern, '', cleaned_text)
    cleaned_text = cleaned_text.strip()
    cleaned_text = ' '.join(cleaned_text.split())
    return cleaned_text

def get_rtype_id(title: str) -> int:
    title_lower = (title or "").lower()
    for keyword, rtype_id in CLASSIFICATION_KEYWORDS.items():
        if keyword in title_lower:
            return rtype_id
    return DEFAULT_RTYPE_ID

def is_valid_created_at(created_at_value) -> bool:
    if not created_at_value:
        return False
    if isinstance(created_at_value, str):
        return bool(created_at_value.strip())
    if isinstance(created_at_value, datetime):
        return True
    return False

def normalize_datetime(dt):
    if dt is None:
        return None
    if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    return dt

# --- Extraer piezas de la fila (same logic) ---
def extract_title_and_link(row, norma_data: Dict, verbose: bool, row_num: int) -> bool:
    title_cell = row.find('td', class_='views-field views-field-title')
    if not title_cell:
        if verbose: print(f"[title] no title cell row {row_num}")
        return False
    title_link = title_cell.find('a')
    if not title_link:
        if verbose: print(f"[title] no link row {row_num}")
        return False
    raw_title = title_link.get_text(strip=True)
    cleaned_title = clean_quotes(raw_title)
    # validate length
    if len(cleaned_title) > 65:
        if verbose: print(f"[title] skipping too long ({len(cleaned_title)}) row {row_num}")
        return False
    norma_data['title'] = cleaned_title
    external_link = title_link.get('href')
    if external_link and not external_link.startswith('http'):
        external_link = 'https://www.ani.gov.co' + external_link
    norma_data['external_link'] = external_link
    norma_data['gtype'] = 'link' if external_link else None
    if not norma_data['external_link']:
        if verbose: print(f"[title] skipping no external link row {row_num}")
        return False
    return True

def extract_summary(row, norma_data: Dict):
    summary_cell = row.find('td', class_='views-field views-field-body')
    if summary_cell:
        raw_summary = summary_cell.get_text(strip=True)
        cleaned_summary = clean_quotes(raw_summary)
        formatted_summary = cleaned_summary.capitalize()
        norma_data['summary'] = formatted_summary
    else:
        norma_data['summary'] = None

def extract_creation_date(row, norma_data: Dict, verbose: bool, row_num: int) -> bool:
    fecha_cell = row.find('td', class_='views-field views-field-field-fecha--1')
    if fecha_cell:
        fecha_span = fecha_cell.find('span', class_='date-display-single')
        if fecha_span:
            created_at_raw = fecha_span.get('content', fecha_span.get_text(strip=True))
            if 'T' in created_at_raw:
                norma_data['created_at'] = created_at_raw.split('T')[0]
            elif '/' in created_at_raw:
                try:
                    day, month, year = created_at_raw.split('/')
                    norma_data['created_at'] = f"{year}-{month.zfill(2)}-{day.zfill(2)}"
                except Exception:
                    norma_data['created_at'] = created_at_raw
            else:
                norma_data['created_at'] = created_at_raw
        else:
            norma_data['created_at'] = fecha_cell.get_text(strip=True)
    else:
        norma_data['created_at'] = None

    if not is_valid_created_at(norma_data['created_at']):
        if verbose:
            print(f"[date] skipping no valid date for '{norma_data.get('title')}' row {row_num}")
        return False
    return True

# --- Scrape single page ---
def scrape_page(page_num: int, verbose: bool=False) -> List[Dict]:
    if page_num == 0:
        page_url = URL_BASE
    else:
        page_url = f"{URL_BASE}&page={page_num}"
    if verbose: print(f"[scrape] page {page_num} -> {page_url}")
    try:
        resp = requests.get(page_url, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.content, 'html.parser')
        tbody = soup.find('tbody')
        if not tbody:
            if verbose: print(f"[scrape] no table page {page_num}")
            return []
        rows = tbody.find_all('tr')
        if verbose: print(f"[scrape] found {len(rows)} rows page {page_num}")
        page_data: List[Dict] = []
        for i, row in enumerate(rows, 1):
            try:
                norma_data = {
                    'created_at': None,
                    'update_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'is_active': True,
                    'title': None,
                    'gtype': None,
                    'entity': ENTITY_VALUE,
                    'external_link': None,
                    'rtype_id': None,
                    'summary': None,
                    'classification_id': FIXED_CLASSIFICATION_ID,
                }
                if not extract_title_and_link(row, norma_data, verbose, i):
                    continue
                extract_summary(row, norma_data)
                if not extract_creation_date(row, norma_data, verbose, i):
                    continue
                norma_data['rtype_id'] = get_rtype_id(norma_data['title'])
                page_data.append(norma_data)
            except Exception as e:
                if verbose: print(f"[scrape] error row {i} page {page_num}: {e}")
                continue
        return page_data
    except requests.RequestException as e:
        if verbose: print(f"[scrape] http error page {page_num}: {e}")
        return []
    except Exception as e:
        if verbose: print(f"[scrape] error page {page_num}: {e}")
        return []

# --- Public API ---
def fetch_rows(num_pages: int = 9, verbose: bool=False) -> List[Dict]:
    """
    Retorna la lista de dicts extraídos recorriendo páginas 0..num_pages-1.
    """
    all_data: List[Dict] = []
    for p in range(num_pages):
        page_rows = scrape_page(p, verbose=verbose)
        if page_rows:
            all_data.extend(page_rows)
    return all_data

def extract(output_csv_path: str, num_pages: int = 9, force_scrape: bool=False, verbose: bool=False) -> str:
    """
    Ejecuta extracción y escribe CSV en output_csv_path.
    Retorna la ruta al CSV generado.
    """
    # fuerza de scrapping ignorada aquí; leave hook for future integration
    rows = fetch_rows(num_pages=num_pages, verbose=verbose)
    if not rows:
        # escribir header vacío para contract, pero levantar excepción si quieres
        expected = [
            "title","external_link","created_at","summary",
            "rtype_id","classification_id","entity","gtype","is_active","update_at"
        ]
        os.makedirs(os.path.dirname(output_csv_path) or ".", exist_ok=True)
        pd.DataFrame(columns=expected).to_csv(output_csv_path, index=False)
        return output_csv_path

    # normalize rows lightly
    normalized = []
    for r in rows:
        nr = dict(r)
        # ensure created_at only date part
        if nr.get("created_at"):
            nr["created_at"] = str(nr["created_at"]).split("T")[0]
        for k, v in list(nr.items()):
            if isinstance(v, str):
                nr[k] = v.strip()
        normalized.append(nr)

    df = pd.DataFrame(normalized)
    # ensure expected columns exist
    expected = [
        "title","external_link","created_at","summary",
        "rtype_id","classification_id","entity","gtype","is_active","update_at"
    ]
    for c in expected:
        if c not in df.columns:
            df[c] = None

    os.makedirs(os.path.dirname(output_csv_path) or ".", exist_ok=True)
    df.to_csv(output_csv_path, index=False)
    return output_csv_path

# CLI quick test
if __name__ == "__main__":
    out = extract("data/output/extracted.csv", num_pages=3, force_scrape=True, verbose=True)
    print("WROTE:", out)
