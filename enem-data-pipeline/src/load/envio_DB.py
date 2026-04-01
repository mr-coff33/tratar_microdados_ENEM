#!/usr/bin/env python3
"""
fast_import_enem_noano.py

Importador otimizado que usa LOAD DATA LOCAL INFILE quando possível (muito rápido).
VERSÃO SEM A COLUNA 'ano' — importa os CSVs tal como estão.

Edite a seção CONFIG abaixo para ajustar credenciais/pastas.
"""
from __future__ import annotations
import os
import re
import glob
import logging
from pathlib import Path
from typing import List, Tuple, Optional
import chardet  # ADICIONAR ESTA IMPORTACAO

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, BigInteger, String, DECIMAL, CHAR, text
from sqlalchemy.exc import SQLAlchemyError

# ================= CONFIG =================
DB_USER = "root"
DB_PASS = "1234"
DB_HOST = "45.5.39.89"
DB_PORT = 3306
DB_NAME = "teste_db"

BASE_DIR = r"C:\Users\efg\Music\danylo\dados_finais"
# pattern for microdados folders containing microdados_enem_YYYY_filtrado.csv
FOLDER_GLOB = "microdados_enem_*_filtrado"

# table name to receive all years (sem coluna 'ano')
TABLE_NAME = "microdados_ENEM"

# pandas chunking fallback
PANDAS_CHUNK = 80000      # rows read per chunk from CSV
PANDAS_BATCH = 50000      # rows per multi-insert batch in to_sql fallback

# enable/disable certain optimizations
USE_LOCAL_INFILE = True   # try LOAD DATA LOCAL INFILE first
DISABLE_CHECKS_DURING_LOAD = True  # set unique_checks/foreign_key_checks = 0 while loading
# ==========================================

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("fast_import_enem_noano")


def build_engine(local_infile: bool = True):
    """Create SQLAlchemy engine with local_infile option if requested."""
    params = f"charset=utf8mb4"
    if local_infile:
        params += "&local_infile=1"
    conn = f"mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?{params}"
    return create_engine(conn, pool_pre_ping=True, future=True)


def detect_delimiter(path: str, sample_size: int = 8192) -> str:
    try:
        with open(path, "rb") as f:
            raw = f.read(sample_size)
        s = raw.decode(errors="ignore")
        if "\t" in s:
            return "\t"
        if s.count(";") > s.count(","):
            return ";"
        return ","
    except Exception:
        return ","


def detect_encoding(file_path: str) -> str:
    """Detect file encoding using chardet"""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(100000)  # Read first 100KB to detect encoding
        result = chardet.detect(raw_data)
        encoding = result['encoding']
        confidence = result['confidence']
        
        log.info(f"Detected encoding for {os.path.basename(file_path)}: {encoding} (confidence: {confidence:.2f})")
        
        if encoding is None:
            return 'latin1'
        if 'windows' in encoding.lower() or 'cp1252' in encoding.lower():
            return 'cp1252'
        if confidence > 0.7:
            return encoding
        return 'latin1'
    except Exception as e:
        log.warning(f"Encoding detection failed: {e}, using latin1 as fallback")
        return 'latin1'


def read_header(path: str, sep: str) -> List[str]:
    """Return header columns from CSV (first row), without loading entire file."""
    encoding = detect_encoding(path)
    try:
        with open(path, "r", encoding=encoding, errors="ignore") as f:
            first = f.readline()
        cols = [c.strip() for c in first.strip().split(sep)]
        return cols
    except Exception as e:
        log.warning(f"Failed to read header with {encoding}, trying latin1: {e}")
        try:
            with open(path, "r", encoding="latin1", errors="ignore") as f:
                first = f.readline()
            cols = [c.strip() for c in first.strip().split(sep)]
            return cols
        except Exception:
            raise RuntimeError("Não foi possível ler header do arquivo")


def sanitize_col(col: str) -> str:
    """Sanitize header column name to a SQL-friendly identifier."""
    if not isinstance(col, str):
        col = str(col)
    c = col.strip()
    c = re.sub(r"\s+", "_", c)
    c = re.sub(r"[^\w_]", "", c, flags=re.UNICODE)
    return c.lower()


def extract_year_from_path(p: str) -> int:
    m = re.search(r"(19|20)\d{2}", p)
    if not m:
        raise ValueError("Ano não encontrado no path: " + p)
    return int(m.group(0))


def find_csv_files(base_dir: str) -> List[Tuple[str, int]]:
    base = Path(base_dir)
    found = []
    for d in base.glob(FOLDER_GLOB):
        if d.is_dir():
            csv = d / f"{d.name}.csv"
            if csv.exists():
                try:
                    y = extract_year_from_path(str(d))
                    found.append((str(csv), y))
                except ValueError:
                    log.warning("Não extraí ano para %s", csv)
    found.sort(key=lambda x: x[1])
    return found


def ensure_table(engine, sample_csv: str):
    """
    Create target table if not exists, inferring columns from sample CSV header.
    We'll declare reasonable types.
    """
    sep = detect_delimiter(sample_csv)
    encoding = detect_encoding(sample_csv)
    
    try:
        df_sample = pd.read_csv(sample_csv, sep=sep, encoding=encoding, nrows=5000, low_memory=False)
    except Exception as e:
        log.warning(f"Failed to read sample with {encoding}, trying latin1: {e}")
        try:
            df_sample = pd.read_csv(sample_csv, sep=sep, encoding="latin1", nrows=5000, low_memory=False)
        except Exception as e2:
            log.error(f"Failed to read sample CSV: {e2}")
            raise

    df_sample.columns = [sanitize_col(c) for c in df_sample.columns]

    meta = MetaData()
    # Basic: id then csv columns
    columns = [Column("id", Integer, primary_key=True, autoincrement=True)]

    for c in df_sample.columns:
        ser = df_sample[c].dropna()
        if ser.empty:
            columns.append(Column(c, String(255), nullable=True))
            continue
        try:
            nums = pd.to_numeric(ser.astype(str).str.replace(',', '.'), errors="coerce")
            if nums.notna().all() and (nums.fillna(0) % 1 == 0).all():
                columns.append(Column(c, Integer, nullable=True))
                continue
            if nums.notna().all():
                columns.append(Column(c, DECIMAL(10,3), nullable=True))
                continue
        except Exception:
            pass
        maxlen = int(ser.astype(str).str.len().max())
        size = 255 if maxlen <= 255 else (1000 if maxlen <= 1000 else 65535)
        columns.append(Column(c, String(size), nullable=True))

    tbl = Table(TABLE_NAME, meta, *columns, mysql_charset="utf8mb4")
    meta.create_all(engine)
    log.info("Tabela %s garantida/criada com colunas: %s", TABLE_NAME, [c.name for c in tbl.columns])
    return tbl


def load_data_local_infile(engine, csv_path: str, sep: str, header_cols: List[str]) -> bool:
    """
    Execute LOAD DATA LOCAL INFILE into TABLE_NAME.
    header_cols: original csv columns (sanitized).
    Returns True if succeeded, False otherwise.
    """
    cols_sql = ", ".join(header_cols)
    csv_db_path = csv_path.replace("\\", "/")

    sql = (
        f"LOAD DATA LOCAL INFILE '{csv_db_path}' "
        f"INTO TABLE {TABLE_NAME} "
        f"FIELDS TERMINATED BY '{sep}' OPTIONALLY ENCLOSED BY '\"' "
        f"LINES TERMINATED BY '\\n' "
        f"IGNORE 1 LINES ({cols_sql});"
    )

    conn = None
    try:
        conn = engine.raw_connection()
        cur = conn.cursor()
        if DISABLE_CHECKS_DURING_LOAD:
            cur.execute("SET FOREIGN_KEY_CHECKS=0;")
            cur.execute("SET UNIQUE_CHECKS=0;")
        cur.execute(sql)
        conn.commit()
        if DISABLE_CHECKS_DURING_LOAD:
            cur.execute("SET UNIQUE_CHECKS=1;")
            cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            conn.commit()
        cur.close()
        log.info("LOAD DATA OK para %s", os.path.basename(csv_path))
        return True
    except Exception as e:
        log.warning("LOAD DATA falhou para %s: %s", os.path.basename(csv_path), e)
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
        return False
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def pandas_fallback_insert(engine, csv_path: str, sep: str):
    """Fallback robusto usando pandas.to_sql em chunks com encoding correto."""
    total_read = 0
    total_inserted = 0

    encoding = detect_encoding(csv_path)
    
    # Tentar diferentes abordagens de leitura
    for attempt, encoding_to_try in enumerate([encoding, 'latin1', 'cp1252', 'iso-8859-1']):
        try:
            log.info(f"Tentando leitura com encoding: {encoding_to_try}")
            reader = pd.read_csv(
                csv_path, 
                sep=sep, 
                dtype=str, 
                iterator=True, 
                chunksize=PANDAS_CHUNK, 
                encoding=encoding_to_try, 
                low_memory=False,
                on_bad_lines='skip'  # Pular linhas problemáticas
            )
            break
        except UnicodeDecodeError as e:
            if attempt < 3:  # Tentar próximo encoding
                log.warning(f"Encoding {encoding_to_try} falhou, tentando próximo: {e}")
                continue
            else:
                log.error("Todos os encodings falharam")
                return 0, 0
        except Exception as e:
            log.error(f"Erro inesperado ao ler CSV: {e}")
            return 0, 0

    for chunk_num, chunk in enumerate(reader):
        try:
            chunk.columns = [sanitize_col(c) for c in chunk.columns]
            chunk = chunk.loc[:, ~chunk.columns.duplicated()]
            
            # Converter colunas numéricas
            numeric_columns = [col for col in chunk.columns if 'nota' in col or 'nu_' in col]
            for col in numeric_columns:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col].astype(str).str.replace(',', '.'), errors='coerce')
            
            chunk.to_sql(
                name=TABLE_NAME, 
                con=engine, 
                if_exists="append", 
                index=False, 
                method="multi", 
                chunksize=PANDAS_BATCH
            )
            total_inserted += len(chunk)
            
        except Exception as e:
            log.warning(f"to_sql batch falhou no chunk {chunk_num}, tentando linha-a-linha: {e}")
            # Fallback linha a linha para chunk problemático
            for i in range(len(chunk)):
                try:
                    row = chunk.iloc[[i]]
                    row.to_sql(name=TABLE_NAME, con=engine, if_exists="append", index=False)
                    total_inserted += 1
                except Exception as e2:
                    log.debug(f"Linha {i} pulada: {e2}")
        
        total_read += len(chunk)
        log.info("Pandas fallback: chunk %d - %d/%d linhas inseridas (%s)", 
                chunk_num, total_inserted, total_read, os.path.basename(csv_path))
    
    return total_read, total_inserted


def main():
    # Instalar chardet se não estiver disponível
    try:
        import chardet
    except ImportError:
        log.error("chardet não instalado. Execute: pip install chardet")
        return

    files = find_csv_files(BASE_DIR)
    if not files:
        log.error("Nenhum arquivo CSV encontrado em %s", BASE_DIR)
        return

    log.info("Arquivos encontrados (anos): %s", [f[1] for f in files])

    engine = build_engine(local_infile=USE_LOCAL_INFILE)

    sample_csv, _ = files[0]
    ensure_table(engine, sample_csv)

    total_read_all = 0
    total_inserted_all = 0

    for csv_path, year in files:
        log.info("Iniciando import para %s (ano %d)", os.path.basename(csv_path), year)
        sep = detect_delimiter(csv_path)
        
        # Detectar encoding antes de processar
        encoding = detect_encoding(csv_path)
        log.info(f"Encoding detectado para {os.path.basename(csv_path)}: {encoding}")
        
        try:
            header = read_header(csv_path, sep)
        except Exception as e:
            log.error("Não consegui ler header de %s: %s", csv_path, e)
            read, ins = pandas_fallback_insert(engine, csv_path, sep)
            total_read_all += read; total_inserted_all += ins
            continue

        header_sanitized = [sanitize_col(c) for c in header]

        loaded = False
        if USE_LOCAL_INFILE:
            loaded = load_data_local_infile(engine, csv_path, sep, header_sanitized)

        if not loaded:
            log.info("Fazendo fallback para pandas.to_sql para %s", os.path.basename(csv_path))
            read, ins = pandas_fallback_insert(engine, csv_path, sep)
            total_read_all += read; total_inserted_all += ins
        else:
            # linhas aproximadas
            try:
                with open(csv_path, "rb") as f:
                    lines = sum(1 for _ in f)
                read = max(0, lines - 1)
            except Exception:
                read = 0
            total_read_all += read
            total_inserted_all += read

    log.info("Processo concluído. Linhas lidas (aprox): %d, linhas inseridas (aprox): %d", total_read_all, total_inserted_all)

    try:
        with engine.connect() as conn:
            res = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
            cnt = res.scalar_one_or_none()
            log.info("Total de registros em %s: %s", TABLE_NAME, cnt)
    except Exception as e:
        log.warning("Não foi possível coletar estatísticas finais: %s", e)


if __name__ == "__main__":
    main()