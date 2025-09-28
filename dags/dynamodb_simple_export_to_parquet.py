from __future__ import annotations
"""DAG simplificado: DynamoDB Export -> Parquet a S3.

Flujo básico:
1. Lanza export de DynamoDB a S3
2. Espera que complete
3. Descarga archivos JSON del export
4. Convierte a Parquet y sube a S3

Sin watermarks, sin lógica incremental compleja, sin estabilización.
Solo el flujo básico que funciona.
"""

import os
import io
import json
import gzip
import hashlib
import time
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional
from airflow import DAG
from airflow.decorators import task
import boto3
import pandas as pd

DEFAULT_ARGS = {"owner": "data-eng", "depends_on_past": False, "retries": 1}

DAG_ID = "dynamodb_simple_export_to_parquet"

def parse_dynamodb_json_attr(node: Any) -> Any:
    """Convierte atributo DynamoDB JSON a tipos nativos Python/Parquet."""
    if not isinstance(node, dict) or len(node) != 1:
        return node
    
    (t, v), = node.items()
    
    # String
    if t == "S": 
        return str(v)
    
    # Number - convertir a tipos numéricos nativos
    if t == "N": 
        try:
            # Si tiene punto decimal, usar float/double
            if "." in str(v):
                return float(v)
            # Sino, usar int/bigint
            num = int(v)
            # Para Parquet, usar int64 si es muy grande
            return num if -2147483648 <= num <= 2147483647 else int(v)
        except (ValueError, TypeError): 
            return str(v)  # Fallback a string si falla conversión
    
    # Boolean
    if t == "BOOL": 
        return bool(v)
    
    # Null
    if t == "NULL": 
        return None
    
    # Map (objeto/struct) - recursivo
    if t == "M": 
        return {k: parse_dynamodb_json_attr(val) for k, val in v.items()}
    
    # List (array) - recursivo
    if t == "L": 
        return [parse_dynamodb_json_attr(i) for i in v]
    
    # String Set -> array de strings
    if t == "SS": 
        return [str(s) for s in v]
    
    # Number Set -> array de números
    if t == "NS": 
        numbers = []
        for n in v:
            try:
                numbers.append(float(n) if "." in str(n) else int(n))
            except (ValueError, TypeError):
                numbers.append(str(n))
        return numbers
    
    # Binary Set -> array de strings (base64)
    if t == "BS": 
        return [str(b) for b in v]
    
    # Binary -> string base64
    if t == "B":
        return str(v)
    
    # Fallback - devolver como string
    return str(v)

def parse_dynamodb_item(raw_item: Dict[str, Any]) -> Dict[str, Any]:
    """Convierte un item DynamoDB completo a dict con tipos nativos."""
    return {k: parse_dynamodb_json_attr(v) for k, v in raw_item.items()}

def extract_table_record(incremental_record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extrae el registro real de la tabla desde el formato incremental export.
    
    Formato incremental:
    {
        "Metadata": {"N": "1759035355355235"},
        "Keys": {"Date": {"S": "2020-04-11T06:00:00"}, "DeviceID": {"S": "d#12343"}},  
        "NewImage": {"Date": {"S": "2020-04-11T06:00:00"}, "DeviceID": {"S": "d#12343"}, "State": {"S": "NORMAL"}}
    }
    
    Resultado: {"Date": "2020-04-11T06:00:00", "DeviceID": "d#12343", "State": "NORMAL"}
    """
    
    # Priorizar NewImage (estado actual del item)
    if "NewImage" in incremental_record and incremental_record["NewImage"]:
        return parse_dynamodb_item(incremental_record["NewImage"])
    
    # Fallback a Keys si no hay NewImage (caso DELETE)  
    elif "Keys" in incremental_record and incremental_record["Keys"]:
        keys_data = parse_dynamodb_item(incremental_record["Keys"])
        keys_data["_record_type"] = "DELETE"  # Marcar como eliminado
        return keys_data
    
    # Si no hay datos útiles, skip este registro
    return None

def normalize_for_analytics(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Normaliza registros para analytics, manejando esquemas dinámicos."""
    if not records:
        return pd.DataFrame()
    
    # Crear DataFrame y optimizar tipos
    df = pd.DataFrame(records)
    
    # Optimizar tipos de columnas para Parquet
    for col in df.columns:
        # Si la columna tiene solo None/NaN, skip
        if df[col].isna().all():
            continue
            
        # Si es numérica pero tiene valores mixtos, intentar convertir
        if df[col].dtype == 'object':
            # Intentar inferir si es numérica
            try:
                # Verificar si todos los valores no-null son números
                non_null = df[col].dropna()
                if len(non_null) > 0:
                    # Si todos son int/float, convertir
                    if all(isinstance(x, (int, float)) for x in non_null):
                        # Detectar si son todos enteros
                        if all(isinstance(x, int) or (isinstance(x, float) and x.is_integer()) 
                               for x in non_null):
                            df[col] = df[col].astype('Int64')  # Nullable integer
                        else:
                            df[col] = df[col].astype('float64')
            except:
                pass  # Mantener como object/string
    
    print(f"[normalize_for_analytics] Esquema final: {dict(df.dtypes)}")
    return df

def get_boto3_client(service: str, region: str, access_key: str = "", secret_key: str = "", session_token: str = ""):
    session = boto3.session.Session(
        aws_access_key_id=access_key or None,
        aws_secret_access_key=secret_key or None,
        aws_session_token=session_token or None,
        region_name=region,
    )
    return session.client(service)

with DAG(
    dag_id=DAG_ID,
    description="DynamoDB Export -> Parquet simple",
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "table_arn": "",  # arn:aws:dynamodb:region:account:table/TABLE_NAME
        "table_name": "",  # Your DynamoDB table name
        "s3_bucket": "",   # Your S3 bucket for exports
        "s3_prefix": "",   # Prefix like: exports/dynamo/table_name
        "from_time": "2025-09-28T03:04:00+00:00",  # 22:04 Perú
        "to_time": "2025-09-28T04:04:00+00:00",    # 23:04 Perú
        "aws_region": "us-east-1",
        "aws_access_key_id": "",
        "aws_secret_access_key": "",
        "aws_session_token": "",
        "parquet_bucket": "",  # Si vacío usa el mismo del export
        "parquet_prefix": "parquet/",
        "parquet_rows_per_file": 250000,
    },
    max_active_runs=1,
    tags=["dynamodb", "parquet", "simple"],
) as dag:

    @task
    def start_export(
        table_arn: str,
        s3_bucket: str, 
        s3_prefix: str,
        from_time: str,
        to_time: str,
        aws_region: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str,
    ) -> Dict[str, Any]:
        """Inicia el export de DynamoDB."""
        print(f"[start_export] Iniciando export {table_arn} desde {from_time} hasta {to_time}")
        
        client = get_boto3_client("dynamodb", aws_region, aws_access_key_id, aws_secret_access_key, aws_session_token)
        
        resp = client.export_table_to_point_in_time(
            TableArn=table_arn,
            ExportType="INCREMENTAL_EXPORT",
            IncrementalExportSpecification={
                "ExportFromTime": datetime.fromisoformat(from_time),
                "ExportToTime": datetime.fromisoformat(to_time),
                "ExportViewType": "NEW_AND_OLD_IMAGES",
            },
            S3Bucket=s3_bucket,
            S3Prefix=s3_prefix,
            S3SseAlgorithm="AES256",
            ExportFormat="DYNAMODB_JSON",
        )
        
        export_arn = resp["ExportDescription"]["ExportArn"]
        print(f"[start_export] Export iniciado: {export_arn}")
        
        return {
            "export_arn": export_arn,
            "s3_bucket": s3_bucket,
            "s3_prefix": s3_prefix,
            "table_name": table_arn.split("/")[-1],
        }

    @task
    def wait_export(
        export_info: Dict[str, Any],
        aws_region: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str,
    ) -> Dict[str, Any]:
        """Espera que el export complete."""
        print(f"[wait_export] Esperando export: {export_info['export_arn']}")
        
        client = get_boto3_client("dynamodb", aws_region, aws_access_key_id, aws_secret_access_key, aws_session_token)
        
        while True:
            desc = client.describe_export(ExportArn=export_info["export_arn"])
            ed = desc["ExportDescription"]
            status = ed.get("ExportStatus")
            item_count = ed.get("ItemCount", 0)
            
            print(f"[wait_export] Status: {status}, Items: {item_count}")
            
            if status == "COMPLETED":
                export_info["item_count"] = item_count
                return export_info
            elif status == "FAILED":
                raise RuntimeError(f"Export falló: {desc}")
            
            time.sleep(30)

    @task
    def convert_to_parquet(
        export_info: Dict[str, Any],
        parquet_bucket: str,
        parquet_prefix: str,
        parquet_rows_per_file: int,
        aws_region: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str,
    ) -> Dict[str, Any]:
        """Descarga JSONs del export y convierte a Parquet."""
        print(f"[convert_to_parquet] Procesando export con {export_info.get('item_count', 0)} items")
        
        s3 = get_boto3_client("s3", aws_region, aws_access_key_id, aws_secret_access_key, aws_session_token)
        
        bucket = export_info["s3_bucket"]
        prefix = export_info["s3_prefix"].rstrip("/")
        
        # Extraer export_id del ARN - ES OBLIGATORIO para incremental
        export_arn = export_info["export_arn"]
        if "/export/" not in export_arn:
            raise ValueError(f"No se puede extraer export_id de: {export_arn}")
        
        export_id = export_arn.split("/export/")[-1]
        
        # Obtener timestamp del export para filtrar archivos
        from datetime import datetime, timezone
        
        # El export se inició hace poco, buscar archivos creados en los últimos 10 minutos
        cutoff_time = datetime.now(timezone.utc) - timedelta(minutes=10)
        
        # Buscar en ubicaciones conocidas, filtrando por timestamp
        candidate_prefixes = [
            f"{prefix}/{export_id}/AWSDynamoDB/data/",
            f"{prefix}/{export_id}/",
            f"{prefix}/AWSDynamoDB/data/",  # Ubicación común
            f"{prefix}/",
        ]
        
        objects = []
        found_prefix = None
        
        for search_prefix in candidate_prefixes:
            print(f"[convert_to_parquet] Explorando: s3://{bucket}/{search_prefix}")
            
            paginator = s3.get_paginator("list_objects_v2")
            temp_objects = []
            
            for page in paginator.paginate(Bucket=bucket, Prefix=search_prefix):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    last_modified = obj.get("LastModified")
                    
                    # Filtrar por extensión y timestamp
                    if (key.endswith((".data", ".data.gz", ".json", ".json.gz")) and 
                        last_modified and last_modified >= cutoff_time):
                        temp_objects.append((key, last_modified))
            
            print(f"[convert_to_parquet] -> Encontrados {len(temp_objects)} archivos recientes")
            
            # Si encontramos archivos recientes, usar estos
            if temp_objects:
                # Ordenar por timestamp (más recientes primero)
                temp_objects.sort(key=lambda x: x[1], reverse=True)
                objects = [obj[0] for obj in temp_objects]
                found_prefix = search_prefix
                
                print(f"[convert_to_parquet] ✅ USANDO: {search_prefix} con {len(objects)} archivos recientes")
                print(f"[convert_to_parquet] Archivos más recientes:")
                for key, ts in temp_objects[:5]:  # Mostrar primeros 5
                    print(f"  {key} (creado: {ts})")
                break
        
        if not objects:
            # DEBUG FINAL: Listar TODO en el prefijo base para ver estructura real
            print(f"[convert_to_parquet] 🔍 DEBUG: Listando TODA la estructura en s3://{bucket}/{prefix}/")
            
            paginator = s3.get_paginator("list_objects_v2")
            all_keys = []
            for page in paginator.paginate(Bucket=bucket, Prefix=f"{prefix}/"):
                for obj in page.get("Contents", []):
                    all_keys.append(obj["Key"])
            
            print(f"[convert_to_parquet] Total objetos en prefijo: {len(all_keys)}")
            
            # Mostrar estructura relevante
            relevant_keys = [k for k in all_keys if export_id in k or "AWSDynamoDB" in k][:20]
            for key in relevant_keys:
                print(f"[convert_to_parquet] Estructura: {key}")
            
            print("[convert_to_parquet] ❌ No se encontraron archivos de datos en ninguna ubicación")
            return {"parquet_files": [], "record_count": 0, "debug_all_keys": all_keys[:50]}
        
        # Procesar archivos
        records = []
        for key in objects:
            print(f"[convert_to_parquet] Procesando {key}")
            
            obj_resp = s3.get_object(Bucket=bucket, Key=key)
            body = obj_resp["Body"].read()
            
            if key.endswith(".gz"):
                body = gzip.decompress(body)
            
            # Parsear líneas JSON y extraer registros de tabla real
            for line in body.splitlines():
                if not line.strip():
                    continue
                try:
                    # Parsear el registro incremental completo
                    incremental_record = json.loads(line.decode("utf-8"))
                    
                    # Extraer el registro real de la tabla (desde NewImage/Keys)
                    table_record = extract_table_record(incremental_record)
                    
                    if table_record:
                        records.append(table_record)
                        
                except Exception as e:
                    print(f"[convert_to_parquet] Error parsing line: {e}")
        
        print(f"[convert_to_parquet] Total registros de tabla extraídos: {len(records)}")
        
        if not records:
            return {"parquet_files": [], "record_count": 0}
        
        # Normalizar para analytics - esquema optimizado
        df = normalize_for_analytics(records)
        print(f"[convert_to_parquet] DataFrame normalizado: {len(df)} filas, {len(df.columns)} columnas")
        
        # Mostrar muestra de datos normalizados + estadísticas
        if len(df) > 0:
            print(f"[convert_to_parquet] Muestra de datos normalizados:")
            for col in df.columns[:5]:  # Primeras 5 columnas
                sample_val = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
                null_count = df[col].isna().sum()
                print(f"  {col}: {type(sample_val).__name__} = {sample_val} (nulls: {null_count}/{len(df)})")
            
            # Alertas para esquemas problemáticos
            if len(df.columns) > 50:
                print(f"⚠️  ALERTA: {len(df.columns)} columnas detectadas. Considera normalizar esquema.")
            
            memory_mb = df.memory_usage(deep=True).sum() / 1024 / 1024
            print(f"[convert_to_parquet] Memoria DataFrame: {memory_mb:.1f} MB")
        
        # Convertir a Parquet y subir
        target_bucket = parquet_bucket or bucket
        base_prefix = parquet_prefix.rstrip("/")
        table_name = export_info["table_name"]
        
        uploaded_files = []
        parquet_rows_per_file = int(parquet_rows_per_file)
        
        # Chunking con DataFrame normalizado
        for i in range(0, len(df), parquet_rows_per_file):
            chunk_df = df.iloc[i:i + parquet_rows_per_file]
            
            # Crear archivo temporal
            temp_file = f"/tmp/{table_name}_{i//parquet_rows_per_file}.parquet"
            
            # Escribir Parquet con esquema optimizado
            chunk_df.to_parquet(
                temp_file, 
                index=False,
                engine='pyarrow',
                compression='snappy',  # Buena compresión para analytics
                use_dictionary=True,   # Optimizar strings repetidos
            )
            
            # Subir a S3
            s3_key = f"{base_prefix}/{table_name}_{i//parquet_rows_per_file}.parquet"
            s3.upload_file(temp_file, target_bucket, s3_key)
            uploaded_files.append(f"s3://{target_bucket}/{s3_key}")
            
            # Limpiar archivo temporal
            os.remove(temp_file)
            
            print(f"[convert_to_parquet] Subido: s3://{target_bucket}/{s3_key} ({len(chunk_df)} filas)")
        
        return {
            "parquet_files": uploaded_files,
            "record_count": len(df),
            "target_bucket": target_bucket,
            "schema_columns": list(df.columns),
            "schema_types": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }

    # Flujo del DAG
    export_info = start_export(
        "{{ params.table_arn }}",
        "{{ params.s3_bucket }}",
        "{{ params.s3_prefix }}",
        "{{ params.from_time }}",
        "{{ params.to_time }}",
        "{{ params.aws_region }}",
        "{{ params.aws_access_key_id }}",
        "{{ params.aws_secret_access_key }}",
        "{{ params.aws_session_token }}",
    )
    
    completed_export = wait_export(
        export_info,
        "{{ params.aws_region }}",
        "{{ params.aws_access_key_id }}",
        "{{ params.aws_secret_access_key }}",
        "{{ params.aws_session_token }}",
    )
    
    result = convert_to_parquet(
        completed_export,
        "{{ params.parquet_bucket }}",
        "{{ params.parquet_prefix }}",
        "{{ params.parquet_rows_per_file }}",
        "{{ params.aws_region }}",
        "{{ params.aws_access_key_id }}",
        "{{ params.aws_secret_access_key }}",
        "{{ params.aws_session_token }}",
    )