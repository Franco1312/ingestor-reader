# Diseño: Sistema de Proyecciones

## Objetivo

Implementar una etapa de proyección que mergee datos de versiones recién publicadas con datos existentes en proyecciones, facilitando el consumo de datos para los consumidores finales.

## Flujo General

```
Versión Publicada → Staging → Merge/Append → Proyección (Atomic)
```

1. **Trigger**: Después de que una versión se publica exitosamente
2. **Staging**: Los datos nuevos se copian a `staging/` con la misma estructura de partición
3. **Merge/Append**: Se mergean los datos de `staging/` con los de `projections/`
4. **Atomic Move**: Si el merge es exitoso, se mueve atómicamente de `staging/` a `projections/`
5. **Cleanup**: Se limpia `staging/` después del movimiento exitoso

## Estructura de Datos en S3

```
datasets/{dataset_id}/
├── index/
│   └── current_version.txt
├── versions/
│   └── {version_id}/
│       ├── data/
│       │   └── {internal_series_code}/year={YYYY}/month={MM}/
│       │       └── data.parquet
│       └── manifest.json
├── staging/                          # Área temporal para merge
│   └── {internal_series_code}/
│       └── year={YYYY}/
│           └── month={MM}/
│               └── data.parquet     # Datos nuevos a mergear
└── projections/                     # Datos finales para consumidores
    └── {internal_series_code}/
        └── year={YYYY}/
            └── month={MM}/
                └── data.parquet     # Datos mergeados (histórico + nuevos)
```

**Nota**: La estructura de partición en `staging/` y `projections/` es idéntica a la de `versions/`, usando la misma `PartitionStrategy` configurada para el dataset.

## Componentes Arquitectónicos

### 1. **ProjectionManager** (`src/infrastructure/projections/projection_manager.py`)

**Responsabilidad**: Orquestar el proceso completo de proyección

**Operaciones**:
- `project_version(version_id: str, dataset_id: str) -> None`: 
  - Método principal que ejecuta el flujo completo
  - Copia datos de versión a staging
  - Ejecuta merge/append
  - Mueve atómicamente a projections
  - Limpia staging

**Flujo interno**:
1. `_copy_version_to_staging()`: Copia parquet files de versión a staging
2. `_merge_staging_with_projections()`: Mergea datos de staging con projections
3. `_atomic_move_to_projections()`: Mueve staging a projections de forma atómica
4. `_cleanup_staging()`: Elimina staging después del movimiento exitoso

### 2. **StagingManager** (`src/infrastructure/projections/staging_manager.py`)

**Responsabilidad**: Gestionar el área de staging

**Operaciones**:
- `copy_from_version(version_id: str, dataset_id: str, parquet_files: List[str]) -> List[str]`:
  - Copia archivos parquet de una versión a staging
  - Retorna lista de paths en staging
  
- `list_staging_partitions(dataset_id: str) -> List[str]`:
  - Lista todas las particiones que hay en staging
  
- `clear_staging(dataset_id: str) -> None`:
  - Limpia completamente el área de staging
  - Útil para rollback o cleanup

### 3. **ProjectionMerger** (`src/infrastructure/projections/projection_merger.py`)

**Responsabilidad**: Mergear datos de staging con projections

**Operaciones**:
- `merge_partition(dataset_id: str, partition_path: str) -> None`:
  - Lee datos de staging para una partición
  - Lee datos existentes de projections para la misma partición (si existen)
  - Detecta duplicados (mismo `obs_time` + `internal_series_code`)
  - Appendea solo datos nuevos (sin duplicados)
  - Escribe el resultado mergeado a staging (sobrescribe el archivo temporal)
  
- `merge_all_partitions(dataset_id: str, partition_strategy: PartitionStrategy) -> None`:
  - Identifica todas las particiones en staging
  - Ejecuta merge para cada partición
  - Usa la misma `PartitionStrategy` que las versiones

**Estrategia de Merge**:
- **Append-only**: Solo se agregan filas nuevas
- **Deduplicación**: Se detectan duplicados por `(obs_time, internal_series_code)`
- **No modificaciones**: Si una fila ya existe, se mantiene la original (no se actualiza)
- **Partición por partición**: Cada partición se mergea independientemente

### 4. **AtomicProjectionMover** (`src/infrastructure/projections/atomic_mover.py`)

**Responsabilidad**: Mover staging a projections de forma atómica y resiliente

**Operaciones**:
- `move_staging_to_projections(dataset_id: str) -> None`:
  - Mueve todos los archivos de `staging/` a `projections/` de forma atómica
  - Implementa estrategia de "copy-then-delete" para atomicidad
  - Maneja errores y rollback

**Estrategia de Atomicidad**:

**Opción 1: Copy-then-Delete (Recomendada)**
1. Copiar todos los archivos de `staging/` a `projections/` (sobrescribiendo si existen)
2. Si todas las copias son exitosas, eliminar `staging/`
3. Si alguna copia falla, hacer rollback (eliminar lo copiado) y mantener staging

**Opción 2: Prefix Swap (Más compleja pero más atómica)**
1. Copiar staging a un prefijo temporal `projections_temp/`
2. Si exitoso, listar todos los objetos en `projections/` y `projections_temp/`
3. Hacer "swap" renombrando prefijos (requiere operaciones batch)
4. Limpiar staging y projections_temp

**Recomendación**: Opción 1 (Copy-then-Delete) es más simple y suficiente para nuestro caso.

**Manejo de Errores**:
- Si falla durante la copia: rollback (eliminar lo copiado), mantener staging
- Si falla durante el delete: los datos ya están en projections, solo cleanup manual
- Logging detallado de cada operación para debugging

### 5. **ProjectionUseCase** (`src/application/projection_use_case.py`)

**Responsabilidad**: Caso de uso de alto nivel para ejecutar proyecciones

**Operaciones**:
- `execute_projection(version_id: str, dataset_id: str, config: Dict[str, Any]) -> None`:
  - Orquesta el proceso completo
  - Maneja errores y logging
  - Puede ser llamado manualmente o automáticamente después de load

**Integración con ETL**:
- Opción 1: Ejecutar automáticamente después de `load()` exitoso
- Opción 2: Ejecutar como etapa separada (más control, más flexible)
- **Recomendación**: Opción 2 (etapa separada) para mayor control y posibilidad de re-proyectar versiones anteriores

## Decisiones de Diseño

### 1. **Atomicidad y Resiliencia**

**Problema**: S3 no soporta transacciones atómicas nativas.

**Solución**:
- **Fase 1 (Merge)**: Mergear en staging (sobrescribir archivos mergeados)
- **Fase 2 (Copy)**: Copiar staging → projections (todas las particiones)
- **Fase 3 (Verify)**: Verificar que todas las copias fueron exitosas
- **Fase 4 (Cleanup)**: Si todo OK, eliminar staging
- **Rollback**: Si falla en Fase 2 o 3, eliminar lo copiado y mantener staging

**Manejo de Errores Parciales**:
- Si falla la copia de una partición: rollback completo
- Mantener staging intacto para retry
- Logging detallado para identificar qué falló

### 2. **Deduplicación**

**Criterio de Duplicados**:
- Clave primaria: `(obs_time, internal_series_code)`
- Si existe en projections, no se agrega desde staging
- **Política**: "First write wins" (se mantiene el dato original)

**Implementación**:
- Leer parquet de projections (si existe)
- Leer parquet de staging
- Filtrar duplicados usando pandas/pyarrow
- Escribir resultado mergeado

### 3. **Particionamiento Consistente**

- Usar la misma `PartitionStrategy` que las versiones
- Garantizar que staging y projections usen la misma estructura
- Centralizar lógica de particionamiento (ya implementado)

### 4. **Performance**

**Consideraciones**:
- Merge partición por partición (paralelizable)
- Usar operaciones batch de S3 cuando sea posible
- Cachear listas de particiones para evitar múltiples list_objects
- Considerar compresión (ya configurada en ParquetWriter)

### 5. **Idempotencia**

- Si se ejecuta proyección dos veces para la misma versión:
  - Primera vez: mergea y mueve
  - Segunda vez: detecta que ya está en projections, skip o re-mergea (según política)
- **Recomendación**: Detectar versiones ya proyectadas y skip (idempotente)

### 6. **Tracking de Proyecciones**

**Opciones**:
- **Opción A**: Guardar lista de versiones proyectadas en `projections/manifest.json`
- **Opción B**: No trackear, confiar en deduplicación
- **Opción C**: Guardar metadata en cada archivo parquet (no recomendado)

**Recomendación**: Opción A - mantener un manifest en projections con:
```json
{
  "projected_versions": ["v20240115_143022", "v20240116_120000"],
  "last_projection_date": "2024-01-16T12:00:00Z",
  "total_data_points": 15234,
  "partitions": [...]
}
```

## Flujo Detallado

### Paso 1: Trigger de Proyección
```
Después de load() exitoso:
  - version_id = "v20240115_143022"
  - projection_use_case.execute_projection(version_id, dataset_id, config)
```

### Paso 2: Copiar Versión a Staging
```
versions/{version_id}/data/{partition}/data.parquet
  → staging/{partition}/data.parquet
```

### Paso 3: Merge Staging con Projections
```
Para cada partición en staging:
  1. Leer projections/{partition}/data.parquet (si existe)
  2. Leer staging/{partition}/data.parquet
  3. Detectar duplicados (obs_time + internal_series_code)
  4. Filtrar duplicados de staging
  5. Concatenar: projections_data + staging_new_data
  6. Escribir resultado a staging/{partition}/data.parquet (sobrescribir)
```

### Paso 4: Atomic Move
```
Para cada archivo en staging:
  1. Copiar staging/{partition}/data.parquet → projections/{partition}/data.parquet
  2. Si todas las copias OK:
     - Eliminar staging/{partition}/data.parquet
  3. Si alguna copia falla:
     - Eliminar todas las copias hechas
     - Mantener staging intacto
     - Raise exception
```

### Paso 5: Cleanup y Tracking
```
1. Verificar que staging está vacío
2. Actualizar projections/manifest.json con nueva versión proyectada
3. Log success
```

## Manejo de Errores

### Escenario 1: Error durante Merge
- **Estado**: Staging tiene datos originales de versión
- **Acción**: Mantener staging, log error, raise exception
- **Recovery**: Retry o cleanup manual de staging

### Escenario 2: Error durante Copy (parcial)
- **Estado**: Algunos archivos copiados a projections, otros no
- **Acción**: Rollback (eliminar lo copiado), mantener staging
- **Recovery**: Retry completo

### Escenario 3: Error durante Delete de Staging
- **Estado**: Datos ya en projections, staging no eliminado
- **Acción**: Log warning, datos ya están disponibles
- **Recovery**: Cleanup manual de staging (idempotente)

### Escenario 4: Re-proyección de Versión
- **Estado**: Versión ya proyectada anteriormente
- **Acción**: Skip (idempotente) o re-mergear según política
- **Recomendación**: Skip si ya está en manifest

## Integración con ETL

### Opción A: Automático después de Load
```python
# En S3VersionedLoader.load()
self._loader.load(data, config)
# Automáticamente:
projection_use_case.execute_projection(version_id, dataset_id, config)
```

**Pros**: Automático, siempre actualizado
**Contras**: Menos control, más tiempo de ejecución

### Opción B: Etapa Separada (Recomendada)
```python
# CLI o scheduler separado
projection_use_case.execute_projection(version_id, dataset_id, config)
```

**Pros**: Más control, puede re-proyectar versiones anteriores, separación de responsabilidades
**Contras**: Requiere ejecución manual o scheduler

**Recomendación**: Opción B - etapa separada con CLI command

## Configuración

```yaml
projection:
  enabled: true  # opcional, default: false
  auto_project: false  # si true, ejecuta automáticamente después de load
  partition_strategy: series_year_month  # debe coincidir con load
  deduplication_key:
    - obs_time
    - internal_series_code
```

## Preguntas Abiertas

1. **Re-proyección**: ¿Permitir re-proyectar versiones anteriores?
   - **Recomendación**: Sí, con flag `--force` para sobrescribir

2. **Modificaciones**: ¿Qué hacer si una fila en staging tiene el mismo `(obs_time, internal_series_code)` pero diferente `value`?
   - **Recomendación**: Mantener la original (first write wins), log warning

3. **Performance con muchas particiones**: ¿Límite de particiones por proyección?
   - **Recomendación**: Procesar en batches, paralelizar si es necesario

4. **Cleanup de versiones antiguas**: ¿Eliminar versiones después de proyectar?
   - **Recomendación**: No, mantener versiones para auditoría

5. **Validación de integridad**: ¿Checksums o validación de datos?
   - **Recomendación**: Validar schema y conteo de filas después del merge

## Próximos Pasos (Post-Implementación)

1. Implementar ProjectionManager con TDD
2. Implementar StagingManager con TDD
3. Implementar ProjectionMerger con TDD
4. Implementar AtomicProjectionMover con TDD
5. Implementar ProjectionUseCase
6. Agregar CLI command para ejecutar proyecciones
7. Tests de integración con S3 real o LocalStack
8. Métricas y observabilidad

