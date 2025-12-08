# Guía de Deployment en Render

Esta guía te ayudará a deployar el proyecto Ingestor Reader v3 en Render como un Web Service con API HTTP.

## Deployment usando render.yaml (Recomendado)

### Paso 1: Preparar el repositorio

Asegúrate de que tu código esté en un repositorio Git (GitHub, GitLab, o Bitbucket).

### Paso 2: Crear un nuevo servicio en Render

1. Ve a [Render Dashboard](https://dashboard.render.com/)
2. Haz clic en "New +" y selecciona "Blueprint"
3. Conecta tu repositorio
4. Render detectará automáticamente el archivo `render.yaml` y creará el servicio configurado

### Paso 3: Configurar Variables de Entorno

En el dashboard de Render, configura las siguientes variables de entorno:

#### Variables Requeridas:

- `ENVIRONMENT`: `production` (o `staging` según corresponda)
- `AWS_ACCESS_KEY_ID`: Tu clave de acceso de AWS
- `AWS_SECRET_ACCESS_KEY`: Tu clave secreta de AWS
- `AWS_REGION`: Región de AWS (ej: `us-east-1`)

**Nota:** Las credenciales sensibles deben configurarse en el dashboard de Render, no directamente en el archivo `render.yaml` por seguridad.

### Paso 4: Verificar el Deployment

Una vez deployado, puedes verificar que el servicio está funcionando:

```bash
# Health check
curl https://tu-servicio.onrender.com/health

# Listar datasets disponibles
curl https://tu-servicio.onrender.com/api/v1/datasets

# Ejecutar ETL para un dataset específico
curl -X POST https://tu-servicio.onrender.com/api/v1/etl/bcra_infomondia_series
```

## Estructura de la API

### Health Check
```
GET /health
```
Retorna el estado del servicio.

### Listar Datasets
```
GET /api/v1/datasets
```
Retorna la lista de todos los datasets disponibles.

**Ejemplo:**
```bash
curl https://tu-servicio.onrender.com/api/v1/datasets
```

**Respuesta:**
```json
{
  "status": "success",
  "datasets": ["bcra_infomondia_series", "indec_emae", "indec_ipc"],
  "count": 3
}
```

### Ejecutar ETL (por URL)
```
POST /api/v1/etl/<dataset_id>
```
Ejecuta el ETL para el dataset especificado en la URL.

**Ejemplo:**
```bash
curl -X POST https://tu-servicio.onrender.com/api/v1/etl/bcra_infomondia_series
```

### Ejecutar ETL (por Body)
```
POST /api/v1/etl
Content-Type: application/json

{
  "dataset_id": "bcra_infomondia_series"
}
```

**Ejemplo:**
```bash
curl -X POST https://tu-servicio.onrender.com/api/v1/etl \
  -H "Content-Type: application/json" \
  -d '{"dataset_id": "bcra_infomondia_series"}'
```

## Programar Ejecuciones con Cronjobs Externos

Cada pipeline tiene su propio período de ejecución. Configura cronjobs externos para ejecutar cada dataset según su frecuencia requerida.

### Servicios Recomendados

- [Cron-job.org](https://cron-job.org/) - Gratis, fácil de usar
- [EasyCron](https://www.easycron.com/) - Plan gratuito disponible
- [UptimeRobot](https://uptimerobot.com/) - Incluye monitoreo HTTP
- [GitHub Actions](https://docs.github.com/en/actions) - Si tu código está en GitHub

### Ejemplo de Configuración

Supongamos que tienes 3 pipelines con diferentes frecuencias:

1. **bcra_infomondia_series** - Diario a las 3:00 AM
2. **indec_emae** - Semanal los lunes a las 2:00 AM
3. **indec_ipc** - Mensual el día 1 a las 1:00 AM

#### Configuración en Cron-job.org:

**Cronjob 1 - bcra_infomondia_series:**
- URL: `https://tu-servicio.onrender.com/api/v1/etl/bcra_infomondia_series`
- Método: POST
- Frecuencia: Diario a las 3:00 AM
- Zona horaria: Tu zona horaria

**Cronjob 2 - indec_emae:**
- URL: `https://tu-servicio.onrender.com/api/v1/etl/indec_emae`
- Método: POST
- Frecuencia: Semanal (Lunes) a las 2:00 AM

**Cronjob 3 - indec_ipc:**
- URL: `https://tu-servicio.onrender.com/api/v1/etl/indec_ipc`
- Método: POST
- Frecuencia: Mensual (día 1) a las 1:00 AM

### Ventajas de este Enfoque

✅ **Escalabilidad**: Cada pipeline se ejecuta independientemente  
✅ **Control**: Puedes ajustar la frecuencia de cada pipeline sin afectar otros  
✅ **Monitoreo**: Puedes monitorear cada pipeline por separado  
✅ **Flexibilidad**: Fácil agregar o quitar pipelines sin cambiar código

## Deployment Manual (Alternativa)

Si prefieres configurar manualmente sin usar `render.yaml`:

1. Ve a [Render Dashboard](https://dashboard.render.com/)
2. Haz clic en "New +" y selecciona "Web Service"
3. Conecta tu repositorio
4. Configura:
   - **Name**: `ingestor-reader-api`
   - **Environment**: `Python 3`
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python server.py`
   - **Plan**: Elige según tus necesidades (Free tier disponible)

5. Configura las variables de entorno (ver Paso 3 arriba)

## Consideraciones Importantes

### Free Tier de Render
- El servicio puede "dormir" después de 15 minutos de inactividad
- El primer request después de dormir puede tardar ~30 segundos
- Considera usar un plan pago para producción

### Límites de Tiempo
- Free tier: 750 horas/mes
- Los procesos pueden ejecutarse hasta 1 hora (configurable)

### Logs
Los logs están disponibles en el dashboard de Render. Revisa los logs si hay problemas con el deployment.

### Configuración de AWS
Asegúrate de que las credenciales de AWS tengan los permisos necesarios:
- Acceso a S3 (para data loading y state management)
- Acceso a DynamoDB (para lock management, si lo usas)

## Troubleshooting

### El servicio no inicia
- Verifica que todas las variables de entorno estén configuradas
- Revisa los logs en el dashboard de Render
- Asegúrate de que `requirements.txt` incluya todas las dependencias (incluyendo Flask)

### Error de conexión a AWS
- Verifica que `AWS_ACCESS_KEY_ID` y `AWS_SECRET_ACCESS_KEY` sean correctos
- Verifica que `AWS_REGION` esté configurado correctamente
- Revisa los permisos IAM de las credenciales

### El ETL falla
- Revisa los logs del servicio en el dashboard de Render
- Verifica que el `dataset_id` en la URL sea válido y corresponda a un archivo en `config/datasets/`
- Asegúrate de que los archivos de configuración en `config/datasets/` estén presentes en el repositorio
- Usa el endpoint `/api/v1/datasets` para verificar qué datasets están disponibles

## Actualizar el Deployment

Render detecta automáticamente los cambios en tu repositorio y redeploya el servicio. Puedes también:

1. Ir al dashboard de Render
2. Seleccionar tu servicio
3. Hacer clic en "Manual Deploy" → "Deploy latest commit"

