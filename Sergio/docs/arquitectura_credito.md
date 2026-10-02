# Arquitectura del prototipo: chat de crédito con derivación a humano

Estado: borrador v0.1. La política de crédito es **sintética y provisional** (`policy/credit_policy.yaml`).

## 1. Alcance

Chat de atención con tres rutas (acordadas en la reunión del 28-sep):

1. **Oferta preaprobada:** el cliente pregunta o el sistema propone un crédito según su perfil.
2. **Recálculo en vivo:** el cliente aporta datos nuevos (ingreso, ingreso de la pareja, deuda cancelada) y se recalcula capacidad, monto máximo y tasa. Todo lo declarado queda "sujeto a verificación".
3. **Derivación a humano:** datos faltantes, casos límite, mora, cliente no activo, aumentos de ingreso muy grandes o preguntas fuera de las tablas. El agente recibe un resumen.

Casos obligatorios de la evaluación: una resolución normal, una solicitud ambigua o no soportada y una que requiere humano.

## 2. Decisión de diseño principal

| Función | Quién la hace | Por qué |
|---|---|---|
| Entender el mensaje, aclarar, extraer datos | LLM (salida JSON validada) | Es lenguaje, no decisión. |
| Elegibilidad, monto máximo, tasa | **Motor determinista** (`app/credit_engine.py`) | El enunciado prohíbe que el modelo invente reglas o apruebe crédito. |
| Permisos y datos del cliente | Capa de herramientas | Se aplica por `customer_id` de la sesión, no por lo que diga el modelo. |
| Redactar la respuesta | LLM, sobre hechos devueltos por herramientas | Solo se reporta lo verificado. |
| Resumen para el agente | LLM, con esquema fijo | Condensa: solicitud, hechos verificados, acciones, evidencia, preguntas abiertas. |
| Riesgo predictivo | **No se entrena** | En los datos `credit_score` no correlaciona con `days_past_due` (r = −0,004). |

Componente aprendido (exigido por el enunciado): clasificador de intención y ruteo (embeddings multilingües + regresión logística), entrenado con conversaciones **generadas por el equipo** y evaluado en un conjunto escrito a mano. Baselines: reglas de palabras clave y el LLM en zero-shot. Las etiquetas son sintéticas y se declaran así.

## 3. Flujo

```
Cliente ─► Chat UI ─► API (FastAPI, sesión de prueba JWT, trace_id)
                          │
                          ▼
                   Orquestador (máquina de estados)
      ┌──────────────┼───────────────────────────┐
      ▼              ▼                           ▼
 LLM: idioma +   Herramientas (authz)      Motor de política
 extracción JSON   ├ perfil crediticio      (credit_engine + YAML)
 (español canónico)├ productos y deuda
      │            └ crear caso / derivar
      ▼                        ▼
 Respuesta en el idioma   Handoff: cola + resumen JSON ─► vista del agente
 del cliente (hechos verificados)
```

Reglas transversales: acciones con confirmación explícita, reintentos acotados, respuesta segura si falla el LLM o una herramienta, y logs JSONL con `trace_id`, latencia y costo por turno. Las salidas de herramientas y los mensajes del cliente se tratan como datos, nunca como instrucciones.

## 4. Datos: medallion en Databricks

| Capa | Contenido |
|---|---|
| Bronze | Ingesta cruda desde S3 (particiones `year/month/day`), columna de rescate por schema evolution. |
| Silver | Tipos, deduplicación, normalización ("México"/"Mexico"), verificación de claves, cuarentena. |
| Gold | Tablas de consumo (abajo). |

Gold para servir al chat: `gold_customer_credit_profile` (**ya prototipada**, un registro por cliente con score, banda, ingreso, deuda mensual estimada, mora máxima y productos activos), `gold_product_offer_rates` (tasas base por producto) y `gold_open_cases`.
Gold para métricas: `gold_kpi_credit_daily`, `gold_dq_metrics` (nulos, duplicados, huérfanos y frescura por corrida), `gold_eval_runs` y `gold_handoff_events` (retorno desde los logs del app).

El prototipo consume un snapshot exportado (parquet/DuckDB), sin llamar a Databricks en cada turno. Consulta en línea con SQL Warehouse y filtros por fila queda como camino de producción documentado.

## 5. Estrategia de idioma

Propuesta del equipo: usar el LLM para llevar todo a un idioma base. Se adopta con un ajuste:

- El LLM **no traduce el mensaje como paso separado**. Lo entiende y devuelve un JSON con **enumeraciones canónicas en español** (`intent`, `product`, `amount`, `income`, `language`). Así hay un solo camino de lógica y no se acumulan errores de traducción.
- La respuesta se redacta en el idioma detectado del cliente a partir de los hechos verificados. Las frases con cifras, tasas o avisos de verificación salen de plantillas revisadas en español y portugués, no de prosa libre del modelo.
- Los números y montos se interpretan en **código**, con reglas de formato (1.500,00 frente a 1,500.00), no por el LLM.
- Acentos (México, Colombia, Argentina) y mezcla portugués/español: se prueban en el conjunto de evaluación.
- El conjunto de prueba en portugués lo escribe el equipo (los datos del organizador no traen portugués) y se reportan métricas por idioma, con sus limitaciones de muestra.

**Pendiente:** confirmar con los organizadores si el portugués es un requisito real o un error del documento. El enunciado lo pide de forma explícita ("demonstrate interactions in Spanish and Portuguese"), así que hasta tener respuesta se implementa.

## 6. Docker y despliegue

Una imagen para el app (FastAPI + UI), usuario no root, `HEALTHCHECK`, dependencias fijadas, semilla fija. Los datos **no** van en la imagen: se monta un volumen con el snapshot gold. La clave del LLM entra por variable de entorno. `docker compose up` levanta el app con el snapshot de ejemplo.

## 7. Riesgos y límites conocidos

- La política es inventada por el equipo: los resultados dependen de ella y no hay verdad de terreno externa. Se declara así en todo reporte.
- La deuda existente se **estima** con supuestos (el dataset no trae cuotas): tarjeta 5% del saldo, préstamo personal 36 meses, hipoteca 180 meses.
- Todos los productos de México están en USD y los ingresos en MXN: se convierte con la tasa de la última fecha.
- Nulos: 15% de `credit_score` y 20% de ingreso. Es la primera causa de derivación por datos faltantes.
- Los transcripts y `detected_intents` no sirven para detectar interés en crédito (texto repetitivo, intención constante).
- El estado civil, el género y el acento no entran a la decisión. Se auditan resultados por grupo.
- Confirmar con los organizadores que se puede enviar datos sintéticos a una API externa de LLM. Si no, hay que usar un modelo local.

## 8. Pendientes de decisión

1. API de Claude o modelo local para el LLM.
2. Alcance del workspace de Databricks (edición gratuita o no) y si puede leer el bucket del organizador.
3. Confirmación del portugués con los organizadores.
4. Ajuste de los números de la política (20% de endeudamiento, bandas de score y spreads) por el equipo.
