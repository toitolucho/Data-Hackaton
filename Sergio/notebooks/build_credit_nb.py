import nbformat as nbf
nb = nbf.v4.new_notebook()
C = []
md = lambda s: C.append(nbf.v4.new_markdown_cell(s.strip()))
code = lambda s: C.append(nbf.v4.new_code_cell(s.strip()))

md("""
# Baseline de elegibilidad de crédito
Hackathon Factored AI & Data 2026. Tema elegido por el equipo: **crédito** (oferta preaprobada, recálculo en vivo de capacidad de pago y derivación a un humano).

**Qué es y qué no es este baseline**
- La política es **sintética y provisional** (`policy/credit_policy.yaml`), definida por el equipo. No hay resultados reales de solicitudes de crédito en los datos, así que **no existe una verdad de terreno externa**.
- Por eso el baseline mide **el comportamiento de la política sobre la base de clientes** y lo compara con dos referencias simples. La evaluación del chat consistirá en verificar que reproduce fielmente lo que decide el motor (sin inventar reglas ni aprobar por su cuenta).
- No se entrena ningún modelo de riesgo: en estos datos `credit_score` no correlaciona con `days_past_due` (r = -0,004).
- Medición offline sobre datos sintéticos. No es una mejora medida en producción.
""")
code("""
import sys, json, warnings
import pandas as pd, numpy as np, matplotlib.pyplot as plt
from pathlib import Path
from IPython.display import display
warnings.filterwarnings('ignore')
pd.set_option('display.width', 200); pd.set_option('display.max_columns', 40)
ROOT = Path('..').resolve(); sys.path.insert(0, str(ROOT))
from app import credit_engine as ce
DATA, GOLD, OUT = ROOT/'data', ROOT/'gold', ROOT/'notebooks'/'outputs'
GOLD.mkdir(exist_ok=True); OUT.mkdir(exist_ok=True)
POLICY = ce.load_policy()
print('política', POLICY['version'], '| sintética:', POLICY['synthetic'])
""")

md("## 1. Carga y contrato de datos")
code("""
cu = pd.read_csv(DATA/'customers.csv', encoding='utf-8-sig')
pr = pd.read_csv(DATA/'products.csv', encoding='utf-8-sig')
fx = pd.read_csv(DATA/'daily_exchange_rates.csv', encoding='utf-8-sig')
cu['country'] = cu.country.replace({'Mexico': 'México'})

checks = {
 'customers: PK única': cu.customer_id.is_unique,
 'products: PK única': pr.product_id.is_unique,
 'products: customer_id existe en customers': pr.customer_id.isin(cu.customer_id).all(),
 'customers: credit_score en 300-850 (no nulos)': cu.credit_score.dropna().between(300, 850).all(),
 'customers: ingreso >= 0 (no nulos)': (cu.estimated_monthly_income.dropna() >= 0).all(),
 'products: days_past_due >= 0 (no nulos)': (pr.days_past_due.dropna() >= 0).all(),
 'products: current_balance >= 0': (pr.current_balance.dropna() >= 0).all(),
 'fx: existe tasa para cada par de monedas en la última fecha': fx[fx.date == fx.date.max()].shape[0] == 12,
}
qc = pd.Series(checks, name='ok').map({True:'PASS', False:'FAIL'}).to_frame(); display(qc)
print('Nulos: credit_score %.1f%% | ingreso %.1f%%' % (100*cu.credit_score.isna().mean(), 100*cu.estimated_monthly_income.isna().mean()))
print('Clientes sin productos:', (~cu.customer_id.isin(pr.customer_id)).sum())
m = pr.merge(cu[['customer_id','country']], on='customer_id')
print('Moneda de productos por país del cliente (hallazgo: todos los productos de México están en USD):')
display(pd.crosstab(m.country, m.currency))
""")

md("""
## 2. Capa gold: perfil crediticio por cliente
Se construye una tabla por cliente con lo que necesita el motor. Es la tabla que el chat consultará (no las transaccionales).

**Supuestos declarados** (el dataset no trae cuotas): cuota mínima de tarjeta = 5% del saldo; préstamos personales a 36 meses restantes; hipotecas a 180 meses; tasa del propio producto. Los saldos en USD o en otra moneda se convierten a la moneda del ingreso con la tasa de la **última fecha** disponible.
""")
code("""
TYPE_MAP = {'Tarjeta Crédito': 'credit_card', 'Préstamo Personal': 'personal_loan', 'Préstamo Hipotecario': 'mortgage'}
INCOME_CCY = {'México': 'MXN', 'Colombia': 'COP', 'Argentina': 'ARS'}
last = fx[fx.date == fx.date.max()]
rate = {(r.source_currency, r.target_currency): r.exchange_rate for r in last.itertuples()}
def convert(amount, src, dst):
    return amount if src == dst else amount * rate[(src, dst)]

A = POLICY['existing_debt_assumptions']
pr['ptype'] = pr.product_type.map(TYPE_MAP)
cr = pr[pr.ptype.notna() & (pr.product_status == 'Active')].merge(cu[['customer_id','country']], on='customer_id')
cr['income_ccy'] = cr.country.map(INCOME_CCY)
cr['balance_inc'] = [convert(b, s, d) for b, s, d in zip(cr.current_balance.fillna(0), cr.currency, cr.income_ccy)]
def monthly_debt(r):
    if r.ptype == 'credit_card': return r.balance_inc * A['card_min_payment_pct']
    months = A['personal_loan_remaining_months'] if r.ptype == 'personal_loan' else A['mortgage_remaining_months']
    return ce.pmt(r.balance_inc, r.interest_rate if r.interest_rate == r.interest_rate else 0.0, months)
cr['monthly_debt'] = cr.apply(monthly_debt, axis=1)
debt = cr.groupby('customer_id').monthly_debt.sum().rename('existing_monthly_debt')

# mora: máximo de días entre todos los productos de crédito del cliente (no cerrados)
credit_all = pr[pr.ptype.notna() & (pr.product_status != 'Closed')]
dpd = credit_all.groupby('customer_id').days_past_due.max().rename('max_days_past_due')
nact = pr[pr.product_status == 'Active'].groupby('customer_id').size().rename('n_active_products')

prof = cu[['customer_id','country','segment','customer_status','credit_score','estimated_monthly_income','gender','date_of_birth']].copy()
prof = prof.join(debt, on='customer_id').join(dpd, on='customer_id').join(nact, on='customer_id')
prof['existing_monthly_debt'] = prof.existing_monthly_debt.fillna(0.0)
prof['max_days_past_due'] = prof.max_days_past_due.fillna(0)
prof['n_active_products'] = prof.n_active_products.fillna(0).astype(int)
prof = prof.rename(columns={'estimated_monthly_income': 'monthly_income'})
prof['income_ccy'] = prof.country.map(INCOME_CCY)
prof['dti_existing'] = prof.existing_monthly_debt / prof.monthly_income
prof['score_band'] = prof.credit_score.map(lambda s: ce.score_band(s, POLICY) if s == s else np.nan)
prof.to_parquet(GOLD/'gold_customer_credit_profile.parquet')
print('gold_customer_credit_profile:', prof.shape)
display(prof[['monthly_income','existing_monthly_debt','dti_existing','max_days_past_due','n_active_products']].describe().round(3))
""")
code("""
# Sanidad de la estimación de deuda: ¿qué tan realista queda la carga existente?
ok = prof.dti_existing.replace([np.inf, -np.inf], np.nan).dropna()
print('DTI existente (deuda/ingreso): p50=%.3f p75=%.3f p95=%.3f | >20%%: %.1f%% | >100%%: %.1f%%' % (
    ok.median(), ok.quantile(.75), ok.quantile(.95), 100*(ok > .20).mean(), 100*(ok > 1).mean()))
print('Bandas de score (%):', (prof.score_band.value_counts(normalize=True, dropna=False).sort_index()*100).round(1).to_dict())
""")

md("""
## 3. Resultado de la política sobre la solicitud tipo
Solicitud tipo: préstamo personal a 36 meses por 3× el ingreso mensual (parámetro en la política). Ingreso **en archivo** (no declarado).
""")
code("""
REQ0 = POLICY['baseline_request']
def run(df, income_col='monthly_income', declared=False, amount_mult=None):
    mult = amount_mult or REQ0['amount_income_multiple']
    out = []
    for r in df.itertuples():
        inc = getattr(r, income_col)
        app = dict(customer_status=r.customer_status, credit_score=r.credit_score, monthly_income=inc,
                   income_is_declared=declared, existing_monthly_debt=r.existing_monthly_debt,
                   max_days_past_due=r.max_days_past_due, n_active_products=r.n_active_products)
        req = dict(product=REQ0['product'], months=REQ0['months'], amount=(inc*mult) if inc == inc else 0.0)
        d = ce.evaluate(app, req, POLICY)
        out.append((d.outcome, d.reasons[0] if d.reasons else '', d.band, d.rate_pct, d.dti_after, d.max_amount))
    return pd.DataFrame(out, columns=['outcome','reason','band','rate_pct','dti_after','max_amount'], index=df.index)
res0 = run(prof)
prof = prof.join(res0)
tab = (prof.outcome.value_counts(normalize=True)*100).round(2)
print('Resultado por cliente (%):'); display(tab.to_frame('%'))
display((prof.reason.value_counts(normalize=True)*100).round(2).to_frame('% por motivo principal'))
""")
code("""
by_country = pd.crosstab(prof.country, prof.outcome, normalize='index').mul(100).round(1)
by_segment = pd.crosstab(prof.segment, prof.outcome, normalize='index').mul(100).round(1)
display(by_country); display(by_segment)
el = prof[prof.outcome.isin(['eligible','eligible_provisional'])]
print('Tasa asignada a elegibles (%%): p10=%.1f p50=%.1f p90=%.1f' % tuple(el.rate_pct.quantile([.1,.5,.9])))
""")

md("""
## 4. Referencias (baselines) del sistema
- **B0 – todo a humano:** el 100% de las consultas de crédito llegan a un agente. Es la línea de base operativa actual conocida (sin automatización).
- **B1 – preaprobación estática por segmento:** Premium y Plus preaprobados, Basic y Student no. No mira ingreso, deuda ni mora. Sirve para mostrar el riesgo de una regla sin cálculo.
- **Sistema propuesto:** motor de política con datos del cliente + recálculo en el chat.
""")
code("""
prof['b1_preapproved'] = prof.segment.isin(['Premium','Plus'])
prof['engine_ok'] = prof.outcome.isin(['eligible','eligible_provisional'])
cmp = pd.crosstab(prof.b1_preapproved.map({True:'B1 preaprueba', False:'B1 no preaprueba'}),
                  prof.outcome, margins=True)
display(cmp)
pre = prof[prof.b1_preapproved]
print('De los preaprobados por B1: el motor los rechaza %.1f%%, pide datos %.1f%%, revisión humana %.1f%%' % (
    100*(pre.outcome=='declined').mean(), 100*(pre.outcome=='needs_data').mean(), 100*(pre.outcome=='needs_review').mean()))
nopre = prof[~prof.b1_preapproved]
print('De los NO preaprobados por B1: el motor los considera elegibles %.1f%% (oportunidad perdida)' % (100*nopre.engine_ok.mean()))
handoff = {'B0': 1.0, 'motor': float(prof.outcome.isin(['needs_review']).mean()),
           'motor_incl_needs_data': float(prof.outcome.isin(['needs_review','needs_data']).mean())}
print('Derivación a humano -> B0: 100%% | motor (solo revisión): %.1f%% | motor + datos faltantes: %.1f%%' % (
    100*handoff['motor'], 100*handoff['motor_incl_needs_data']))
""")

md("""
## 5. Recálculo en vivo: sensibilidad a datos que aporta el cliente
Caso de uso de la reunión: el cliente declara más ingreso (aumento de sueldo, ingreso de la pareja). El motor recalcula con ingreso **declarado (no verificado)**, por lo que un resultado favorable es *provisional*. Se cuenta cuántos rechazos por capacidad de pago cambian.
""")
code("""
dti_rej = prof[(prof.outcome == 'declined') & (prof.reason == 'DTI_EXCEEDED')].copy()
print('Rechazados por capacidad de pago:', len(dti_rej))
rows = []
for uplift in [0.10, 0.25, 0.50, 1.00]:
    dti_rej['inc_decl'] = dti_rej.monthly_income * (1 + uplift)
    r = run(dti_rej, income_col='inc_decl', declared=True, amount_mult=REQ0['amount_income_multiple']*1/(1+uplift))
    # se mantiene el MONTO solicitado original: amount = 3 * ingreso original
    rows.append(dict(aumento_declarado=f'+{int(uplift*100)}%',
                     pasan_a_provisional=(r.outcome=='eligible_provisional').mean(),
                     pasan_a_revision=(r.outcome=='needs_review').mean(),
                     siguen_rechazados=(r.outcome=='declined').mean()))
sens = pd.DataFrame(rows).set_index('aumento_declarado'); display((sens*100).round(1))
print('Regla: aumentos declarados mayores a', int(POLICY['declared_income']['max_uplift_without_review']*100),
      '% no deberían resolverse solos; el orquestador los envía a revisión humana.')
""")

md("""
## 6. Auditoría de equidad (solo medición)
Los atributos protegidos **no entran** a la política. Se mide si los resultados difieren por grupo para detectar disparidades de fondo (por ejemplo, vía ingreso).
""")
code("""
prof['age'] = (pd.Timestamp('2026-06-17') - pd.to_datetime(prof.date_of_birth)).dt.days // 365
prof['age_band'] = pd.cut(prof.age, [17, 25, 35, 45, 55, 65, 120], labels=['18-25','26-35','36-45','46-55','56-65','66+'])
def rate_tab(col):
    g = prof.groupby(col, observed=True)
    return pd.DataFrame({'n': g.size(), 'elegibles_%': g.engine_ok.mean()*100,
                         'rechazados_%': g.apply(lambda x:(x.outcome=='declined').mean()*100),
                         'datos_faltantes_%': g.apply(lambda x:(x.outcome=='needs_data').mean()*100)}).round(1)
for c in ['gender','country','age_band']: print(c); display(rate_tab(c))
""")

md("## 7. Resumen guardado")
code("""
summary = {
  'politica': {'version': POLICY['version'], 'sintetica': True, 'max_dti': POLICY['max_dti']},
  'solicitud_tipo': REQ0,
  'clientes': int(len(prof)),
  'resultado_pct': tab.to_dict(),
  'derivacion_humano': handoff,
  'b1_preaprobados_rechazados_por_motor_pct': float(100*(pre.outcome=='declined').mean()),
  'sensibilidad_ingreso_declarado_pct': (sens*100).round(1).to_dict(),
  'supuestos_deuda': A,
  'limites': [
    'Politica sintetica y provisional; no hay verdad de terreno externa.',
    'El dataset no trae cuotas: la deuda existente se estima con supuestos declarados.',
    'Todos los productos de Mexico estan en USD; se convierten con la tasa de la ultima fecha.',
    'credit_score no correlaciona con days_past_due: no se entrena modelo de riesgo.',
    'Medicion offline sobre datos sinteticos, no mejora en produccion.',
  ]}
(OUT/'credit_baseline_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2, default=float), encoding='utf-8')
print('guardado', OUT/'credit_baseline_summary.json')
""")
nb['cells'] = C
nb.metadata['kernelspec'] = {'name':'python3','display_name':'Python 3','language':'python'}
nbf.write(nb, 'baseline_credito.ipynb')
print('ok')
