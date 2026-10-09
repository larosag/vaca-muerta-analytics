const SQL = `SELECT anio, mes, barriles_dia, participacion_pct, variacion_mom_pct, variacion_yoy_pct FROM energia.upstream.vw_vaca_muerta_kpis ORDER BY anio DESC, mes DESC LIMIT 36`;
const KEYS = ['anio','mes','barriles_dia','participacion_pct','variacion_mom_pct','variacion_yoy_pct'];
const pause = ms => new Promise(resolve => setTimeout(resolve, ms));

export async function GET() {
  const host = process.env.DATABRICKS_HOST;
  const token = process.env.DATABRICKS_TOKEN;
  const warehouse = process.env.DATABRICKS_WAREHOUSE_ID;
  if (!host || !token || !warehouse) {
    return Response.json({error:'Faltan variables de entorno de Databricks'}, {status:500});
  }
  const base = host.replace(/\/$/, '');
  const headers = {Authorization: `Bearer ${token}`, 'Content-Type':'application/json'};
  try {
    const start = await fetch(`${base}/api/2.0/sql/statements`, {
      method:'POST', headers,
      body:JSON.stringify({warehouse_id:warehouse, statement:SQL, wait_timeout:'10s', disposition:'INLINE', format:'JSON_ARRAY'})
    });
    if (!start.ok) throw new Error(`Statement HTTP ${start.status}`);
    let result = await start.json();
    for (let i = 0; i < 15 && result.status?.state !== 'SUCCEEDED'; i++) {
      if (['FAILED','CANCELED','CLOSED'].includes(result.status?.state)) throw new Error('Consulta SQL fallida');
      if (!result.statement_id) throw new Error('Falta statement_id');
      await pause(1000);
      const poll = await fetch(`${base}/api/2.0/sql/statements/${result.statement_id}`, {headers});
      if (!poll.ok) throw new Error(`Polling HTTP ${poll.status}`);
      result = await poll.json();
    }
    if (result.status?.state !== 'SUCCEEDED') return Response.json({error:'El Warehouse aún está iniciando, reintentá'}, {status:503});
    if (result.result?.next_chunk_internal_link) throw new Error('Resultado paginado inesperadamente');
    const historial = (result.result?.data_array || []).map(row =>
      Object.fromEntries(KEYS.map((k,i) => [k, row[i] == null ? null : Number(row[i])]))
    );
    return Response.json({ultimo_mes:historial[0] || null, historial}, {
      headers:{'Cache-Control':'public, s-maxage=3600'}
    });
  } catch (error) {
    console.error('Error consultando Databricks:', error);
    return Response.json({error:'No se pudo consultar Databricks'}, {status:502});
  }
}
