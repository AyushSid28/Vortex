from fastapi import FastAPI
from prometheus_client import make_asgi_app
from api.routes import workflows,runs,agents


app=FastAPI(
    title="Vortex",
    description="AI Agent Orchestration Engine-define,execute,monitor and scale multi-agent workflows",
    version="0.1.0",
)

app.include_router(workflows.router)
app.include_router(runs.router)
app.include_router(agents.router)

metrics_app=make_asgi_app()
app.mount("/metrics",metrics_app)

@app.get("/health")
async def health():
    return {"status":"ok","service":"vortex"}