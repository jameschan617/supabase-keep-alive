import os
import json
from typing import List, Dict, Any

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

# =============================
# 环境变量与配置解析
# =============================
SUPABASE_CONFIG_RAW = os.getenv("SUPABASE_CONFIG", "[]")

config_list: List[Dict[str, Any]] = []
startup_error: str | None = None

try:
    # 解析 JSON 数组
    config_list = json.loads(SUPABASE_CONFIG_RAW)
    if not isinstance(config_list, list):
        raise ValueError("SUPABASE_CONFIG 必须是 JSON 数组格式")
    if len(config_list) == 0:
        raise ValueError("SUPABASE_CONFIG 不能为空数组")

    # 校验每一项的必要字段
    required_fields = {"name", "supabase_url", "supabase_key", "table_name"}
    for idx, conf in enumerate(config_list):
        missing = required_fields - conf.keys()
        if missing:
            raise ValueError(
                f"配置 index={idx} 缺少字段: {', '.join(missing)}"
            )
except Exception as e:
    startup_error = str(e)
    print(f"Startup Error: {startup_error}")

# =============================
# 工具函数
# =============================
def _perform_ping(conf: dict) -> tuple[bool, str]:
    try:
        # 初始化 Supabase 客户端
        supabase = create_client(conf["supabase_url"], conf["supabase_key"])
        
        # 1. 查询所有 schema（排除系统内置 schema）
        schemas_response = supabase.sql("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        """).execute()
        
        schemas = [row["schema_name"] for row in schemas_response.data]
        if not schemas:
            return False, f"No schemas found for {conf['name']}"
        
        # 2. 遍历每个 schema，查询其中的表
        all_tables = {}
        for schema in schemas:
            tables_response = supabase.sql(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema}' AND table_type = 'BASE TABLE'
            """).execute()
            
            tables = [row["table_name"] for row in tables_response.data]
            if tables:
                all_tables[schema] = tables
        
        if not all_tables:
            return False, f"No tables found in any schema for {conf['name']}"
        
        # 3. 选择第一个找到的表执行轻量查询
        first_schema = next(iter(all_tables.keys()))
        first_table = all_tables[first_schema][0]
        
        response = supabase.table(f"{first_schema}.{first_table}").select("*", count="exact").limit(1).execute()
        
        # 构建包含所有 schema 和表的信息
        schema_info = ", ".join([f"{s}: [{', '.join(ts)}]" for s, ts in all_tables.items()])
        return True, f"Pinged {conf['name']} (schemas and tables: {schema_info}) successfully"
    
    except Exception as e:
        return False, f"Error pinging {conf['name']}: {str(e)}"


def _get_conf_by_index(idx: int):
    if idx < 0 or idx >= len(config_list):
        raise HTTPException(status_code=404, detail=f"index {idx} 不存在")
    return config_list[idx]


def _get_conf_by_name(name: str):
    for conf in config_list:
        if conf["name"] == name:
            return conf
    raise HTTPException(status_code=404, detail=f"name '{name}' 未找到对应配置")


# =============================
# 路由
# =============================


@app.get("/api/keepalive")
@app.get("/api/keepalive/all")
async def keepalive_all(request: Request):
    """遍历所有配置并执行 keep‑alive"""
    if startup_error:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Startup failed: {startup_error}"})

    success_count = 0
    for idx, conf in enumerate(config_list):
        success, msg = _perform_ping(conf)
        if success:
            success_count += 1

    if success_count == len(config_list):
        return JSONResponse(status_code=200, content={"status": "success", "message": "ok"})
    elif success_count > 0:
        return JSONResponse(status_code=500, content={"status": "error", "message": "partial_failure"})
    else:
        return JSONResponse(status_code=500, content={"status": "error", "message": "all_failure"})

@app.get("/api/keepalive/index")
@app.get("/api/keepalive/index/{idx}")
async def keepalive_by_index(request: Request, idx: int = 0):
    if startup_error:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Startup failed: {startup_error}"})

    conf = _get_conf_by_index(idx)
    success, msg = _perform_ping(conf)
    status_code = 200 if success else 500
    return JSONResponse(status_code=status_code, content={"status": "success" if success else "error", "message": msg})


@app.get("/api/keepalive/name/{conf_name}")
async def keepalive_by_name(request: Request, conf_name: str):
    if startup_error:
        return JSONResponse(status_code=500, content={"status": "error", "message": f"Startup failed: {startup_error}"})

    conf = _get_conf_by_name(conf_name)
    success, msg = _perform_ping(conf)
    status_code = 200 if success else 500
    return JSONResponse(status_code=status_code, content={"status": "success" if success else "error", "message": msg})
