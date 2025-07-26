from fastapi import APIRouter, HTTPException, Query
from typing import List, Optional
from api.models import Integration, CreateIntegrationRequest, UpdateIntegrationRequest
from api.db.integration import (
    create_integration,
    get_integration,
    list_integrations,
    update_integration,
    delete_integration,
)

router = APIRouter()

@router.post("/", response_model=Integration)
async def create_integration_api(data: CreateIntegrationRequest):
    integration_id = await create_integration(data)
    integration = await get_integration(integration_id)
    if not integration:
        raise HTTPException(status_code=500, detail="Failed to create integration")
    return integration


@router.get("/{integration_id}", response_model=Integration)
async def get_integration_api(integration_id: int):
    integration = await get_integration(integration_id)
    if not integration:
        raise HTTPException(status_code=404, detail="Integration not found")
    return integration


@router.get("/", response_model=List[Integration])
async def list_integrations_api(user_id: Optional[int] = Query(None)):
    return await list_integrations(user_id=user_id)


@router.put("/{integration_id}", response_model=Integration)
async def update_integration_api(integration_id: int, data: UpdateIntegrationRequest):
    updated = await update_integration(integration_id, data)
    if not updated:
        raise HTTPException(status_code=404, detail="Integration not found or not updated")
    integration = await get_integration(integration_id)
    return integration


@router.delete("/{integration_id}")
async def delete_integration_api(integration_id: int):
    deleted = await delete_integration(integration_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Integration not found or not deleted")
    return {"success": True}