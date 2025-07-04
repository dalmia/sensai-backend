from fastapi import APIRouter, HTTPException
from typing import List, Dict

from api.db.batch import (
    create_batch_with_members as create_batch_with_members_in_db,
    get_all_batches_for_cohort as get_all_batches_for_cohort_from_db,
    get_batch_by_id as get_batch_by_id_from_db,
    delete_batch as delete_batch_from_db,
    get_batches_for_user_in_cohort as get_batches_for_user_in_cohort_from_db,
    update_batch_name_and_members,
)
from api.models import (
    CreateBatchRequest,
    CreateBatchResponse,
    AddMembersToBatchRequest,
    RemoveMembersFromBatchRequest,
    UpdateBatchRequest,
)

router = APIRouter()


@router.get("/")
async def get_all_batches_for_cohort(cohort_id: int) -> List[Dict]:
    """Get all batches for a cohort"""
    return await get_all_batches_for_cohort_from_db(cohort_id)


@router.post("/", response_model=CreateBatchResponse)
async def create_batch(request: CreateBatchRequest) -> CreateBatchResponse:
    """Create a new batch by name, optionally with initial members"""
    try:
        batch_id = await create_batch_with_members_in_db(
            request.name, request.cohort_id, request.user_ids
        )
        return {"id": batch_id}
    except Exception as e:
        if "already in the batch" in str(e):
            raise HTTPException(status_code=400, detail=str(e))
        elif "does not belong to this organization" in str(e):
            raise HTTPException(status_code=403, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{batch_id}")
async def get_batch_by_id(batch_id: int) -> Dict:
    """Get batch details including all members"""
    batch_data = await get_batch_by_id_from_db(batch_id)
    if not batch_data:
        raise HTTPException(status_code=404, detail="Batch not found")

    return batch_data


@router.delete("/{batch_id}")
async def delete_batch(batch_id: int):
    """Delete a batch"""
    await delete_batch_from_db(batch_id)
    return {"success": True}


@router.put("/{batch_id}")
async def update_batch(batch_id: int, request: UpdateBatchRequest):
    """Update batch name and members"""
    return await update_batch_name_and_members(
        batch_id,
        request.name,
        request.members_added,
        request.members_removed,
    )


@router.get("/user/{user_id}/cohort/{cohort_id}")
async def get_batches_for_user_in_cohort(user_id: int, cohort_id: int) -> List[Dict]:
    """List all batches for a user id in a cohort based on cohort id and user id -
    return each batch name and batch id and role in batch"""
    try:
        return await get_batches_for_user_in_cohort_from_db(user_id, cohort_id)
    except Exception as e:
        if "not a member of the specified cohort" in str(e):
            raise HTTPException(status_code=403, detail=str(e))
        elif "Cohort not found" in str(e):
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=500, detail=str(e))
