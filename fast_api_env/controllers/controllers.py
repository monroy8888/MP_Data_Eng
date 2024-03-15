from typing import Annotated
import logging
from fastapi import APIRouter, File, UploadFile, HTTPException, status


from ..models.main_models import UploadResponse, FunctionType, MessageResponse, MessageRequest



router = APIRouter()
log = logging.getLogger(__name__)


@router.post('/new_message/')
async def post_message(function: UploadResponse):
    try:
        pass
    except Exception as e:
        log.info(f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        ) from e



