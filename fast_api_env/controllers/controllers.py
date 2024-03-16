from typing import Annotated
import logging
from fastapi import APIRouter, HTTPException, status
from ..services.db_service import select_data, select_query


router = APIRouter()
log = logging.getLogger(__name__)


@router.get('/prints_table/<table>')
async def prints_table(table:str):
    """
     Campo que indique si se hizo click o no.
    """

    try:
        response_data = await select_data(table)
        return {"data": response_data}
    except Exception as e:
        log.info(f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        ) from e


@router.get('/prints_click_df/')
async def prints_click_df():
    """
     Campo que indique si se hizo click o no.
    """

    try:
        response = await select_query('prints_click')
        return response
    except Exception as e:
        log.info(f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        ) from e


@router.get('/prints_view_quantity_df/')
async def prints_view_quantity_df():
    """
     Cantidad de veces que el usuario vio cada value prop en las 3 semanas previas a ese print.
    """

    try:
        response = await select_query('prints_view_quantity')
        return response
    except Exception as e:
        log.info(f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        ) from e


@router.get('/taps_clicks_quantity_df/')
async def taps_clicks_quantity_df():
    """
     Contar la cantidad de veces que el usuario clickeó cada value prop en las 3 semanas previas
    """

    try:
        response = await select_query('taps_clicks_quantity')
        return response
    except Exception as e:
        log.info(f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        ) from e


@router.get('/pays_count_df/')
async def pays_count_df():
    """
     Contar la cantidad de pagos que el usuario realizó para cada value prop en las 3 semanas previas
    """

    try:
        response = await select_query('pays_count')
        return response
    except Exception as e:
        log.info(f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        ) from e


@router.get('/pays_total_df/')
async def pays_total_df():
    """
     Calcular importes acumulados que el usuario gastó para cada value prop en las 3 semanas previas
    """

    try:
        response = await select_query('pays_total')
        return response
    except Exception as e:
        log.info(f"Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal Server Error",
        ) from e

