# internal imports
from typing import Dict, Optional, List

# external imports
import asyncio
import gspread
import gspread_asyncio
from google.oauth2.service_account import Credentials


def get_creds(filename=gspread.auth.DEFAULT_SERVICE_ACCOUNT_FILENAME, scopes=gspread.auth.DEFAULT_SCOPES):
    """Retrieve gsheets api credentials.

    Uses gspread's default location/scopes by default.
    """
    creds = Credentials.from_service_account_file(
        filename, scopes=scopes
    )
    return creds


def a1_from_list(row, col, data):
    return f'{gspread.utils.rowcol_to_a1(row, col)}:{gspread.utils.rowcol_to_a1(row, col + len(data))}'


async def get_worksheet(sheet_name, worksheet_name):
    agcm = gspread_asyncio.AsyncioGspreadClientManager(get_creds)
    agc = await agcm.authorize()
    sh = await agc.open(sheet_name)
    if worksheet_name is None:
        return await sh.get_worksheet(0)
    return await sh.worksheet(worksheet_name)


async def add_new_row(sheet, data):
    await sheet.append_row(data)


async def update_row(sheet, row, col, data):
    a1 = a1_from_list(row, col, data)
    data = [[d for d in data]]
    await sheet.batch_update([{'range': a1, 'values': data}])


async def clear_worksheet(sheet_name, worksheet_name):
    ws = await get_worksheet(sheet_name, worksheet_name)
    await ws.clear()


async def init_metrics(sheet_name: str, exp_name: str, metric_keys: List[str], worksheet_name: Optional[str] = None, phases: Optional[List[str]] = None) -> None:
    ws = await get_worksheet(sheet_name, worksheet_name)

    exp_row = len(await ws.col_values(1)) + 4
    
    if phases is None:
        phase = [None]

    phase_cols = {phase: 1 + (len(metric_keys) + 2) * i for i, phase in enumerate(phases)}

    await update_row(ws, exp_row, 1, [exp_name])
    for phase in phases:
        await update_row(ws, exp_row + 1, phase_cols[phase], [phase])
        await update_row(ws, exp_row + 2, phase_cols[phase], ['iter'] + metric_keys)
    
    return exp_row, phase_cols


async def upload_metrics(sheet_name: str, metrics: Dict[str, int], worksheet_name: Optional[str] = None, epoch: Optional[int] = None, row: Optional[int] = None, col: Optional[int] = 1) -> None:
    """
    Upload metrics to googlesheets.
    """
    ws = await get_worksheet(sheet_name, worksheet_name)

    if row is None:
        row = len(await ws.col_values(col)) + 1

    # Add data
    await update_row(ws, row, col, [epoch] + list(metrics.values()))


async def begin_experiment(sheet_name: str, exp_name: str, args: Dict[str, int], worksheet_name: Optional[str] = None):
    """ Adds a new row for a given experiment name, if necessary, and returns the cell object.

    Add way to find args keys and insert val in correct columns
    """
    ws = await get_worksheet(sheet_name, worksheet_name)

    exp_row = len(await ws.col_values(1)) + 1
    await update_row(ws, exp_row, 1, [exp_name])
    return exp_row


async def upload_results(sheet_name: str, exp_name: str, results: Dict[str, int], worksheet_name: Optional[str] = None, row: Optional[int] = 1, col: Optional[int] = 1) -> None:
    """
    Upload the results to googlesheets. If no row with the exp_name
    exists, then a new row will be added. If the experiment does
    exist, the row will simply be updated.
    """
    ws = await get_worksheet(sheet_name, worksheet_name)

    data = [v for v in results.values()]
    await update_row(ws, row, col, data)
