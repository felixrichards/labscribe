# internal imports
from typing import Dict, Optional, List

# external imports
import gspread


def add_new_row(sheet, data):
    sheet.append_row(data)


def update_row(sheet, cell, data):
    a1 = f'{cell.address}:{gspread.utils.rowcol_to_a1(cell.row, cell.col + len(data))}'
    data = [[d] for d in data]
    sheet.update(a1, data, major_dimension='COLUMNS')


def get_worksheet(sheet_name, worksheet_name):
    gc = gspread.service_account()
    sh = gc.open(sheet_name)
    if worksheet_name is None:
        worksheet_name = sh.sheet1.title
    return sh.worksheet(worksheet_name)


def clear_worksheet(sheet_name, worksheet_name):
    ws = get_worksheet(sheet_name, worksheet_name)
    ws.clear()


def init_metrics(sheet_name: str, exp_name: str, metric_keys: List[str], worksheet_name: Optional[str] = None, phases: Optional[List[str]] = None) -> None:
    ws = get_worksheet(sheet_name, worksheet_name)

    exp_row = len(ws.col_values(1)) + 4
    
    if phases is None:
        phase = [None]

    phase_cols = {phase: 1 + (len(metric_keys) + 2) * i for i, phase in enumerate(phases)}

    update_row(ws, ws.cell(exp_row, 1), [exp_name])
    for phase in phases:
        update_row(ws, ws.cell(exp_row + 1, phase_cols[phase]), [phase])
        update_row(ws, ws.cell(exp_row + 2, phase_cols[phase]), ['iter'] + metric_keys)
    
    return exp_row, phase_cols


def upload_metrics(sheet_name: str, metrics: Dict[str, int], worksheet_name: Optional[str] = None, iter: Optional[int] = None, col: Optional[int] = 1) -> None:
    """
    Upload metrics to googlesheets.
    """
    ws = get_worksheet(sheet_name, worksheet_name)

    # Add data
    update_row(ws, ws.cell(len(ws.col_values(col)) + 1, col), [iter] + list(metrics.values()))


def begin_experiment(sheet_name: str, exp_name: str, args: Dict[str, int], worksheet_name: Optional[str] = None):
    """ Adds a new row for a given experiment name, if necessary, and returns the cell object.

    Add way to find args keys and insert val in correct columns
    """
    ws = get_worksheet(sheet_name, worksheet_name)

    exp_row = len(ws.col_values(1)) + 1
    cell = ws.cell(exp_row, 1)
    update_row(ws, cell, [exp_name])
    return cell



def upload_results(sheet_name: str, exp_name: str, results: Dict[str, int], worksheet_name: Optional[str] = None, row: Optional[int] = 1, col: Optional[int] = 1) -> None:
    """
    Upload the results to googlesheets. If no row with the exp_name
    exists, then a new row will be added. If the experiment does
    exist, the row will simply be updated.
    """
    ws = get_worksheet(sheet_name, worksheet_name)

    data = [v for v in results.values()]
    cell = ws.cell(row,  col)
    update_row(ws, cell, data)