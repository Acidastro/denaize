import io
import os
import zipfile

import pandas as pd
from fastapi import Response
from statsmodels.tsa.seasonal import seasonal_decompose


def create_files(
        file_name,
        daily_aggregated_filename,
        by_average_filename,
        sheet_name,
        n_rows,
        well_number,
):
    """
    Create 2 new Excel file
    :param well_number: номер клапана
    :param n_rows: количество считываемых строк
    :param sheet_name: имя страницы
    :param file_name: полное имя полученного файла с форматом
    :param daily_aggregated_filename: имя первого файла
    :param by_average_filename: имя второго файла
    """

    df = pd.read_excel(
        file_name,
        sheet_name=sheet_name,
        nrows=n_rows,
    )
    df_well = df.loc[
        df['Скважина'] == well_number,
        ['Дата замера', 'Обводненность(объемная)']
    ]

    df_well.set_index('Дата замера', inplace=True)
    df_well['Month'] = df_well.index.month
    df_well['Y-m'] = df_well.index.year
    df_well['Y-m'] = df_well['Y-m'].astype(str) + '-' + df_well[
        'Month'].astype(str)

    series = df.loc[
        df['Скважина'] == 101, ['Дата замера', 'Обводненность(объемная)']]
    series.set_index('Дата замера', inplace=True)

    result = seasonal_decompose(series, model='additive', )
    df_well['trend_sesonialclean'] = result.trend

    df_well.to_excel(daily_aggregated_filename)

    by_average = df_well.groupby('Y-m').apply(
        lambda x: x['trend_sesonialclean'].mean())
    by_average.to_excel(by_average_filename)
    return True


def response_zip(filenames: list):
    zip_filename = "archive.zip"

    s = io.BytesIO()
    with zipfile.ZipFile(s, 'w') as zf:
        for fpath in filenames:
            fdir, f_name = os.path.split(fpath)
            zf.write(fpath, f_name)

    resp = Response(
        s.getvalue(), media_type="application/x-zip-compressed",
        headers={
            'Content-Disposition': f'attachment;filename={zip_filename}'
        })
    return resp
