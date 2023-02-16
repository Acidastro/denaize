import io
import os
import zipfile

import numpy as np
import pandas as pd
from fastapi import Response
from statsmodels.tsa.seasonal import seasonal_decompose

from logger_conf import logger


def get_dataframe(file_name, sheet_name=None, n_rows=None):
    """
    DataFrame from xlsx or csv
    :param str file_name: file for dataframe
    :param str sheet_name: for Excel file
    :param int n_rows: for cut
    :return: DataFrame
    """
    if file_name.endswith('.xlsx'):
        df = pd.read_excel(
            file_name,
            sheet_name=sheet_name,
            nrows=n_rows,
        )
    else:
        df = pd.read_csv(
            file_name,
            nrows=n_rows,
        )
    return df


def create_files(
        file_name,
        daily_aggregated_filename,
        by_average_filename,
        sheet_name=None,
        n_rows=None,
):
    """
    Create 2 new Excel file
    :param int n_rows: количество считываемых строк
    :param str sheet_name: имя страницы
    :param str file_name: полное имя полученного файла с форматом
    :param str daily_aggregated_filename: имя первого файла
    :param str by_average_filename: имя второго файла
    """

    df = get_dataframe(file_name, sheet_name, n_rows)
    df["Скважина"] = df["Скважина"].astype(str)

    # create df_well
    df_well = df[['Дата замера', 'Обводненность(объемная)', "Скважина"]]
    df_well.set_index('Дата замера', inplace=True)
    df_well['Month'] = df_well.index.month
    df_well['Y-m'] = df_well.index.year
    df_well['Y-m'] = df_well['Y-m'].astype(str) + '-' + \
                     df_well['Month'].astype(str)

    list_wells = df_well["Скважина"].unique()
    # Для каждой скважины отдельно просчитываем seasonal_decompose
    for n_well in list_wells:
        try:
            series = df.loc[
                df["Скважина"] == n_well,
                ['Дата замера', 'Обводненность(объемная)']
            ]
            series.set_index('Дата замера', inplace=True)

            result = seasonal_decompose(
                series,
                model='additive',
            )

            # тренд применяется к скважине
            df_well.loc[
                df_well['Скважина'] == n_well,
                'trend_sesonialclean'
            ] = np.array(result.trend)

        except Exception as e:
            logger.exception(e)
            logger.info(n_well)

    # Найти средние значения по дате
    by_average = df_well.groupby('Y-m').apply(
        lambda x: x['trend_sesonialclean'].mean())

    # to Excel
    by_average.to_excel(by_average_filename)
    df_well.to_excel(daily_aggregated_filename)

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
